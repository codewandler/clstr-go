package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/codewandler/clstr-go/core/es"
)

const (
	defaultSubjectPrefix = "clstr.es"
)

type storeLoadOptions struct {
	startVersion es.Version
	startSeq     uint64 // startSeq is the minimum sequence to include
}

func (l *storeLoadOptions) SetStartVersion(i es.Version) { l.startVersion = i }
func (l *storeLoadOptions) SetStartSeq(i uint64)         { l.startSeq = i }

type EventStoreConfig struct {
	Connect        Connector    // Connect is used to create the underlying NATS connection. If nil, ConnectDefault() is used.
	Log            *slog.Logger // Log for diagnostics (optional)
	SubjectPrefix  string
	StreamSubjects []string
	StreamName     string
	RenameType     func(string) string
}

type EventStore struct {
	nc            *natsgo.Conn
	closeNc       closeFunc
	js            jetstream.JetStream
	stream        jetstream.Stream
	log           *slog.Logger
	subjectPrefix string
	streamName    string
	renameType    func(string) string
}

func NewEventStore(cfg EventStoreConfig) (*EventStore, error) {
	doConnect := cfg.Connect
	if doConnect == nil {
		doConnect = ConnectDefault()
	}

	nc, closeNatsCon, err := doConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	streamName := strings.ToUpper(cfg.StreamName)
	if streamName == "" {
		streamName = "CLSTR_ES"
	}

	subjectPrefix := cfg.SubjectPrefix
	if subjectPrefix == "" {
		subjectPrefix = defaultSubjectPrefix
	}

	streamSubjects := cfg.StreamSubjects
	if streamSubjects == nil || len(streamSubjects) == 0 {
		streamSubjects = []string{fmt.Sprintf("%s.>", subjectPrefix)}
	}

	log = log.With(
		slog.String("store", "nats_js"),
		slog.String("stream", streamName),
		slog.String("subjectPrefix", subjectPrefix),
	)

	log.Debug("ensuring stream")

	stream, streamInfo, err := ensureStream(js, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: streamSubjects,
		//Retention:  jetstream.LimitsPolicy,
		//Storage:    jetstream.MemoryStorage,
		//MaxAge:     0,
		//DenyDelete: true,
		//DenyPurge:  true,
		FirstSeq: 1,
		//Duplicates: 1 * time.Minute,
		//NoAck:      false,
		//Replicas:   0,
	})
	if err != nil {
		return nil, err
	}

	log.Info("ensured", slog.Any("stream", streamInfo))

	return &EventStore{
		nc:            nc,
		closeNc:       closeNatsCon,
		js:            js,
		log:           log,
		stream:        stream,
		subjectPrefix: subjectPrefix,
		streamName:    streamName,
		renameType:    cfg.RenameType,
	}, nil
}

func (e *EventStore) Close() error {
	e.js.CleanupPublisher()
	e.closeNc()
	e.log.Debug("closed event store")
	return nil
}

func (e *EventStore) Subscribe(ctx context.Context, opts ...es.SubscribeOption) (es.Subscription, error) {
	options := es.NewSubscribeOpts(opts...)

	var filterSubjects []string
	for _, f := range options.Filters() {
		if f.AggregateType != "" && f.AggregateID != "" {
			filterSubjects = append(filterSubjects, e.subjectForAggregate(f.AggregateType, f.AggregateID))
		} else if f.AggregateType != "" {
			filterSubjects = append(filterSubjects, e.subjectForAggregate(f.AggregateType, "*"))
		} else {
			return nil, fmt.Errorf("invalid filter: %+v", f)
		}
	}

	if filterSubjects == nil || len(filterSubjects) == 0 {
		filterSubjects = []string{e.subjectForAggregate("*", "*")}
	}

	ch := make(chan es.Envelope, 64)

	consumerCfg := jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		FilterSubjects:    filterSubjects,
		InactiveThreshold: 10 * time.Minute,
	}
	switch options.DeliverPolicy() {
	case es.DeliverNewPolicy:
		consumerCfg.DeliverPolicy = jetstream.DeliverNewPolicy
	case es.DeliverAllPolicy:
		consumerCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	default:
		consumerCfg.DeliverPolicy = jetstream.DeliverNewPolicy
	}

	e.log.Info("subscribe", slog.Any("config", filterSubjects))

	consumer, err := e.stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := msg.Ack(); err != nil {
			e.log.Error("es: failed to ack message", slog.Any("error", err))
			return
		}

		ev, err := e.decodeMsg(msg)
		if err != nil {
			e.log.Error("es: failed to decode message", slog.Any("error", err))
			return
		}

		select {
		case ch <- *ev:
		case <-ctx.Done():
		}
	})
	if err != nil {
		cancel()
		return nil, err
	}

	stopOnce := sync.Once{}
	stop := func() {
		stopOnce.Do(func() {
			cc.Drain()
			cancel()
			close(ch)
		})
	}

	context.AfterFunc(ctx, func() {
		stop()
	})

	return &jsStoreSubscription{ch: ch, cancel: stop}, nil
}

func (e *EventStore) Load(
	ctx context.Context,
	aggType string,
	aggID string,
	opts ...es.StoreLoadOption,
) (loadedEvents []es.Envelope, err error) {
	if aggType == "" {
		return nil, errors.New("aggregate type is empty")
	}
	if aggID == "" {
		return nil, errors.New("aggregate id is empty")
	}

	loadOpts := &storeLoadOptions{}
	for _, opt := range opts {
		opt.ApplyToStoreLoadOptions(loadOpts)
	}

	var (
		startSeq     = loadOpts.startSeq
		startVersion = loadOpts.startVersion
	)

	log := e.log.With(
		slog.Group(
			"agg",
			slog.String("type", aggType),
			slog.String("id", aggID),
		),
	)

	log.Debug(
		"load",
		slog.Group(
			"opts",
			startVersion.SlogAttrWithKey("start_version"),
			slog.Uint64("start_seq", startSeq),
		),
	)

	// create consumer
	var (
		subj         = e.subjectForAggregate(aggType, aggID)
		consumerName = fmt.Sprintf("loader-%s-%s-%s", aggType, aggID, gonanoid.Must())
	)
	consumerCfg := jetstream.ConsumerConfig{
		Name:           consumerName,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subj},
	}
	if startSeq > 0 {
		consumerCfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		consumerCfg.OptStartSeq = startSeq
	}
	cc, err := e.stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		errDelete := e.stream.DeleteConsumer(ctx, consumerName)
		if errDelete != nil {
			e.log.Error("failed to delete consumer", slog.Any("error", errDelete))
		}
	}()
	log.Debug(
		"created new consumer",
		slog.String("consumer", consumerName),
		slog.Uint64("start_seq", consumerCfg.OptStartSeq),
	)

	// get end sequence TODO: verify this is actually correct
	var endSeq uint64
	if mre, getRecentErr := e.getMostRecentEventForAgg(ctx, aggType, aggID); getRecentErr == nil && mre != nil && mre.Seq > 0 {
		endSeq = mre.Seq
	}

	mc, err := cc.Messages()
	if err != nil {
		return nil, err
	}

	var (
		msg    jetstream.Msg
		ev     *es.Envelope
		curSeq uint64
	)
	for {
		msg, err = mc.Next(jetstream.NextMaxWait(5 * time.Millisecond))
		if err != nil {
			// ErrMsgIteratorClosed is triggered by .Drain() below
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				break
			}
			if errors.Is(err, natsgo.ErrTimeout) {
				break
			}
			return nil, fmt.Errorf("failed to fetch message: %w", err)
		}

		// ACK
		err = msg.Ack()
		if err != nil {
			return nil, fmt.Errorf("failed to ack message: %w", err)
		}

		ev, err = e.decodeMsg(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to decode message: %w", err)
		}

		curSeq = ev.Seq
		loadedEvents = append(loadedEvents, *ev)

		// consume stop criteria
		if endSeq > 0 && ev.Seq >= endSeq {
			mc.Drain()
			continue
		}
	}

	log.Debug(
		"fetched",
		slog.Int("num_events", len(loadedEvents)),
		slog.Uint64("last_seq", curSeq),
		slog.Uint64("end_seq", endSeq),
	)

	return loadedEvents, nil
}

func (e *EventStore) Append(
	ctx context.Context,
	aggType string,
	aggID string,
	expectedVersion es.Version,
	events []es.Envelope,
) (res *es.StoreAppendResult, err error) {
	if len(events) == 0 {
		return nil, nil
	}
	if aggType == "" {
		return nil, errors.New("aggregate type is empty")
	}
	if aggID == "" {
		return nil, errors.New("aggregate id is empty")
	}

	// obtain persisted version of the aggregate
	var version es.Version
	version, err = e.getMostRecentVersionForAgg(ctx, aggType, aggID)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	// Optimistic check (best-effort): read current last version.
	if version != expectedVersion {
		return nil, fmt.Errorf("%w: expected version %d, got %d (%s %s)", es.ErrConcurrencyConflict, expectedVersion, version, aggType, aggID)
	}

	// append events
	var lastSeq uint64
	for _, ev := range events {
		lastSeq, err = e.append(ctx, aggType, ev)
		if err != nil {
			return nil, err
		}
	}

	return &es.StoreAppendResult{LastSeq: lastSeq}, nil
}

func (e *EventStore) append(ctx context.Context, aggregateType string, ev es.Envelope) (lastSeq uint64, err error) {
	//appendAt := time.Now()

	err = ev.Validate()
	if err != nil {
		return 0, fmt.Errorf("failed to validate event: %w", err)
	}

	// create message
	subject := e.subjectForAggregate(aggregateType, ev.AggregateID)
	msg := natsgo.NewMsg(subject)
	msg.Header.Set("x-event-type", ev.Type)
	msg.Header.Set("x-aggregate-type", aggregateType)
	msg.Header.Set("x-aggregate-id", ev.AggregateID)
	msg.Data, err = json.Marshal(ev)
	if err != nil {
		return 0, err
	}

	var ack *jetstream.PubAck
	ack, err = e.js.PublishMsg(
		ctx,
		msg,
		jetstream.WithMsgID(ev.ID),
	)

	if err != nil {
		return 0, fmt.Errorf("failed to append to subject %s %s: %w", subject, ev.Type, err)
	}

	/*e.log.Debug(
		"append",
		slog.Group(
			"event",
			slog.String("id", ev.ID),
			slog.String("type", ev.Type),
			slog.Int("version", ev.Version),
			slog.String("aggregate_type", aggregateType),
			slog.String("aggregate_id", ev.AggregateID),
		),
		slog.String("subject", subject),
		slog.Group(
			"ack",
			slog.Int64("seq", int64(ack.Sequence)),
			slog.String("stream", ack.Stream),
			slog.Bool("dup", ack.Duplicate),
		),
		slog.Duration("took", time.Since(appendAt)),
	)*/

	return ack.Sequence, nil
}

func ensureStream(js jetstream.JetStream, cfg jetstream.StreamConfig) (s jetstream.Stream, si *jetstream.StreamInfo, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*natsgo.DefaultTimeout)
	defer cancel()

	s, err = js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	si, err = s.Info(ctx)
	if err != nil {
		return nil, nil, err
	}
	return s, si, nil
}

func (e *EventStore) decodeMsg(msg jetstream.Msg) (env *es.Envelope, err error) {
	var md *jetstream.MsgMetadata
	md, err = msg.Metadata()
	if err != nil {
		return nil, err
	}

	// Decode
	env = &es.Envelope{}
	err = json.Unmarshal(msg.Data(), env)
	if err != nil {
		return nil, err
	}
	env.Seq = md.Sequence.Stream
	return env, nil
}

func (e *EventStore) getMostRecentEventForAgg(ctx context.Context, aggType, aggID string) (lastMsg *es.Envelope, err error) {
	var (
		subject = e.subjectForAggregate(aggType, aggID)
	)
	if lm, getLastErr := e.stream.GetLastMsgForSubject(ctx, subject); getLastErr != nil {
		if !errors.Is(getLastErr, jetstream.ErrMsgNotFound) {
			return nil, getLastErr
		}
	} else if lm != nil {
		lastMsg = &es.Envelope{}
		err = json.Unmarshal(lm.Data, lastMsg)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal last message for subject %q: %w", subject, err)
		}
		lastMsg.Seq = lm.Sequence
	}
	return
}

// getMostRecentVersionForAgg reads the last event from the store and returns its version.
func (e *EventStore) getMostRecentVersionForAgg(ctx context.Context, aggType string, aggID string) (es.Version, error) {
	if lm, err := e.getMostRecentEventForAgg(ctx, aggType, aggID); err != nil {
		return 0, err
	} else if lm != nil {
		return lm.Version, nil
	}

	return 0, nil
}

var _ es.EventStore = &EventStore{}

// --- helpers ---

func (e *EventStore) subjectForAggregate(aggregateType, aggregateID string) string {
	if e.renameType != nil {
		aggregateType = e.renameType(aggregateType)
	}
	return e.subjectPrefix + "." + aggregateType + "." + aggregateID
}

// --- Subscription ---

type jsStoreSubscription struct {
	ch     chan es.Envelope
	cancel context.CancelFunc
}

func (s *jsStoreSubscription) Cancel()                  { s.cancel() }
func (s *jsStoreSubscription) Chan() <-chan es.Envelope { return s.ch }
