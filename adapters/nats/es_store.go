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
	SubjectPrefix  string       // SubjectPrefix is the prefix used to store events
	StreamSubjects []string     // StreamSubjects is the list of subjects the stream is fed with
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
		return nil, errors.New("stream subjects are required")
	}

	log = log.With(
		slog.String("store", "nats_js"),
		slog.String("stream", streamName),
		slog.String("subjectPrefix", subjectPrefix),
	)

	log.Debug("ensuring stream")

	// TODO: retentions

	stream, streamInfo, err := ensureStream(js, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: streamSubjects,
		//Retention:  jetstream.LimitsPolicy,
		//Storage: jetstream.MemoryStorage,
		Storage: jetstream.FileStorage,
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

	log.Debug("ensured", slog.Any("stream", streamInfo))

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

	maxSeq := 0
	for _, s := range filterSubjects {
		m, err := e.stream.GetLastMsgForSubject(ctx, s)
		if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil, fmt.Errorf("failed to get last message for subject %q: %w", s, err)
		} else if err == nil {
			maxSeq = max(maxSeq, int(m.Sequence))
		}
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

	e.log.Debug("subscribe", slog.Any("config", filterSubjects))

	consumer, err := e.stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer filter_subjects=%+v: %w", filterSubjects, err)
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

	return &jsStoreSubscription{ch: ch, cancel: stop, maxSeq: uint64(maxSeq)}, nil
}

func (e *EventStore) Load(
	ctx context.Context,
	aggType string,
	aggID string,
	opts ...es.StoreLoadOption,
) (loadedEvents []es.Envelope, err error) {

	// validate
	if aggType == "" {
		return nil, errors.New("aggregate type is empty")
	}
	if aggID == "" {
		return nil, errors.New("aggregate id is empty")
	}

	// handle options
	loadOpts := &storeLoadOptions{}
	for _, opt := range opts {
		opt.ApplyToStoreLoadOptions(loadOpts)
	}

	var (
		startAt      = time.Now()
		subj         = e.subjectForAggregate(aggType, aggID)
		startSeq     = loadOpts.startSeq
		startVersion = loadOpts.startVersion
	)

	defer func() {
		if err != nil {
			e.log.Debug(
				"loaded events",
				slog.Group(
					"agg",
					slog.String("type", aggType),
					slog.String("id", aggID),
				),
				slog.Group(
					"opts",
					startVersion.SlogAttrWithKey("start_version"),
					slog.Uint64("start_seq", startSeq),
				),
				slog.Duration("duration", time.Since(startAt)),
			)
		}
	}()

	// get end sequence TODO: verify this is actually correct
	var endSeq uint64
	var mre *es.Envelope
	mre, err = e.getMostRecentEventForAgg(ctx, aggType, aggID)
	if err != nil {
		return nil, err
	}
	if mre == nil {
		// there is no recent message, we do not need to load anything
		return loadedEvents, nil
	}
	endSeq = mre.Seq

	// consume
	consumerCfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		FilterSubjects: []string{subj},
	}
	if startSeq > 0 {
		consumerCfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		consumerCfg.OptStartSeq = startSeq
	}
	cc, err := e.stream.OrderedConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, err
	}
	loadedEvents, err = e.consumeEvents(ctx, cc, endSeq)
	if err != nil {
		return nil, err
	}
	return loadedEvents, nil
}

func (e *EventStore) consumeEvents(
	ctx context.Context,
	cc jetstream.Consumer,
	endSeq uint64,
) (loadedEvents []es.Envelope, err error) {

	var (
		mb  jetstream.MessageBatch
		msg jetstream.Msg
		ev  *es.Envelope
	)

outer:

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		mb, err = cc.FetchNoWait(100)
		if err != nil {
			return nil, err
		}
		if mb.Error() != nil {
			return nil, mb.Error()
		}

		empty := true

		for msg = range mb.Messages() {
			empty = false
			ev, err = e.decodeMsg(msg)
			if err != nil {
				return nil, fmt.Errorf("failed to decode message: %w", err)
			}

			loadedEvents = append(loadedEvents, *ev)

			// consume stop criteria
			if endSeq > 0 && ev.Seq >= endSeq {
				break outer
			}
		}

		if empty {
			break
		}
	}

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
	var mostRecentAvailableVersion es.Version
	mostRecentAvailableVersion, err = e.getMostRecentVersionForAgg(ctx, aggType, aggID)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	// Optimistic check (best-effort): read current last version.
	if mostRecentAvailableVersion != expectedVersion {
		return nil, fmt.Errorf(
			"%w: expected version %d, got %d (agg_type=%s agg_id=%s)",
			es.ErrConcurrencyConflict,
			expectedVersion,
			mostRecentAvailableVersion,
			aggType,
			aggID,
		)
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

	var ackF jetstream.PubAckFuture
	ackF, err = e.js.PublishMsgAsync(
		msg,
		jetstream.WithMsgID(ev.ID),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to append to subject %s %s: %w", subject, ev.Type, err)
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case ack := <-ackF.Ok():
		return ack.Sequence, nil
	}
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
		if errors.Is(getLastErr, jetstream.ErrMsgNotFound) {
			return nil, nil
		}
		return nil, getLastErr
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
	maxSeq uint64
}

func (s *jsStoreSubscription) MaxSequence() uint64      { return s.maxSeq }
func (s *jsStoreSubscription) Cancel()                  { s.cancel() }
func (s *jsStoreSubscription) Chan() <-chan es.Envelope { return s.ch }

var _ es.Subscription = (*jsStoreSubscription)(nil)
