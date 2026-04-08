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

// RetentionPolicy defines how messages are retained in the stream.
type RetentionPolicy int

const (
	// RetentionLimits keeps messages until limits (MaxMsgs, MaxBytes, MaxAge) are reached.
	// This is the default NATS behavior - messages are kept until explicitly limited.
	RetentionLimits RetentionPolicy = iota

	// RetentionInterest keeps messages only while there are consumers with interest.
	// Messages are deleted once all interested consumers have acknowledged.
	RetentionInterest

	// RetentionWorkQueue makes each message available to only one consumer (queue semantics).
	// Messages are deleted after being consumed by any one consumer.
	RetentionWorkQueue
)

func (r RetentionPolicy) toJetStream() jetstream.RetentionPolicy {
	switch r {
	case RetentionInterest:
		return jetstream.InterestPolicy
	case RetentionWorkQueue:
		return jetstream.WorkQueuePolicy
	default:
		return jetstream.LimitsPolicy
	}
}

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

	// Retention defines the retention policy for the stream (default: RetentionLimits).
	Retention RetentionPolicy

	// At least one of MaxAge, MaxBytes, or MaxMsgs must be set to prevent unbounded growth.

	// MaxAge is the maximum age of messages in the stream.
	MaxAge time.Duration

	// MaxBytes is the maximum total size of messages in the stream.
	MaxBytes int64

	// MaxMsgs is the maximum number of messages in the stream.
	MaxMsgs int64

}

type EventStore struct {
	nc               *natsgo.Conn
	js               jetstream.JetStream
	stream           jetstream.Stream
	log              *slog.Logger
	subjectPrefix string
	streamName    string
	renameType    func(string) string
}

func NewEventStore(cfg EventStoreConfig) (*EventStore, error) {
	if cfg.MaxAge == 0 && cfg.MaxBytes == 0 && cfg.MaxMsgs == 0 {
		return nil, errors.New("at least one retention limit must be set (MaxAge, MaxBytes, or MaxMsgs)")
	}

	doConnect := cfg.Connect
	if doConnect == nil {
		doConnect = ConnectDefault()
	}

	nc, err := doConnect()
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
	if len(streamSubjects) == 0 {
		return nil, errors.New("stream subjects are required")
	}

	// Apply values for stream config (0 means unlimited in NATS for these fields)
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = -1
	}
	maxMsgs := cfg.MaxMsgs
	if maxMsgs == 0 {
		maxMsgs = -1
	}

	log = log.With(
		slog.String("store", "nats_js"),
		slog.String("stream", streamName),
		slog.String("subjectPrefix", subjectPrefix),
	)

	log.Debug("ensuring stream")

	stream, streamInfo, err := ensureStream(js, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  streamSubjects,
		Retention: cfg.Retention.toJetStream(),
		Storage:   jetstream.FileStorage,
		MaxAge:    cfg.MaxAge,
		MaxBytes:  maxBytes,
		MaxMsgs:   maxMsgs,
		FirstSeq:  1,
	})
	if err != nil {
		return nil, err
	}

	log.Debug("ensured", slog.Any("stream", streamInfo))

	return &EventStore{
		nc:            nc,
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
	// Drain gracefully shuts down by unsubscribing all subscriptions,
	// waiting for pending messages to be processed, and flushing outgoing messages.
	// This is preferred over nc.Close() which closes immediately.
	if err := e.nc.Drain(); err != nil {
		e.nc.Close()
		e.log.Debug("closed event store (drain failed)", slog.Any("error", err))
		return nil
	}
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

	if len(filterSubjects) == 0 {
		filterSubjects = []string{e.subjectForAggregate("*", "*")}
	}

	var maxSeq uint64
	for _, s := range filterSubjects {
		m, err := e.stream.GetLastMsgForSubject(ctx, s)
		if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil, fmt.Errorf("failed to get last message for subject %q: %w", s, err)
		} else if err == nil {
			maxSeq = max(maxSeq, m.Sequence)
		}
	}

	ch := make(chan es.Envelope, 64)
	done := make(chan error, 1)

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

	if options.StartSequence() > 0 {
		consumerCfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		consumerCfg.OptStartSeq = options.StartSequence()
	}

	e.log.Debug("subscribe", slog.Any("consumer_config", consumerCfg), slog.Uint64("max_sequence", maxSeq))

	consumer, err := e.stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer filter_subjects=%+v: %w", filterSubjects, err)
	}

	msgCtx, err := consumer.Messages(
		// PullExpiry caps each pull request at 5 seconds. Without this the
		// default is 30 seconds, which means a stuck iterator (e.g. after a
		// server restart that wiped JetStream state) would block msgCtx.Next()
		// for 30 s before surfacing an error through sub.Done().
		jetstream.PullExpiry(5 * time.Second),
	)
	if err != nil {
		return nil, err
	}

	stopOnce := sync.Once{}
	stop := func() {
		stopOnce.Do(func() {
			e.log.Debug("draining subscription")
			msgCtx.Drain()
		})
	}

	context.AfterFunc(ctx, func() {
		stop()
	})

	go func() {
		var exitErr error
		defer func() {
			stop() // ensure cleanup even on error exit
			e.log.Debug("unsubscribed")
			// Signal subscription termination before closing ch so the consumer
			// can handle the Done case before seeing a closed channel.
			if exitErr != nil {
				done <- exitErr
			} else {
				close(done)
			}
			close(ch)
		}()

		for {
			msg, err := msgCtx.Next()
			if err != nil {
				if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
					return
				}
				e.log.Error("failed to read next message", slog.Any("error", err))
				exitErr = err
				return
			}

			if err := msg.Ack(); err != nil {
				e.log.Error("es: failed to ack message", slog.Any("error", err))
				exitErr = err
				return
			}

			ev, err := e.decodeMsg(msg)
			if err != nil {
				e.log.Error("es: failed to decode message", slog.Any("error", err))
				exitErr = err
				return
			}

			select {
			case ch <- *ev:
			case <-ctx.Done():
				return
			}
		}
	}()

	return &jsStoreSubscription{
		ch:     ch,
		done:   done,
		cancel: stop,
		maxSeq: maxSeq,
	}, nil
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
		startAt  = time.Now()
		subj     = e.subjectForAggregate(aggType, aggID)
		startSeq = loadOpts.startSeq
	)

	defer func() {
		if err == nil {
			e.log.Debug(
				"loaded events",
				slog.Group(
					"agg",
					slog.String("type", aggType),
					slog.String("id", aggID),
				),
				slog.Group(
					"opts",
					loadOpts.startVersion.SlogAttrWithKey("start_version"),
					slog.Uint64("start_seq", startSeq),
				),
				slog.Int("count", len(loadedEvents)),
				slog.Duration("duration", time.Since(startAt)),
			)
		}
	}()

	// endSeq is the global stream sequence of the last known event for this aggregate.
	// The consumer stops as soon as it delivers a message at or beyond this seq,
	// preventing unbounded reads if new events are appended during Load.
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

	// Nothing to load: the caller's start position is already at or beyond
	// the latest event (e.g. snapshot was taken at the most recent event).
	// Without this guard iter.Next() would block forever waiting for a new
	// message that will never arrive on this subject.
	if startSeq > 0 && startSeq > endSeq {
		return loadedEvents, nil
	}

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

	iter, err := cc.Messages()
	if err != nil {
		return nil, fmt.Errorf("create message iterator: %w", err)
	}
	defer iter.Stop()

	// Ensure the iterator is stopped when the caller's context is cancelled,
	// so Next() unblocks promptly instead of waiting for the next server push.
	stopCtx := context.AfterFunc(ctx, iter.Stop)
	defer stopCtx()

	for {
		msg, err := iter.Next()
		if err != nil {
			// Surface the original context error when the iterator was closed
			// because the caller cancelled.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("read next message: %w", err)
		}

		ev, err := e.decodeMsg(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to decode message: %w", err)
		}

		loadedEvents = append(loadedEvents, *ev)

		if ev.Seq >= endSeq {
			return loadedEvents, nil
		}
	}
}

func (e *EventStore) Append(
	ctx context.Context,
	aggType string,
	aggID string,
	expectedVersion es.Version,
	events []es.Envelope,
) (res *es.StoreAppendResult, err error) {

	if len(events) == 0 {
		return nil, fmt.Errorf("no events to append")
	}
	if aggType == "" {
		return nil, errors.New("aggregate type is empty")
	}
	if aggID == "" {
		return nil, errors.New("aggregate id is empty")
	}

	// Determine the expected last subject sequence for atomic CAS.
	// JetStream's WithExpectLastSequencePerSubject rejects the publish
	// server-side if another message was appended since we read, closing
	// the TOCTOU gap that the old best-effort version check had.
	var expectedLastSubjSeq uint64
	if expectedVersion > 0 {
		lm, getErr := e.getMostRecentEventForAgg(ctx, aggType, aggID)
		if getErr != nil {
			return nil, fmt.Errorf("failed to get last event: %w", getErr)
		}
		if lm == nil {
			return nil, fmt.Errorf(
				"%w: expected version %d, got 0 (agg_type=%s agg_id=%s)",
				es.ErrConcurrencyConflict, expectedVersion, aggType, aggID,
			)
		}
		if lm.Version != expectedVersion {
			return nil, fmt.Errorf(
				"%w: expected version %d, got %d (agg_type=%s agg_id=%s)",
				es.ErrConcurrencyConflict, expectedVersion, lm.Version, aggType, aggID,
			)
		}
		expectedLastSubjSeq = lm.Seq
	}
	// else: expectedVersion == 0 → new aggregate, expectedLastSubjSeq stays 0
	// which tells JetStream the subject must have no prior messages.

	// Append events. The first publish carries the CAS header;
	// subsequent events are sequenced by JetStream on the same subject.
	var lastSeq uint64
	for i, ev := range events {
		var casSeq *uint64
		if i == 0 {
			casSeq = &expectedLastSubjSeq
		}
		lastSeq, err = e.append(ctx, aggType, ev, casSeq)
		if err != nil {
			return nil, err
		}
	}

	return &es.StoreAppendResult{LastSeq: lastSeq}, nil
}

func (e *EventStore) append(ctx context.Context, aggregateType string, ev es.Envelope, casSeq *uint64) (lastSeq uint64, err error) {
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

	opts := []jetstream.PublishOpt{
		jetstream.WithMsgID(ev.ID),
	}
	if casSeq != nil {
		opts = append(opts, jetstream.WithExpectLastSequencePerSubject(*casSeq))
	}

	ack, err := e.js.PublishMsg(ctx, msg, opts...)
	if err != nil {
		// Map JetStream wrong-last-sequence error to ErrConcurrencyConflict.
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			return 0, fmt.Errorf("%w: %s", es.ErrConcurrencyConflict, err)
		}
		return 0, fmt.Errorf("failed to append to subject %s %s: %w", subject, ev.Type, err)
	}

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
	done   chan error
	cancel context.CancelFunc
	maxSeq uint64
}

func (s *jsStoreSubscription) MaxSequence() uint64      { return s.maxSeq }
func (s *jsStoreSubscription) Cancel()                  { s.cancel() }
func (s *jsStoreSubscription) Chan() <-chan es.Envelope { return s.ch }
func (s *jsStoreSubscription) Done() <-chan error       { return s.done }

var _ es.Subscription = (*jsStoreSubscription)(nil)
