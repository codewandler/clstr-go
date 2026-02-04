package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"

	actor "github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/metrics"
)

// actorMetrics implements actor.ActorMetrics using Prometheus.
type actorMetrics struct {
	messageDuration       *prometheus.HistogramVec
	messagesTotal         *prometheus.CounterVec
	panicTotal            *prometheus.CounterVec
	mailboxDepth          *prometheus.GaugeVec
	schedulerInflight     *prometheus.GaugeVec
	schedulerTaskDuration prometheus.Histogram
	schedulerTasksTotal   *prometheus.CounterVec
}

// NewActorMetrics creates a new Prometheus implementation of ActorMetrics.
func NewActorMetrics(reg prometheus.Registerer) actor.ActorMetrics {
	m := &actorMetrics{
		messageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_actor_message_duration_seconds",
			Help:    "Message handling time in seconds",
			Buckets: defaultBuckets,
		}, []string{"message_type"}),

		messagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_actor_messages_total",
			Help: "Total number of messages processed",
		}, []string{"message_type", "success"}),

		panicTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_actor_panics_total",
			Help: "Total number of handler panics",
		}, []string{"message_type"}),

		mailboxDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "clstr_actor_mailbox_depth",
			Help: "Current mailbox queue depth",
		}, []string{"actor_id"}),

		schedulerInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "clstr_actor_scheduler_inflight",
			Help: "Number of concurrent scheduled tasks",
		}, []string{"actor_id"}),

		schedulerTaskDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "clstr_actor_scheduler_task_duration_seconds",
			Help:    "Scheduled task duration in seconds",
			Buckets: defaultBuckets,
		}),

		schedulerTasksTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_actor_scheduler_tasks_total",
			Help: "Total number of scheduled tasks completed",
		}, []string{"success"}),
	}

	reg.MustRegister(
		m.messageDuration,
		m.messagesTotal,
		m.panicTotal,
		m.mailboxDepth,
		m.schedulerInflight,
		m.schedulerTaskDuration,
		m.schedulerTasksTotal,
	)

	return m
}

func (m *actorMetrics) MessageDuration(msgType string) metrics.Timer {
	return newTimer(m.messageDuration.WithLabelValues(msgType))
}

func (m *actorMetrics) MessageProcessed(msgType string, success bool) {
	m.messagesTotal.WithLabelValues(msgType, boolToStr(success)).Inc()
}

func (m *actorMetrics) MessagePanic(msgType string) {
	m.panicTotal.WithLabelValues(msgType).Inc()
}

func (m *actorMetrics) MailboxDepth(actorID string, depth int) {
	m.mailboxDepth.WithLabelValues(actorID).Set(float64(depth))
}

func (m *actorMetrics) SchedulerInflight(actorID string, count int) {
	m.schedulerInflight.WithLabelValues(actorID).Set(float64(count))
}

func (m *actorMetrics) SchedulerTaskDuration() metrics.Timer {
	return newTimer(m.schedulerTaskDuration)
}

func (m *actorMetrics) SchedulerTaskCompleted(success bool) {
	m.schedulerTasksTotal.WithLabelValues(boolToStr(success)).Inc()
}

var _ actor.ActorMetrics = (*actorMetrics)(nil)
