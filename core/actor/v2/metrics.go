package actor

import "github.com/codewandler/clstr-go/core/metrics"

// ActorMetrics defines the metrics interface for the Actor pillar.
// All methods are thread-safe.
type ActorMetrics interface {
	// Message handling
	MessageDuration(msgType string) metrics.Timer
	MessageProcessed(msgType string, success bool)
	MessagePanic(msgType string)

	// Mailbox
	MailboxDepth(actorID string, depth int)

	// Scheduler
	SchedulerInflight(actorID string, count int)
	SchedulerTaskDuration() metrics.Timer
	SchedulerTaskCompleted(success bool)
}

// nopActorMetrics is a no-op implementation of ActorMetrics.
type nopActorMetrics struct{}

func (nopActorMetrics) MessageDuration(string) metrics.Timer { return metrics.NopTimer() }
func (nopActorMetrics) MessageProcessed(string, bool)        {}
func (nopActorMetrics) MessagePanic(string)                  {}

func (nopActorMetrics) MailboxDepth(string, int) {}

func (nopActorMetrics) SchedulerInflight(string, int)        {}
func (nopActorMetrics) SchedulerTaskDuration() metrics.Timer { return metrics.NopTimer() }
func (nopActorMetrics) SchedulerTaskCompleted(bool)          {}

// NopActorMetrics returns a no-op ActorMetrics implementation.
func NopActorMetrics() ActorMetrics { return nopActorMetrics{} }
