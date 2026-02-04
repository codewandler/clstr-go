package prometheus

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewESMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewESMetrics(reg)

	require.NotNil(t, m)

	// Test store operations
	timer := m.StoreLoadDuration("user")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	timer = m.StoreAppendDuration("user")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	m.EventsAppended("user", 5)

	// Test repository operations
	timer = m.RepoLoadDuration("user")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	timer = m.RepoSaveDuration("user")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	m.ConcurrencyConflict("user")

	// Test cache
	m.CacheHit("user")
	m.CacheMiss("user")

	// Test snapshots
	timer = m.SnapshotLoadDuration("user")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	timer = m.SnapshotSaveDuration("user")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	// Test consumer
	timer = m.ConsumerEventDuration("UserCreated", true)
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	m.ConsumerEventProcessed("UserCreated", true, true)
	m.ConsumerEventProcessed("UserCreated", false, false)

	m.ConsumerLag("my-consumer", 100)

	// Verify metrics were registered
	mfs, err := reg.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, mfs)

	// Check that we have the expected metric families
	names := make(map[string]bool)
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}

	assert.True(t, names["clstr_es_store_load_duration_seconds"])
	assert.True(t, names["clstr_es_repo_load_duration_seconds"])
	assert.True(t, names["clstr_es_cache_hits_total"])
	assert.True(t, names["clstr_es_consumer_lag"])
}

func TestNewActorMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewActorMetrics(reg)

	require.NotNil(t, m)

	// Test message handling
	timer := m.MessageDuration("MyMessage")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	m.MessageProcessed("MyMessage", true)
	m.MessageProcessed("MyMessage", false)
	m.MessagePanic("MyMessage")

	// Test mailbox
	m.MailboxDepth("actor-123", 10)

	// Test scheduler
	m.SchedulerInflight("actor-123", 5)

	timer = m.SchedulerTaskDuration()
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	m.SchedulerTaskCompleted(true)
	m.SchedulerTaskCompleted(false)

	// Verify metrics were registered
	mfs, err := reg.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, mfs)

	names := make(map[string]bool)
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}

	assert.True(t, names["clstr_actor_message_duration_seconds"])
	assert.True(t, names["clstr_actor_messages_total"])
	assert.True(t, names["clstr_actor_mailbox_depth"])
}

func TestNewClusterMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewClusterMetrics(reg)

	require.NotNil(t, m)

	// Test client operations
	timer := m.RequestDuration("GetUser")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	m.RequestCompleted("GetUser", true)
	m.RequestCompleted("GetUser", false)
	m.NotifyCompleted("UserUpdated", true)

	// Test transport errors
	m.TransportError("no_subscriber")
	m.TransportError("timeout")

	// Test handler operations
	timer = m.HandlerDuration("GetUser")
	assert.NotNil(t, timer)
	timer.ObserveDuration()

	m.HandlerCompleted("GetUser", true)
	m.HandlersActive("node-1", 5)

	// Test shards
	m.ShardsOwned("node-1", 10)

	// Verify metrics were registered
	mfs, err := reg.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, mfs)

	names := make(map[string]bool)
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}

	assert.True(t, names["clstr_cluster_request_duration_seconds"])
	assert.True(t, names["clstr_cluster_transport_errors_total"])
	assert.True(t, names["clstr_cluster_shards_owned"])
}

func TestNewAllMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewAllMetrics(reg)

	require.NotNil(t, m)
	require.NotNil(t, m.ES)
	require.NotNil(t, m.Actor)
	require.NotNil(t, m.Cluster)

	// All metrics should be usable
	m.ES.CacheHit("user")
	m.Actor.MessageProcessed("test", true)
	m.Cluster.RequestCompleted("test", true)

	// Verify all metric families registered
	mfs, err := reg.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, mfs)
}

func TestBoolToStr(t *testing.T) {
	assert.Equal(t, "true", boolToStr(true))
	assert.Equal(t, "false", boolToStr(false))
}
