package nats

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/codewandler/clstr-go/core/es"
)

// startNATSC starts a NATS container with JetStream enabled and returns it.
func startNATSC(t *testing.T, ctx context.Context) testcontainers.Container {
	t.Helper()
	c, err := testcontainers.Run(
		ctx, "nats:latest",
		testcontainers.WithCmd("-js"),
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp"),
			wait.ForLog("Server is ready"),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.TerminateContainer(c) })
	return c
}

// startToxiC starts a Toxiproxy container exposing the API port and one proxy port.
func startToxiC(t *testing.T, ctx context.Context) testcontainers.Container {
	t.Helper()
	c, err := testcontainers.Run(
		ctx, "ghcr.io/shopify/toxiproxy:2.9.0",
		testcontainers.WithExposedPorts("8474/tcp", "4223/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("8474/tcp")),
	)
	require.NoError(t, err)
	t.Cleanup(func() { testcontainers.TerminateContainer(c) })
	return c
}

// newStoreWithURL creates an EventStore connected to the given NATS URL.
func newStoreWithURL(t *testing.T, natsURL string) *EventStore {
	t.Helper()
	store, err := NewEventStore(EventStoreConfig{
		Connect:        ConnectURL(natsURL),
		Log:            slog.Default(),
		SubjectPrefix:  "reconnect.t",
		StreamSubjects: []string{"reconnect.>"},
		StreamName:     "RECONNECT_TEST",
		MaxMsgs:        100_000,
	})
	require.NoError(t, err)
	return store
}

// injectEvents appends n events directly to the given store.
func injectEvents(t *testing.T, store *EventStore, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := store.Append(context.Background(), "test", gonanoid.Must(6), es.Version(0), []es.Envelope{
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   gonanoid.Must(6),
				Type:          "TestEvent",
				Version:       1,
			},
		})
		require.NoError(t, err)
	}
}

// drainEvents reads exactly want events from sub within deadline, failing the test if they don't arrive.
func drainEvents(t *testing.T, sub es.Subscription, want int, deadline time.Duration, phase string) {
	t.Helper()
	got := 0
	timeout := time.After(deadline)
	for got < want {
		select {
		case ev, ok := <-sub.Chan():
			if !ok {
				t.Fatalf("[%s] sub.Chan() closed after %d/%d events", phase, got, want)
			}
			got++
			t.Logf("[%s] received event seq=%d (%d/%d)", phase, ev.Seq, got, want)
		case subErr, ok := <-sub.Done():
			if ok {
				t.Fatalf("[%s] sub.Done() received error after %d/%d events: %v", phase, got, want, subErr)
			}
			t.Fatalf("[%s] sub.Done() closed after %d/%d events", phase, got, want)
		case <-timeout:
			t.Fatalf("[%s] timed out after %d/%d events (deadline %s)", phase, got, want, deadline)
		}
	}
}

// TestStore_SubscribeReconnect explores what happens to an active subscription when
// the NATS connection is temporarily interrupted via Toxiproxy.
//
// Three questions answered:
//  1. Does sub.Done() fire during the outage?                  (subscription died?)
//  2. After the connection is restored, does sub.Done() fire? (subscription died on restore?)
//  3. Do events published post-restore arrive on sub.Chan()?  (auto-resume works?)
//
// The test is intentionally observational — it logs its findings.
// Assertions are added for what we know must be true; findings drive the plan.
func TestStore_SubscribeReconnect(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	ctx := t.Context()

	// ── 1. Start containers ───────────────────────────────────────────────────
	natsC := startNATSC(t, ctx)
	toxiC := startToxiC(t, ctx)

	natsIP, err := natsC.ContainerIP(ctx)
	require.NoError(t, err)
	t.Logf("NATS internal IP: %s", natsIP)

	toxiAPIPort, err := toxiC.MappedPort(ctx, "8474/tcp")
	require.NoError(t, err)
	toxiProxyPort, err := toxiC.MappedPort(ctx, "4223/tcp")
	require.NoError(t, err)
	t.Logf("Toxiproxy API: localhost:%s  proxy: localhost:%s", toxiAPIPort.Port(), toxiProxyPort.Port())

	// ── 2. Configure proxy: toxiproxy:4223 → nats:4222 ────────────────────────
	toxiClient := toxiclient.NewClient(fmt.Sprintf("localhost:%s", toxiAPIPort.Port()))
	proxy, err := toxiClient.CreateProxy("nats", "0.0.0.0:4223", fmt.Sprintf("%s:4222", natsIP))
	require.NoError(t, err)
	t.Logf("proxy created: %s → %s", proxy.Listen, proxy.Upstream)

	natsProxyURL := fmt.Sprintf("nats://localhost:%s", toxiProxyPort.Port())
	natsDirectURL := fmt.Sprintf("nats://%s:4222", natsIP)

	// proxyStore — subscription goes through Toxiproxy (affected by outage)
	proxyStore := newStoreWithURL(t, natsProxyURL)
	// directStore — used to inject events; bypasses Toxiproxy (always works)
	directStore := newStoreWithURL(t, natsDirectURL)

	// ── 3. Subscribe through proxy ────────────────────────────────────────────
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	sub, err := proxyStore.Subscribe(subCtx, es.WithDeliverPolicy(es.DeliverAllPolicy))
	require.NoError(t, err)

	// ── 4. Phase 1: baseline — events flow normally ───────────────────────────
	t.Log("── Phase 1: baseline ──")
	injectEvents(t, directStore, 3)
	drainEvents(t, sub, 3, 5*time.Second, "baseline")
	t.Log("baseline OK: 3/3 events received before outage")

	// ── 5. Phase 2: cut the connection ────────────────────────────────────────
	t.Log("── Phase 2: outage start ──")
	require.NoError(t, proxy.Disable())
	t.Log("⚡ proxy disabled — connection severed")

	// Wait for NATS to detect the disconnect.
	// With MaxReconnects(-1) and ReconnectWait(2s) the client will keep retrying.
	outageWindow := 4 * time.Second
	t.Logf("waiting %s for NATS to detect disconnect…", outageWindow)
	time.Sleep(outageWindow)

	// Observe Done() during the outage (non-blocking).
	select {
	case subErr, ok := <-sub.Done():
		if ok {
			t.Logf("FINDING ⚠️  sub.Done() received ERROR during outage: %v", subErr)
		} else {
			t.Log("FINDING ⚠️  sub.Done() CLOSED (clean shutdown) during outage")
		}
		t.Log("→ subscription died during outage; auto-resume is not supported in current impl")
		return
	default:
		t.Log("FINDING ✅  sub.Done() has NOT fired — subscription survived the outage window")
	}

	// ── 6. Phase 3: restore the connection ────────────────────────────────────
	t.Log("── Phase 3: restore ──")
	require.NoError(t, proxy.Enable())
	t.Log("✅ proxy re-enabled")

	reconnectWindow := 4 * time.Second
	t.Logf("waiting %s for NATS to reconnect…", reconnectWindow)
	time.Sleep(reconnectWindow)

	// ── 7. Phase 4: post-restore observations ─────────────────────────────────
	t.Log("── Phase 4: post-restore ──")

	// Check Done() again after restore.
	select {
	case subErr, ok := <-sub.Done():
		if ok {
			t.Logf("FINDING ⚠️  sub.Done() received ERROR after restore: %v", subErr)
		} else {
			t.Log("FINDING ⚠️  sub.Done() CLOSED after restore")
		}
		t.Log("→ subscription died on reconnect; auto-resume is not supported")
		return
	default:
		t.Log("FINDING ✅  sub.Done() still silent after restore")
	}

	// Inject 3 more events and see if they arrive.
	injectEvents(t, directStore, 3)
	t.Log("injected 3 post-restore events")

	received := 0
	timeout := time.After(10 * time.Second)
outer:
	for {
		select {
		case ev, ok := <-sub.Chan():
			if !ok {
				t.Logf("FINDING ⚠️  sub.Chan() CLOSED after %d post-restore events", received)
				break outer
			}
			received++
			t.Logf("post-restore event seq=%d (%d/3)", ev.Seq, received)
			if received == 3 {
				break outer
			}
		case subErr, ok := <-sub.Done():
			if ok {
				t.Logf("FINDING ⚠️  sub.Done() received ERROR while waiting for post-restore events: %v", subErr)
			} else {
				t.Log("FINDING ⚠️  sub.Done() CLOSED while waiting for post-restore events")
			}
			break outer
		case <-timeout:
			t.Logf("FINDING ⚠️  timed out waiting for post-restore events (got %d/3)", received)
			break outer
		}
	}

	if received == 3 {
		t.Log("FINDING ✅  subscription AUTO-RESUMED: all 3 post-restore events arrived")
		t.Log("→ NATS Messages() iterator survives a transient outage transparently")
	} else {
		t.Logf("FINDING ⚠️  only %d/3 post-restore events arrived — partial or no resume", received)
	}
}

// TestStore_SubscribeServerRestart asserts that when the NATS server crashes and
// restarts with completely fresh state (no JetStream stream or consumer), the
// subscription detects the stale consumer within PullExpiry (5s) and signals
// an error on Done(), allowing the consumer's self-healing retry loop to
// re-subscribe against the new server.
func TestStore_SubscribeServerRestart(t *testing.T) {
	ctx := t.Context()

	// ── 1. Start containers ───────────────────────────────────────────────────
	natsC1 := startNATSC(t, ctx)
	toxiC := startToxiC(t, ctx)

	natsIP1, err := natsC1.ContainerIP(ctx)
	require.NoError(t, err)

	toxiAPIPort, err := toxiC.MappedPort(ctx, "8474/tcp")
	require.NoError(t, err)
	toxiProxyPort, err := toxiC.MappedPort(ctx, "4223/tcp")
	require.NoError(t, err)

	// ── 2. Configure proxy → NATS #1 ──────────────────────────────────────────
	toxiClient := toxiclient.NewClient(fmt.Sprintf("localhost:%s", toxiAPIPort.Port()))
	proxy, err := toxiClient.CreateProxy("nats", "0.0.0.0:4223", fmt.Sprintf("%s:4222", natsIP1))
	require.NoError(t, err)

	natsProxyURL := fmt.Sprintf("nats://localhost:%s", toxiProxyPort.Port())
	natsDirectURL1 := fmt.Sprintf("nats://%s:4222", natsIP1)

	proxyStore := newStoreWithURL(t, natsProxyURL)
	directStore1 := newStoreWithURL(t, natsDirectURL1)

	// ── 3. Subscribe and establish baseline ───────────────────────────────────
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	sub, err := proxyStore.Subscribe(subCtx, es.WithDeliverPolicy(es.DeliverAllPolicy))
	require.NoError(t, err)

	t.Log("── Phase 1: baseline ──")
	injectEvents(t, directStore1, 3)
	drainEvents(t, sub, 3, 5*time.Second, "baseline")

	// ── 4. Simulate crash: cut traffic, kill NATS ─────────────────────────────
	t.Log("── Phase 2: crash ──")
	require.NoError(t, proxy.Disable())
	require.NoError(t, testcontainers.TerminateContainer(natsC1))
	t.Log("NATS #1 terminated")

	// ── 5. Start fresh NATS #2 (no JetStream state) ───────────────────────────
	t.Log("── Phase 3: fresh NATS #2 ──")
	natsC2 := startNATSC(t, ctx)
	natsIP2, err := natsC2.ContainerIP(ctx)
	require.NoError(t, err)

	// ── 6. Redirect proxy to NATS #2 ──────────────────────────────────────────
	proxy.Upstream = fmt.Sprintf("%s:4222", natsIP2)
	require.NoError(t, proxy.Save())
	require.NoError(t, proxy.Enable())
	t.Logf("proxy redirected to NATS #2 (%s)", natsIP2)

	// ── 7. Assert sub.Done() signals an error within 10s ─────────────────────
	// PullExpiry(5s) means msgCtx.Next() surfaces the "no heartbeat" error
	// within one pull window after the NATS client reconnects.
	t.Log("── Phase 4: asserting Done() signals ──")
	select {
	case subErr, ok := <-sub.Done():
		require.True(t, ok, "sub.Done() must send an error, not close cleanly")
		require.Error(t, subErr, "sub.Done() error must be non-nil")
		t.Logf("sub.Done() received error (as expected): %v", subErr)
	case <-time.After(10 * time.Second):
		t.Fatal("sub.Done() did not signal within 10s after server restart — stuck iterator?")
	}

	// ── 8. Assert sub.Chan() is closed shortly after Done() fires ────────────
	select {
	case _, ok := <-sub.Chan():
		require.False(t, ok, "sub.Chan() must be closed after Done() signals")
	case <-time.After(2 * time.Second):
		t.Fatal("sub.Chan() was not closed within 2s after Done() signalled")
	}
}
