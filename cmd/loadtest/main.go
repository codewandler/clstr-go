package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/codewandler/clstr-go/adapters/nats"
	"github.com/codewandler/clstr-go/core/es"
)

// === Config ===

// NOTE: run nats: docker run -v "/tmp/nats/jetstream:/tmp/nats/jetstream" --net=host nats:latest -js

var (
	logLevel      = slog.LevelInfo
	N             = getEnvInt("N", 50_000)
	batchSize     = getEnvInt("B", 1_000)
	backendType   = getEnv("BACKEND", "nats")
	useSnapshot   = getEnvBool("SNAPSHOT", true)
	loadAfterSave = getEnvBool("LOAD_AFTER_SAVE", false)
)

func getEnvBool(key string, fallback bool) bool {
	v := getEnv(key, "0")
	if v == "" {
		return fallback
	}
	if v == "1" || strings.ToLower(v) == "true" {
		return true
	}
	return false
}

func getEnv(key, fallback string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) int {
	v, err := strconv.Atoi(getEnv(key, fmt.Sprintf("%d", fallback)))
	if err != nil {
		return fallback
	}
	return v
}

//

type MyProjection struct {
	TotalEvents int
}

func (m *MyProjection) Name() string { return "project_1" }
func (m *MyProjection) Handle(ctx context.Context, env es.Envelope, event any) error {
	m.TotalEvents++
	return nil
}

var _ es.Projection = (*MyProjection)(nil)

func main() {
	var (
		env  *es.Env
		err  error
		repo es.TypedRepository[*User]
		log  = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: logLevel,
		}))
	)

	fmt.Printf("Snaphot: %s\n", strconv.FormatBool(useSnapshot))
	fmt.Printf("Backend: %s\n", backendType)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	switch backendType {
	case "nats":
		env = createNatsEnv(
			log,
			&natsTesting{ctx: context.Background(), log: log, cleanups: make([]func(), 0)},
		)
	default:
		env = createMemEnv(log)
	}

	repo = es.NewTypedRepositoryFrom[*User](log, env.Repository(), es.WithRepoCacheLRU(1_000))

	// === START ===

	log.Info("==================================")
	log.Info("Starting ...")

	startAt := time.Now()

	var myUser *User
	var userID = "user-1"
	myUser, err = repo.GetOrCreate(ctx, userID, es.WithSnapshot(true))
	checkErr(err)
	checkNil(myUser)

	lastTime := time.Now()

	var loaded *User
	for i := 0; i < N; i++ {
		// write a change
		checkErr(myUser.ChangeEmail(fmt.Sprintf("user@host-%d.com", i)))
		checkErr(repo.Save(ctx, myUser, es.WithSnapshot(useSnapshot)))

		if loadAfterSave {
			loaded, err = repo.GetByID(ctx, userID, es.WithSnapshot(useSnapshot))
			checkErr(err)
			checkNil(loaded)
		}

		if i == 0 {
			continue
		}
		if i%100 == 0 {
			print(".")
		}
		if i%batchSize == 0 {
			mu := getMemUsage()

			n := time.Now()
			took := n.Sub(lastTime)
			fmt.Printf(" | %5d events | %6d ms |  %6d events/s | (%d / %d) MiB mem (sys) |\n", batchSize, took.Milliseconds(), int(float64(batchSize)/took.Seconds()), mu.Alloc/1024/1024, mu.Sys/1024/1024)
			lastTime = n
		}
	}

	// === stats ===
	println("")
	println("==========================================")

	doneAt := time.Now()
	took := doneAt.Sub(startAt)
	runtime.GC()

	fmt.Printf("total runtime: %.3f seconds\n", took.Seconds())
	fmt.Printf("      version: %d\n", myUser.GetVersion())
	fmt.Printf("   stream seq: %d\n", myUser.GetSeq())
	fmt.Printf("avg. writes/s: %d\n", int(float64(N)/took.Seconds()))
}

// === stats helpers ===

type MemUsage struct {
	Alloc      uint64 // bytes allocated and not yet freed (heap)
	TotalAlloc uint64 // cumulative bytes allocated
	Sys        uint64 // total bytes obtained from OS
	NumGC      uint32 // gc cycles
}

func getMemUsage() MemUsage {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemUsage{
		Alloc:      m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		NumGC:      m.NumGC,
	}
}

// === Env ===

func createMemEnv(log *slog.Logger) (env *es.Env) {
	var err error
	env, err = es.NewEnv(
		es.WithInMemory(),
		es.WithAggregates(new(User)),
		es.WithProjections(&MyProjection{}, es.NewDebugProjection(log)),
	)
	checkErr(err)
	return env
}

func createNatsEnv(log *slog.Logger, lt nats.Testing) (env *es.Env) {
	var err error

	connectNats := nats.ConnectDefault()
	var store es.EventStore
	store, err = nats.NewEventStore(nats.EventStoreConfig{
		Log:           log,
		Connect:       connectNats,
		SubjectPrefix: "clstr.loadtest",
		StreamSubjects: []string{
			"clstr.>",
		},
	})
	checkErr(err)

	var snapshotter es.Snapshotter
	snapshotter, err = nats.NewSnapshotter(nats.KvConfig{
		Connect: connectNats,
		TTL:     5 * time.Minute,
		Bucket:  "loadtest_snapshots",
	})
	checkErr(err)

	var cps es.CpStore
	cps, err = nats.NewCpStore(nats.CpStoreConfig{
		Bucket:  "loadtest_cps",
		Connect: connectNats,
	})

	var subCps es.SubCpStore
	subCps, err = nats.NewSubCpStore(nats.SubCpStoreConfig{
		Bucket:  "loadtest_subcps",
		Key:     "loadtest_subcps_key",
		Connect: connectNats,
	})

	// === wire env ===

	env, err = es.NewEnv(
		es.WithStore(store),
		es.WithSnapshotter(snapshotter),
		es.WithSubCheckpointStore(subCps),
		es.WithCheckpointStore(cps),
		es.WithAggregates(new(User)),
		es.WithProjections(&MyProjection{}, es.NewDebugProjection(log)),
	)
	checkErr(err)
	return env
}

// === Domain ===

type (
	User struct {
		es.BaseAggregate

		Name  string
		Email string
	}

	NameChanged  struct{ NewName string }
	EmailChanged struct{ NewEmail string }
)

func (u *User) Apply(e any) error {
	switch evt := e.(type) {
	case *es.AggregateCreatedEvent:
		return u.BaseAggregate.Apply(evt)
	case *NameChanged:
		u.Name = evt.NewName
		return nil
	case *EmailChanged:
		u.Email = evt.NewEmail
		return nil
	}
	return nil
}

func (u *User) ChangeName(name string) error {
	if name == "" {
		return fmt.Errorf("name is empty")
	}
	return es.RaiseAndApply(u, &NameChanged{NewName: name})
}

func (u *User) ChangeEmail(email string) error {
	if email == "" {
		return fmt.Errorf("email is empty")
	}
	return es.RaiseAndApply(u, &EmailChanged{NewEmail: email})
}

func (u *User) GetAggType() string { return "user" }

func (u *User) Register(r es.Registrar) {
	es.RegisterEvents(
		r,
		es.Event[NameChanged](),
		es.Event[EmailChanged](),
	)
}

var _ es.Aggregate = (*User)(nil)

// === Helpers ===

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func checkNil(v any) {
	if v == nil {
		panic("nil value")
	}
}

// === Testing Helper ===

type natsTesting struct {
	ctx      context.Context
	log      *slog.Logger
	cleanups []func()
}

func (l *natsTesting) Errorf(format string, args ...interface{}) {
	l.log.Error("LOADTEST :: " + fmt.Sprintf(format, args...))
	l.FailNow()
}
func (l *natsTesting) FailNow()                 { panic("implement me") }
func (l *natsTesting) Context() context.Context { return l.ctx }
func (l *natsTesting) Logf(format string, args ...any) {
	l.log.Info("LOADTEST :: " + fmt.Sprintf(format, args...))
}
func (l *natsTesting) Cleanup(f func()) { l.cleanups = append(l.cleanups, f) }

func (l *natsTesting) doCleanup() {
	for _, c := range l.cleanups {
		c()
	}
}
