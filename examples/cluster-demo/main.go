// Package main demonstrates a distributed event-sourced actor cluster using NATS JetStream.
//
// This example shows all three pillars of clstr working together:
//   - Cluster: Routes requests to the correct node based on account ID
//   - Actor: Processes messages with mailbox isolation (one actor per account)
//   - Event Sourcing: Persists account state as a sequence of events
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/codewandler/clstr-go/adapters/nats"
	"github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/cluster"
	"github.com/codewandler/clstr-go/core/es"
)

// =============================================================================
// Domain: Account Aggregate (Event Sourcing)
// =============================================================================

// Account is an event-sourced aggregate representing a bank account.
type Account struct {
	es.BaseAggregate
	balance int
	owner   string
}

func (a *Account) GetAggType() string { return "account" }

func (a *Account) Register(r es.Registrar) {
	es.RegisterEventFor[AccountOpened](r)
	es.RegisterEventFor[MoneyDeposited](r)
	es.RegisterEventFor[MoneyWithdrawn](r)
}

func (a *Account) Apply(evt any) error {
	switch e := evt.(type) {
	case *es.AggregateCreatedEvent:
		return a.BaseAggregate.Apply(evt)
	case *AccountOpened:
		a.owner = e.Owner
		a.balance = e.InitialBalance
	case *MoneyDeposited:
		a.balance += e.Amount
	case *MoneyWithdrawn:
		a.balance -= e.Amount
	default:
		return fmt.Errorf("unknown event: %T", evt)
	}
	return nil
}

// Open initializes the account with an owner.
func (a *Account) Open(owner string, initialBalance int) error {
	if a.IsCreated() {
		return fmt.Errorf("account already exists")
	}
	if err := a.Create(a.GetID()); err != nil {
		return err
	}
	return es.RaiseAndApply(a, &AccountOpened{Owner: owner, InitialBalance: initialBalance})
}

// Deposit adds money to the account.
func (a *Account) Deposit(amount int) error {
	if amount <= 0 {
		return fmt.Errorf("deposit amount must be positive")
	}
	return es.RaiseAndApply(a, &MoneyDeposited{Amount: amount})
}

// Withdraw removes money from the account.
func (a *Account) Withdraw(amount int) error {
	if amount <= 0 {
		return fmt.Errorf("withdrawal amount must be positive")
	}
	if a.balance < amount {
		return fmt.Errorf("insufficient funds: balance=%d, requested=%d", a.balance, amount)
	}
	return es.RaiseAndApply(a, &MoneyWithdrawn{Amount: amount})
}

func (a *Account) Balance() int  { return a.balance }
func (a *Account) Owner() string { return a.owner }

// Events
type (
	AccountOpened struct {
		Owner          string `json:"owner"`
		InitialBalance int    `json:"initial_balance"`
	}
	MoneyDeposited struct {
		Amount int `json:"amount"`
	}
	MoneyWithdrawn struct {
		Amount int `json:"amount"`
	}
)

// =============================================================================
// Commands & Responses (Actor Messages)
// =============================================================================

type (
	OpenAccount struct {
		Owner          string `json:"owner"`
		InitialBalance int    `json:"initial_balance"`
	}
	DepositMoney struct {
		Amount int `json:"amount"`
	}
	WithdrawMoney struct {
		Amount int `json:"amount"`
	}
	GetBalance      struct{}
	BalanceResponse struct {
		Balance int    `json:"balance"`
		Owner   string `json:"owner"`
	}
)

// =============================================================================
// Configuration
// =============================================================================

const (
	numNodes  = 3
	numShards = 64
	seed      = "demo-cluster"
)

// =============================================================================
// Main
// =============================================================================

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(ctx, log); err != nil {
		log.Error("demo failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	// Start NATS container
	log.Info("starting NATS container...")
	natsURL, cleanup, err := startNATSContainer(ctx, log)
	if err != nil {
		return fmt.Errorf("start nats: %w", err)
	}
	defer cleanup()
	log.Info("NATS ready", slog.String("url", natsURL))

	// Create event sourcing environment (shared across all nodes)
	esEnv, err := createEventSourcingEnv(ctx, log, natsURL)
	if err != nil {
		return fmt.Errorf("create es env: %w", err)
	}
	defer esEnv.Shutdown()

	// Create typed repository for accounts
	accountRepo := es.NewTypedRepositoryFrom[*Account](log, esEnv.Repository())

	// Create transports for each node
	transports := make([]*nats.Transport, numNodes)
	for i := range numNodes {
		tr, err := nats.NewTransport(nats.TransportConfig{
			Connect:       nats.ConnectURL(natsURL),
			Log:           log.With(slog.Int("node", i)),
			SubjectPrefix: "demo",
		})
		if err != nil {
			return fmt.Errorf("create transport %d: %w", i, err)
		}
		defer tr.Close()
		transports[i] = tr
	}

	// Generate node IDs
	nodeIDs := make([]string, numNodes)
	for i := range numNodes {
		nodeIDs[i] = fmt.Sprintf("node-%d", i)
	}

	// Start cluster nodes
	var wg sync.WaitGroup
	nodeCtx, nodeCancel := context.WithCancel(ctx)
	defer nodeCancel()

	for i := range numNodes {
		nodeID := nodeIDs[i]
		shards := cluster.ShardsForNode(nodeID, nodeIDs, numShards, seed)
		log.Info("starting node",
			slog.String("nodeID", nodeID),
			slog.Int("shardCount", len(shards)),
		)

		node := cluster.NewNode(cluster.NodeOptions{
			NodeID:    nodeID,
			Transport: transports[i],
			Shards:    shards,
			Log:       log.With(slog.String("node", nodeID)),
			Handler:   createAccountHandler(nodeCtx, log.With(slog.String("node", nodeID)), accountRepo),
		})

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := node.Run(nodeCtx); err != nil && nodeCtx.Err() == nil {
				log.Error("node error", slog.Any("error", err))
			}
		}()
	}

	// Give nodes time to subscribe
	time.Sleep(100 * time.Millisecond)

	// Create a client
	client, err := cluster.NewClient(cluster.ClientOptions{
		Transport: transports[0],
		NumShards: numShards,
		Seed:      seed,
	})
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	// ==========================================================================
	// Demo: Event-Sourced Accounts
	// ==========================================================================
	log.Info("=== Demo: Event-Sourced Bank Accounts ===")

	accounts := []string{"alice", "bob", "charlie"}

	// Open accounts with initial balances
	log.Info("opening accounts...")
	for _, name := range accounts {
		if _, err := cluster.NewRequest[OpenAccount, BalanceResponse](client.Key(name)).
			Request(ctx, OpenAccount{Owner: name, InitialBalance: 100}); err != nil {
			return fmt.Errorf("open account %s: %w", name, err)
		}
	}

	// Perform some transactions
	log.Info("performing transactions...")

	if _, err := cluster.NewRequest[DepositMoney, BalanceResponse](client.Key("alice")).
		Request(ctx, DepositMoney{Amount: 50}); err != nil {
		return fmt.Errorf("deposit: %w", err)
	}

	if _, err := cluster.NewRequest[WithdrawMoney, BalanceResponse](client.Key("bob")).
		Request(ctx, WithdrawMoney{Amount: 30}); err != nil {
		return fmt.Errorf("withdraw: %w", err)
	}

	if _, err := cluster.NewRequest[DepositMoney, BalanceResponse](client.Key("charlie")).
		Request(ctx, DepositMoney{Amount: 200}); err != nil {
		return fmt.Errorf("deposit: %w", err)
	}

	// Check final balances (demonstrates state reconstruction from events)
	log.Info("querying final balances...")
	for _, name := range accounts {
		if _, err := cluster.NewRequest[GetBalance, BalanceResponse](client.Key(name)).
			Request(ctx, GetBalance{}); err != nil {
			return fmt.Errorf("get balance %s: %w", name, err)
		}
	}

	log.Info("demo complete, shutting down...")
	nodeCancel()
	wg.Wait()

	return nil
}

// =============================================================================
// Event Sourcing Setup
// =============================================================================

func createEventSourcingEnv(ctx context.Context, log *slog.Logger, natsURL string) (*es.Env, error) {
	// Create event store backed by NATS JetStream
	eventStore, err := nats.NewEventStore(nats.EventStoreConfig{
		Connect:        nats.ConnectURL(natsURL),
		Log:            log,
		SubjectPrefix:  "demo.es",
		StreamName:     "DEMO_EVENTS",
		StreamSubjects: []string{"demo.es.>"},
		MaxAge:         24 * time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("create event store: %w", err)
	}

	// Create environment with in-memory snapshotter (production would use NATS KV)
	env := es.NewEnv(
		es.WithCtx(ctx),
		es.WithLog(log),
		es.WithStore(eventStore),
		es.WithInMemory(), // For snapshots; in production use nats.NewSnapshotter
		es.WithAggregates(&Account{}),
	)

	return env, nil
}

// =============================================================================
// Actor Handler
// =============================================================================

// createAccountHandler returns a cluster handler that spawns one actor per account.
// Each actor loads/saves its account aggregate via event sourcing.
func createAccountHandler(ctx context.Context, log *slog.Logger, repo es.TypedRepository[*Account]) cluster.ServerHandlerFunc {
	return cluster.NewActorHandler(func(accountID string) (actor.Actor, error) {
		return actor.New(
			actor.Options{
				Context:     ctx,
				Logger:      log.With(slog.String("account", accountID)),
				MailboxSize: 256,
			},
			actor.TypedHandlers(
				actor.HandleRequest[OpenAccount, BalanceResponse](
					func(hc actor.HandlerCtx, cmd OpenAccount) (*BalanceResponse, error) {
						// Try to load existing account
						acc, err := repo.GetByID(hc, accountID)
						if err == nil {
							return &BalanceResponse{Balance: acc.Balance(), Owner: acc.Owner()}, nil
						}
						if !errors.Is(err, es.ErrAggregateNotFound) {
							return nil, err
						}

						// Create new account
						acc = repo.NewWithID(accountID)
						if err := acc.Open(cmd.Owner, cmd.InitialBalance); err != nil {
							return nil, err
						}
						if err := repo.Save(hc, acc); err != nil {
							return nil, err
						}

						hc.Log().Info("opened account", slog.Int("balance", acc.Balance()))
						return &BalanceResponse{Balance: acc.Balance(), Owner: acc.Owner()}, nil
					},
				),

				actor.HandleRequest[DepositMoney, BalanceResponse](
					func(hc actor.HandlerCtx, cmd DepositMoney) (*BalanceResponse, error) {
						acc, err := repo.GetByID(hc, accountID)
						if err != nil {
							return nil, err
						}

						if err := acc.Deposit(cmd.Amount); err != nil {
							return nil, err
						}

						if err := repo.Save(hc, acc); err != nil {
							return nil, err
						}

						hc.Log().Info("deposited", slog.Int("amount", cmd.Amount), slog.Int("balance", acc.Balance()))
						return &BalanceResponse{Balance: acc.Balance(), Owner: acc.Owner()}, nil
					},
				),

				actor.HandleRequest[WithdrawMoney, BalanceResponse](
					func(hc actor.HandlerCtx, cmd WithdrawMoney) (*BalanceResponse, error) {
						acc, err := repo.GetByID(hc, accountID)
						if err != nil {
							return nil, err
						}

						if err := acc.Withdraw(cmd.Amount); err != nil {
							return nil, err
						}

						if err := repo.Save(hc, acc); err != nil {
							return nil, err
						}

						hc.Log().Info("withdrew", slog.Int("amount", cmd.Amount), slog.Int("balance", acc.Balance()))
						return &BalanceResponse{Balance: acc.Balance(), Owner: acc.Owner()}, nil
					},
				),

				actor.HandleRequest[GetBalance, BalanceResponse](
					func(hc actor.HandlerCtx, _ GetBalance) (*BalanceResponse, error) {
						acc, err := repo.GetByID(hc, accountID)
						if err != nil {
							return nil, err
						}

						hc.Log().Info("get balance", slog.Int("balance", acc.Balance()))
						return &BalanceResponse{Balance: acc.Balance(), Owner: acc.Owner()}, nil
					},
				),
			),
		), nil
	})
}

// =============================================================================
// Infrastructure
// =============================================================================

func startNATSContainer(ctx context.Context, log *slog.Logger) (string, func(), error) {
	natsC, err := testcontainers.Run(
		ctx, "nats:latest",
		testcontainers.WithCmd("-js"),
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp"),
			wait.ForLog("Server is ready"),
		),
	)
	if err != nil {
		return "", nil, fmt.Errorf("start container: %w", err)
	}

	ip, err := natsC.ContainerIP(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(natsC)
		return "", nil, fmt.Errorf("get container IP: %w", err)
	}

	natsURL := fmt.Sprintf("nats://%s:4222", ip)
	cleanup := func() {
		log.Info("terminating NATS container...")
		if err := testcontainers.TerminateContainer(natsC); err != nil {
			log.Error("failed to terminate container", slog.Any("error", err))
		}
	}

	return natsURL, cleanup, nil
}
