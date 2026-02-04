// Package app provides a high-level API for building cluster applications
// that combine actors with sharded message routing.
//
// The App type orchestrates a cluster [cluster.Node] and [cluster.Client]
// with actor-based message handling, providing a simple way to build
// horizontally scalable services.
//
// # Basic Usage
//
//	app, err := app.Run(app.Config{
//	    Node: app.NodeConfig{
//	        ID:        "node-1",
//	        NumShards: 64,
//	        Transport: natsTransport,
//	    },
//	},
//	    actor.HandleMsg[CreateUserCmd](handleCreateUser),
//	    actor.HandleRequest[GetUserQuery, *User](handleGetUser),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Send messages via the client
//	user, err := cluster.NewRequest[GetUserQuery, User](
//	    app.Client().Key("user:123"),
//	).Request(ctx, GetUserQuery{ID: "123"})
//
//	// Graceful shutdown
//	app.Shutdown(ctx)
//
// # Multi-Node Clusters
//
// For multi-node deployments, configure all nodes with the same NumShards
// and ShardSeed, and provide the full list of NodeIDs:
//
//	app.NodeConfig{
//	    ID:        myNodeID,
//	    NumShards: 256,
//	    NodeIDs:   []string{"node-1", "node-2", "node-3"},
//	    ShardSeed: "production",
//	    Transport: natsTransport,
//	}
//
// Shards are distributed across nodes using consistent hashing, so adding
// or removing nodes only redistributes a minimal number of shards.
//
// # Custom Actor Factory
//
// For advanced use cases, provide a custom CreateActor function:
//
//	app.Config{
//	    CreateActor: func(key string) (actor.Actor, error) {
//	        // Create actor with custom configuration based on key
//	        return actor.New(opts, handler), nil
//	    },
//	}
package app
