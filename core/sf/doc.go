// Package sf provides a generic single-flight mechanism for deduplicating
// concurrent function calls with the same key.
//
// Single-flight ensures that only one execution of a function is in-flight
// for a given key at a time. If multiple goroutines call [Singleflight.Do]
// with the same key concurrently, only the first call executes the function;
// subsequent callers block until the first call completes and then receive
// the same result.
//
// This pattern is useful for:
//   - Preventing thundering herd problems on cache misses
//   - Deduplicating expensive operations like database queries or API calls
//   - Reducing load on backend services during traffic spikes
//
// # Usage
//
//	sf := sf.New[*User]()
//
//	// Multiple concurrent calls with the same key will only execute once
//	user, err := sf.Do("user:123", func() (*User, error) {
//	    return db.GetUser(ctx, "123")
//	})
//
// The generic type parameter T allows type-safe returns without casting.
package sf
