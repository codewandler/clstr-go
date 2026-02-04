# Examples

Working examples demonstrating clstr's pillars in action.

| Example | Pillars | Description |
|---------|---------|-------------|
| [counter](./counter/) | Cluster + Actor | Distributed counter with HTTP API and Prometheus metrics |
| [accounting](./accounting/) | Cluster + Actor + ES | Event-sourced bank accounts with full persistence |
| [loadtest](./loadtest/) | Event Sourcing | Performance benchmark for aggregate operations |

## Quick start

```bash
# Interactive counter demo
cd examples/counter && go run .

# Automated accounting demo
cd examples/accounting && go run .

# Performance benchmark
cd examples/loadtest && go run .
```

See each example's README for detailed documentation.
