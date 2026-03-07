# Overview

Orchis is a Rust library for building typed pipelines over distributed task queues. You chain tasks together declaratively, and the compiler verifies that every step's output type matches the next step's input. At runtime, each step executes on a remote worker via Redis Streams, and a middleware layer automatically advances the pipeline between steps.

```rust
let handle = Pipeline::new(broker)
    .first::<FindTargets>()
    .fan_out()
    .then::<ScanTarget>()
    .collect()
    .then::<GenerateReport>()
    .dispatch(())
    .await?;

let report: Report = handle.wait_result().await?;
```

The pipeline above finds targets, scans each one in parallel across workers, gathers the results, and generates a report. If `ScanTarget` expects a `Target` but `FindTargets` returns a `Vec<String>`, the code won't compile.

## Crates

- `orchis-core`: Broker-agnostic traits, operation types, typestate pipeline builder, and middleware.
- `orchis-redis`: Redis backend implementing `Broker` and `BarrierBackend` via fred, plus a stream-based worker with bounded concurrency.
