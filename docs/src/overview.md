# Overview

Orchis is a Rust library for building typed pipelines over distributed task queues. You chain tasks together declaratively, and the compiler verifies that every step's output type matches the next step's input. At runtime, each step executes on a remote worker via Redis Streams, and a middleware layer automatically advances the pipeline between steps.

```rust
let handle = Pipeline::new(broker)
    .first::<FetchPages>()
    .fan_out()
    .then::<ExtractContent>()
    .collect()
    .then::<GenerateSummary>()
    .dispatch(())
    .await?;

let summary: Summary = handle.wait_result(Duration::from_secs(60)).await?;
```

The pipeline above fetches pages, extracts content from each one in parallel across workers, gathers the results, and generates a summary. If `ExtractContent` expects a `Page` but `FetchPages` returns a `Vec<String>`, the code won't compile.

## Crates

- `orchis-core`: Broker-agnostic traits, operation types, typestate pipeline builder, and middleware.
- `orchis-redis`: Redis backend implementing `Broker` and `BarrierBackend` via fred, plus a stream-based worker with bounded concurrency.
