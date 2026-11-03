# Orchis

<!-- ANCHOR: overview -->
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
- `orchis-macros`: `#[task("name")]` attribute that derives a `Task` implementation from an async function signature.
<!-- ANCHOR_END: overview -->

## Example

[`examples/pipeline_demo.rs`](examples/pipeline_demo.rs) runs a fan-out/collect pipeline against Redis on `localhost:6379` (override with `REDIS_URL`):

```sh
cargo run --bin pipeline-demo -- worker    # terminal 1
cargo run --bin pipeline-demo -- dispatch  # terminal 2
```

Concepts, patterns, and the operations reference are in the [docs](https://ap-1.github.io/orchis/).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE-2.0](LICENSE-APACHE-2.0))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
