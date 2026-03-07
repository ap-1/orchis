# Observability

The middleware emits [tracing](https://docs.rs/tracing) spans at key advancement points. These are compatible with any tracing subscriber:

- `tracing-subscriber` for structured stdout/JSON logs
- `tracing-opentelemetry` for exporting to Jaeger, Tempo, or Datadog
- `tracing-appender` for file-based log output

## Spans

| Span | Fields | Emitted when |
|---|---|---|
| `post_save` | `task_id` | A completed task enters the middleware |
| `execute_step` | `step_index` | The middleware dispatches the next operation |
| `handle_fan_out_child` | `task_id` | A fan-out child completes |
| `gather_results` | `child_count` | Barrier fires and results are fetched |

Spans nest naturally: a `post_save` span contains an `execute_step` span, which may contain a `gather_results` span. This gives a trace tree of the full pipeline advancement.

## Setup

Orchis does nothing visible until you wire up a subscriber. In your worker binary:

```rust
use tracing_subscriber::{fmt, EnvFilter};

fn main() {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    // ... start worker
}
```

Then run with `RUST_LOG=orchis_core=debug` to see middleware spans.

For OpenTelemetry export:

```rust
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap();

    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    // ... start worker
}
```

Each pipeline invocation shares a `_run_id` label across all tasks, which you can use to correlate spans across workers.
