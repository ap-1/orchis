# Concepts

## Tasks

A task is a unit of work with a known input type, output type, and name. Tasks run on remote workers, not in the pipeline builder. The `Task` trait captures the type signature at compile time:

```rust
struct ScanTarget;
impl Task for ScanTarget {
    type Input = Target;
    type Output = ScanResult;
    const NAME: &'static str = "scanner::scan_target";
}
```

The `orchis-macros` crate provides `#[task]` to generate this from a function:

```rust
#[orchis::task("scanner::scan_target")]
async fn scan_target(input: Target) -> ScanResult {
    // ...
}
```

## Pipeline builder

The builder uses a typestate pattern: each method returns a different type, so invalid sequences are compile errors. For example, you cannot call `.collect()` outside a fan-out, or `.then()` after `.sink()`.

Output types thread through the builder generics. `.then::<T>()` requires `T::Input` to equal the previous step's output type. A mismatch is a compile error.

## Operations

Nine operation types:

| Op | Role |
|---|---|
| `then` | Sequential step |
| `tap` | Fire-and-forget side task (result unchanged) |
| `fan_out` | Split iterable into parallel children |
| `flat_map` | Nested fan-out within a barrier |
| `collect` | Gather all children into a list |
| `wait` | Wait for all children, discard results |
| `race` | Resolve on first child, discard the rest |
| `reduce` | Sequential fold over gathered results |
| `sink` | Fire-and-forget terminal |

## Middleware

When a task completes, the worker calls `PipelineMiddleware::post_save`. The middleware deserialises the pipeline from the task's message labels, determines the next step, and dispatches it. This is event-driven with no polling.

## Barriers

Fan-out operations use barriers to synchronise parallel children:

- **Simple barrier**: counter-based, fires when all children complete.
- **Flat-map barrier**: pending counter adjusted dynamically as expanders spawn new children.
- **Race barrier**: first-wins via atomic set-if-not-exists.
