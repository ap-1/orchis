# Operations Reference

## Sequential operations

### `then`

Dispatches a single task with the previous step's result.

```rust
.first::<TaskA>()
.then::<TaskB>()    // TaskB::Input must equal TaskA::Output
```

### `tap`

Dispatches a fire-and-forget side task without affecting the pipeline result. The side task receives the current result but runs independently. The pipeline continues with the original result unchanged.

```rust
.first::<ProcessData>()
.tap::<AuditLog>()      // AuditLog runs in background
.then::<SaveResult>()   // receives ProcessData's output, not AuditLog's
```

Works in both sequential and fan-out contexts.

## Fan-out operations

### `fan_out`

Splits an iterable into parallel children. The previous step must return something that implements `IntoIterator`.

```rust
.first::<FindItems>()   // () -> Vec<Item>
.fan_out()               // dispatches one child per Item
.then::<ProcessItem>()   // Item -> Result
```

### `fan_out_chunked`

Like `fan_out`, but groups items into fixed-size batches before dispatching. Each per-item task receives `Vec<Item>` instead of `Item`. Batch size is a runtime parameter.

```rust
.first::<FindUrls>()              // () -> Vec<Url>
.fan_out_chunked::<Url>(100)      // dispatches one child per chunk of 100
.then::<ProcessUrlBatch>()        // Vec<Url> -> BatchResult
.collect()                        // Vec<BatchResult>
```

### `flat_map`

Like `then` inside a fan-out, but the task returns an iterable that spawns additional children within the same barrier. Enables nested parallelism without intermediate collect/flatten cycles.

```rust
.fan_out()
.flat_map::<ExpandItem>()  // Item -> Vec<SubItem>, each SubItem becomes a child
.then::<ProcessSubItem>()  // SubItem -> Result
.collect()                 // gathers ALL leaf results
```

## Terminator operations

Terminators close a fan-out context and return to sequential mode.

### `collect`

Gathers all parallel results into a `Vec`.

```rust
.fan_out()
.then::<Process>()
.collect()          // Sequential<B, In, Vec<ProcessOutput>>
```

### `wait`

Waits for all children to complete but discards their results. The next step receives a null payload.

```rust
.fan_out()
.then::<Process>()
.wait()             // Sequential<B, In, ()>
.then::<Notify>()   // Notify::Input = ()
```

### `race`

Resolves on the first child to complete. All other children's results are discarded. The winning child's result is forwarded.

```rust
.fan_out()
.then::<TryResolver>()
.race()             // Sequential<B, In, ResolverOutput>
.then::<UseResult>()
```

### `reduce`

Gathers results, then sequentially folds them with a reducer task. The reducer receives `(Accumulator, Item)` and returns `Accumulator`.

```rust
.fan_out()
.then::<Process>()
.reduce::<Fold, u64>(0)  // Sequential<B, In, u64>
```

### `sink`

Fire-and-forget terminal. Dispatches a task without waiting for its result. The pipeline resolves immediately with an empty payload.

```rust
.first::<Prepare>()
.sink::<Notify>()    // Terminated<B, In>
```

Can also terminate a fan-out, dispatching the sink task for each child independently.

## Modifiers

### `retry`

Sets a retry policy on the preceding `then` step. When the task returns an error, the worker re-dispatches it with the original input up to `max_retries` times before aborting the pipeline.

```rust
.first::<FetchData>()
.then::<ProcessData>().retry(3)   // retry up to 3 times on failure
.then::<SaveResult>()
```

Works in both sequential and fan-out contexts.
