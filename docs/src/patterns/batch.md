# Chunked Fan-Out

## The problem

`fan_out()` dispatches one task per item. For large iterables (thousands of URLs, database rows, etc.), this creates one Redis stream message per item, one result key per item, and one barrier counter increment per item. The overhead per task can exceed the cost of the actual work.

## The solution: `fan_out_chunked`

A chunked fan-out groups items into fixed-size batches and dispatches one task per batch:

```rust
Pipeline::new(broker)
    .first::<FindUrls>()              // () -> Vec<Url>
    .fan_out_chunked::<Url>(100)      // Vec<Url> -> chunks of Vec<Url>
    .then::<ProcessUrlBatch>()        // Vec<Url> -> Vec<PageResult>
    .collect()                        // Vec<Vec<PageResult>>
    .then::<Flatten>()                // Vec<Vec<PageResult>> -> Vec<PageResult>
    .dispatch(())
    .await?;
```

## Why runtime, not const generic

The batch size is a runtime parameter (`fan_out_chunked::<Url>(100)`), not a const generic (`fan_out_chunked::<100>()`). Batch size is typically a tuning knob read from configuration, not something you want baked into the type system.

## Type implications

With regular `fan_out()`, per-item tasks receive `Item`. With chunked fan-out, per-item tasks receive `Vec<Item>`. The type signature changes:

```text
fan_out()                    -> FanOutHead<B, In, Item>
fan_out_chunked::<Item>(n)   -> FanOutHead<B, In, Vec<Item>>
```

The collect step then gathers `Vec<Vec<Result>>` instead of `Vec<Result>`, so you typically need a flatten step afterwards.

## Implementation

The chunking happens inside the middleware's `execute_fan_out`. `FanOutOp` has an optional `chunk_size: Option<usize>` field. When set, the middleware partitions the iterable into chunks via `Vec::chunks` before dispatching children. The barrier counts chunks, not individual items.
