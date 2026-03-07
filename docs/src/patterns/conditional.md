# Conditional Logic

Orchis pipelines are linear: there is no built-in `if`/`else` or branch combinator. This is intentional. Conditional branching at the pipeline level would require the type system to encode the merge point of two branches with potentially different output types, adding significant complexity for a use case that is better handled inside tasks.

## The pattern: branch inside the task

Model the decision as an enum and dispatch both the decision and the execution as sequential tasks:

```rust
#[derive(Serialize, Deserialize)]
enum ScanStrategy {
    Deep(DeepScanInput),
    Shallow(ShallowScanInput),
}

struct DecideScanStrategy;
impl Task for DecideScanStrategy {
    type Input = Target;
    type Output = ScanStrategy;
    const NAME: &'static str = "scanner::decide_strategy";
}

struct ExecuteScan;
impl Task for ExecuteScan {
    type Input = ScanStrategy;
    type Output = ScanResult;
    const NAME: &'static str = "scanner::execute_scan";
}
```

The decision task returns the enum variant:

```rust
worker.register::<DecideScanStrategy>(|target| async move {
    if target.is_high_priority {
        ScanStrategy::Deep(DeepScanInput { target, depth: 5 })
    } else {
        ScanStrategy::Shallow(ShallowScanInput { target })
    }
});
```

The execution task matches on it:

```rust
worker.register::<ExecuteScan>(|strategy| async move {
    match strategy {
        ScanStrategy::Deep(input) => deep_scan(input).await,
        ScanStrategy::Shallow(input) => shallow_scan(input).await,
    }
});
```

The pipeline stays linear:

```rust
Pipeline::new(broker)
    .first::<DecideScanStrategy>()
    .then::<ExecuteScan>()
    .dispatch(target)
    .await?;
```

## Why this works

- The pipeline type signature is unambiguous: `Target -> ScanStrategy -> ScanResult`.
- The branching logic lives in the task where it can access runtime context.
- Both branches produce the same output type (`ScanResult`), so the pipeline can continue.
- If the branches need different output types, wrap them in an enum and let the next task handle it the same way.

## When you need truly separate pipelines

If two branches have completely different downstream processing (different number of steps, different output types, different fan-out patterns), dispatch them as separate pipelines from within a task:

```rust
worker.register::<Router>(|input| async move {
    if input.needs_deep_scan {
        // Dispatch a separate pipeline for deep scanning
        Pipeline::new(broker.clone())
            .first::<DeepScan>()
            .then::<DeepAnalysis>()
            .dispatch(input)
            .await
            .unwrap();
    } else {
        Pipeline::new(broker.clone())
            .first::<ShallowScan>()
            .dispatch(input)
            .await
            .unwrap();
    }
});
```
