# Conditional Logic

Orchis pipelines are linear: there is no built-in `if`/`else` or branch combinator. This is intentional. Conditional branching at the pipeline level would require the type system to encode the merge point of two branches with potentially different output types, adding significant complexity for a use case that is better handled inside tasks.

## The pattern: branch inside the task

Model the decision as an enum and dispatch both the decision and the execution as sequential tasks:

```rust
#[derive(Serialize, Deserialize)]
enum ProcessingStrategy {
    Full(FullProcessingInput),
    Quick(QuickProcessingInput),
}

struct DecideStrategy;
impl Task for DecideStrategy {
    type Input = Page;
    type Output = ProcessingStrategy;
    const NAME: &'static str = "processor::decide_strategy";
}

struct ExecuteProcessing;
impl Task for ExecuteProcessing {
    type Input = ProcessingStrategy;
    type Output = ProcessingResult;
    const NAME: &'static str = "processor::execute";
}
```

The decision task returns the enum variant:

```rust
worker.register::<DecideStrategy>(|page| async move {
    if page.is_high_priority {
        ProcessingStrategy::Full(FullProcessingInput { page, depth: 5 })
    } else {
        ProcessingStrategy::Quick(QuickProcessingInput { page })
    }
});
```

The execution task matches on it:

```rust
worker.register::<ExecuteProcessing>(|strategy| async move {
    match strategy {
        ProcessingStrategy::Full(input) => full_process(input).await,
        ProcessingStrategy::Quick(input) => quick_process(input).await,
    }
});
```

The pipeline stays linear:

```rust
Pipeline::new(broker)
    .first::<DecideStrategy>()
    .then::<ExecuteProcessing>()
    .dispatch(page)
    .await?;
```

## Why this works

- The pipeline type signature is unambiguous: `Page -> ProcessingStrategy -> ProcessingResult`.
- The branching logic lives in the task where it can access runtime context.
- Both branches produce the same output type (`ProcessingResult`), so the pipeline can continue.
- If the branches need different output types, wrap them in an enum and let the next task handle it the same way.

## Partitioning items by type

A common variant: a fan-out produces items of different categories, and you want to process each category differently. The same enum pattern applies, but inside the per-item task:

```rust
#[derive(Serialize, Deserialize)]
enum ContentItem {
    Text(TextData),
    Image(ImageData),
    Video(VideoData),
}

#[derive(Serialize, Deserialize)]
struct ContentResult { /* ... */ }

struct ProcessContent;
impl Task for ProcessContent {
    type Input = ContentItem;
    type Output = ContentResult;
    const NAME: &'static str = "processor::process_content";
}
```

The handler routes internally:

```rust
worker.register::<ProcessContent>(|item| async move {
    match item {
        ContentItem::Text(data) => process_text(data).await,
        ContentItem::Image(data) => process_image(data).await,
        ContentItem::Video(data) => process_video(data).await,
    }
});
```

The pipeline fans out over mixed content items, and each item is routed to the right handler logic:

```rust
Pipeline::new(broker)
    .first::<FetchContentItems>()  // () -> Vec<ContentItem>
    .fan_out()
    .then::<ProcessContent>()      // ContentItem -> ContentResult (routes internally)
    .collect()
    .then::<AggregateResults>()
    .dispatch(())
    .await?;
```

This is simpler than a pipeline-level partition because the routing happens at runtime inside the task, the type signature stays clean (`ContentItem -> ContentResult`), and all branches produce the same output type for collect.

## When you need truly separate pipelines

If two branches have completely different downstream processing (different number of steps, different output types, different fan-out patterns), dispatch them as separate pipelines from within a task:

```rust
worker.register::<Router>(|input| async move {
    if input.needs_full_processing {
        Pipeline::new(broker.clone())
            .first::<FullProcess>()
            .then::<DetailedAnalysis>()
            .dispatch(input)
            .await
            .unwrap();
    } else {
        Pipeline::new(broker.clone())
            .first::<QuickProcess>()
            .dispatch(input)
            .await
            .unwrap();
    }
});
```
