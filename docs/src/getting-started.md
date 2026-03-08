# Getting Started

## Define tasks

```rust
use orchis_core::Task;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Page { url: String }

#[derive(Serialize, Deserialize)]
struct PageContent { entries: Vec<String> }

struct FetchPages;
impl Task for FetchPages {
    type Input = ();
    type Output = Vec<Page>;
    const NAME: &'static str = "processor::fetch_pages";
}

struct ExtractContent;
impl Task for ExtractContent {
    type Input = Page;
    type Output = PageContent;
    const NAME: &'static str = "processor::extract_content";
}

struct GenerateReport;
impl Task for GenerateReport {
    type Input = Vec<PageContent>;
    type Output = String;
    const NAME: &'static str = "report::generate";
}
```

## Build and dispatch a pipeline

```rust
use orchis_core::Pipeline;
use orchis_redis::{RedisBroker, RedisBarrier, RedisConfig};

let config = RedisConfig::new("redis://127.0.0.1:6379");
let broker = Arc::new(RedisBroker::connect(config.clone()).await?);
let barrier = Arc::new(RedisBarrier::new(broker.client(), config));

let handle = Pipeline::new(Arc::clone(&broker))
    .first::<FetchPages>()
    .fan_out()
    .then::<ExtractContent>()
    .collect()
    .then::<GenerateReport>()
    .dispatch(())
    .await?;

let report: String = handle.wait_result(Duration::from_secs(60)).await?;
```

## Run a worker

```rust
use orchis_redis::{RedisWorkerBuilder, WorkerConfig};

let mut builder = RedisWorkerBuilder::new(broker, barrier, WorkerConfig::default());

builder.register::<FetchPages>(|_| async {
    vec![Page { url: "https://example.com".into() }]
});

builder.register::<ExtractContent>(|page| async move {
    PageContent { entries: vec![format!("processed {}", page.url)] }
});

builder.register::<GenerateReport>(|results| async move {
    format!("{} pages processed", results.len())
});

let worker = builder.build();
worker.run(shutdown_token).await?;
```
