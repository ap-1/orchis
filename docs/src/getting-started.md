# Getting Started

## Define tasks

```rust
use orchis_core::Task;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Target { url: String }

#[derive(Serialize, Deserialize)]
struct ScanResult { findings: Vec<String> }

struct FindTargets;
impl Task for FindTargets {
    type Input = ();
    type Output = Vec<Target>;
    const NAME: &'static str = "recon::find_targets";
}

struct ScanTarget;
impl Task for ScanTarget {
    type Input = Target;
    type Output = ScanResult;
    const NAME: &'static str = "scanner::scan_target";
}

struct GenerateReport;
impl Task for GenerateReport {
    type Input = Vec<ScanResult>;
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
    .first::<FindTargets>()
    .fan_out()
    .then::<ScanTarget>()
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

builder.register::<FindTargets>(|_| async {
    vec![Target { url: "https://example.com".into() }]
});

builder.register::<ScanTarget>(|target| async move {
    ScanResult { findings: vec![format!("scanned {}", target.url)] }
});

builder.register::<GenerateReport>(|results| async move {
    format!("{} targets scanned", results.len())
});

let worker = builder.build();
worker.run(shutdown_token).await?;
```
