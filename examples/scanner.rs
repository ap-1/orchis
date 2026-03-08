//! Fan-out/collect scanner example.
//!
//! Discovers targets, scans each in parallel, collects findings,
//! and generates a report.
//!
//! Requires Redis on localhost:6379 (or set REDIS_URL).
//!
//! Run in one terminal:
//!   cargo run --bin scanner -- worker
//!
//! Run in another:
//!   cargo run --bin scanner -- dispatch

use std::sync::Arc;
use std::time::Duration;

use orchis_core::Pipeline;
use orchis_core::task::Task;
use orchis_redis::{RedisBarrier, RedisBroker, RedisConfig, RedisWorkerBuilder, WorkerConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Target {
    url: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ScanResult {
    url: String,
    findings: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Report {
    total_targets: usize,
    total_findings: usize,
    details: Vec<ScanResult>,
}

struct DiscoverTargets;
impl Task for DiscoverTargets {
    type Input = ();
    type Output = Vec<Target>;
    const NAME: &'static str = "scanner::discover_targets";
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
    type Output = Report;
    const NAME: &'static str = "scanner::generate_report";
}

async fn connect() -> (Arc<RedisBroker>, Arc<RedisBarrier>) {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let config = RedisConfig::new(url);
    let broker = Arc::new(RedisBroker::connect(config.clone()).await.unwrap());
    let barrier = Arc::new(RedisBarrier::new(broker.client(), config));
    (broker, barrier)
}

async fn run_worker(broker: Arc<RedisBroker>, barrier: Arc<RedisBarrier>) {
    let mut builder = RedisWorkerBuilder::new(broker, barrier, WorkerConfig::default());

    builder.register::<DiscoverTargets, _, _>(|_| async {
        tracing::info!("discovering targets");
        vec![
            Target {
                url: "https://example.com".into(),
            },
            Target {
                url: "https://test.example.com".into(),
            },
            Target {
                url: "https://api.example.com".into(),
            },
        ]
    });

    builder.register::<ScanTarget, _, _>(|target: Target| async move {
        tracing::info!(url = target.url, "scanning target");
        ScanResult {
            findings: vec![
                format!("open port 80 on {}", target.url),
                format!("TLS cert valid on {}", target.url),
            ],
            url: target.url,
        }
    });

    builder.register::<GenerateReport, _, _>(|results: Vec<ScanResult>| async move {
        let total_findings: usize = results.iter().map(|r| r.findings.len()).sum();
        tracing::info!(
            targets = results.len(),
            findings = total_findings,
            "generating report"
        );
        Report {
            total_targets: results.len(),
            total_findings,
            details: results,
        }
    });

    let worker = builder.build();
    tracing::info!("worker started, waiting for tasks");
    let token = tokio_util::sync::CancellationToken::new();
    worker.run(token).await.unwrap();
}

async fn dispatch_pipeline(broker: Arc<RedisBroker>) {
    let handle = Pipeline::new(broker)
        .first::<DiscoverTargets>()
        .fan_out()
        .then::<ScanTarget>()
        .collect()
        .then::<GenerateReport>()
        .dispatch(())
        .await
        .unwrap();

    tracing::info!(
        task_id = handle.task_id(),
        "pipeline dispatched, waiting for result"
    );

    let report: Report = handle.wait_result(Duration::from_secs(30)).await.unwrap();

    println!("\n=== Scan Report ===");
    println!("Targets scanned: {}", report.total_targets);
    println!("Total findings:  {}", report.total_findings);
    for result in &report.details {
        println!("\n  {}:", result.url);
        for finding in &result.findings {
            println!("    - {finding}");
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mode = std::env::args().nth(1).unwrap_or_else(|| "help".into());
    let (broker, barrier) = connect().await;

    match mode.as_str() {
        "worker" => run_worker(broker, barrier).await,
        "dispatch" => dispatch_pipeline(broker).await,
        _ => {
            eprintln!("Usage: scanner <worker|dispatch>");
            eprintln!();
            eprintln!("  worker    Start a worker that processes scan tasks");
            eprintln!("  dispatch  Dispatch a scan pipeline and wait for results");
            std::process::exit(1);
        }
    }
}
