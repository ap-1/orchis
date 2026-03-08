//! Fan-out/collect pipeline example.
//!
//! Fetches pages, extracts content from each in parallel, collects results,
//! and generates a summary.
//!
//! Requires Redis on localhost:6379 (or set REDIS_URL).
//!
//! Run in one terminal:
//!   cargo run --bin pipeline-demo -- worker
//!
//! Run in another:
//!   cargo run --bin pipeline-demo -- dispatch

use std::sync::Arc;
use std::time::Duration;

use orchis_core::Pipeline;
use orchis_core::task::Task;
use orchis_redis::{RedisBarrier, RedisBroker, RedisConfig, RedisWorkerBuilder, WorkerConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Page {
    url: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PageContent {
    url: String,
    entries: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Summary {
    total_pages: usize,
    total_entries: usize,
    details: Vec<PageContent>,
}

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

struct GenerateSummary;
impl Task for GenerateSummary {
    type Input = Vec<PageContent>;
    type Output = Summary;
    const NAME: &'static str = "processor::generate_summary";
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

    builder.register::<FetchPages, _, _>(|_| async {
        tracing::info!("fetching pages");
        vec![
            Page {
                url: "https://example.com".into(),
            },
            Page {
                url: "https://blog.example.com".into(),
            },
            Page {
                url: "https://docs.example.com".into(),
            },
        ]
    });

    builder.register::<ExtractContent, _, _>(|page: Page| async move {
        tracing::info!(url = page.url, "extracting content");
        PageContent {
            entries: vec![
                format!("title extracted from {}", page.url),
                format!("body text extracted from {}", page.url),
            ],
            url: page.url,
        }
    });

    builder.register::<GenerateSummary, _, _>(|results: Vec<PageContent>| async move {
        let total_entries: usize = results.iter().map(|r| r.entries.len()).sum();
        tracing::info!(
            pages = results.len(),
            entries = total_entries,
            "generating summary"
        );
        Summary {
            total_pages: results.len(),
            total_entries,
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
        .first::<FetchPages>()
        .fan_out()
        .then::<ExtractContent>()
        .collect()
        .then::<GenerateSummary>()
        .dispatch(())
        .await
        .unwrap();

    tracing::info!(
        task_id = handle.task_id(),
        "pipeline dispatched, waiting for result"
    );

    let summary: Summary = handle.wait_result(Duration::from_secs(30)).await.unwrap();

    println!("\n=== Pipeline Summary ===");
    println!("Pages processed: {}", summary.total_pages);
    println!("Total entries:   {}", summary.total_entries);
    for result in &summary.details {
        println!("\n  {}:", result.url);
        for entry in &result.entries {
            println!("    - {entry}");
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
            eprintln!("Usage: pipeline-demo <worker|dispatch>");
            eprintln!();
            eprintln!("  worker    Start a worker that processes pipeline tasks");
            eprintln!("  dispatch  Dispatch a processing pipeline and wait for results");
            std::process::exit(1);
        }
    }
}
