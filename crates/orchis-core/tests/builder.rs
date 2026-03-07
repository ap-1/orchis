//! Tests that the pipeline builder produces the correct ops sequence.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use orchis_core::Pipeline;
use orchis_core::broker::Broker;
use orchis_core::error::BrokerError;
use orchis_core::ops::StepOp;
use orchis_core::result::TaskResult;
use orchis_core::task::Task;

struct MockBroker {
    counter: AtomicU64,
}

impl MockBroker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            counter: AtomicU64::new(0),
        })
    }
}

impl Broker for MockBroker {
    fn generate_id(&self) -> String {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("id-{n}")
    }

    fn dispatch(
        &self,
        _task_name: &str,
        _task_id: &str,
        _payload: Bytes,
        _labels: HashMap<String, String>,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send {
        async { Ok(()) }
    }

    fn set_result(
        &self,
        _task_id: &str,
        _result: TaskResult,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send {
        async { Ok(()) }
    }

    fn get_result(
        &self,
        _task_id: &str,
    ) -> impl Future<Output = Result<Option<TaskResult>, BrokerError>> + Send {
        async { Ok(None) }
    }

    fn wait_result(
        &self,
        _task_id: &str,
        _timeout: Option<Duration>,
    ) -> impl Future<Output = Result<TaskResult, BrokerError>> + Send {
        async { Err(BrokerError::Timeout) }
    }
}

#[derive(Serialize, Deserialize)]
struct Input(String);

#[derive(Serialize, Deserialize)]
struct Middle(u32);

#[derive(Serialize, Deserialize)]
struct Output(bool);

#[derive(Serialize, Deserialize)]
struct Item(i64);

#[derive(Serialize, Deserialize)]
struct Processed(String);

struct TaskA;
impl Task for TaskA {
    type Input = Input;
    type Output = Middle;
    const NAME: &'static str = "test::task_a";
}

struct TaskB;
impl Task for TaskB {
    type Input = Middle;
    type Output = Output;
    const NAME: &'static str = "test::task_b";
}

struct ProduceItems;
impl Task for ProduceItems {
    type Input = Input;
    type Output = Vec<Item>;
    const NAME: &'static str = "test::produce_items";
}

struct ProcessItem;
impl Task for ProcessItem {
    type Input = Item;
    type Output = Processed;
    const NAME: &'static str = "test::process_item";
}

struct Aggregate;
impl Task for Aggregate {
    type Input = Vec<Processed>;
    type Output = Output;
    const NAME: &'static str = "test::aggregate";
}

struct ExpandItem;
impl Task for ExpandItem {
    type Input = Item;
    type Output = Vec<Processed>;
    const NAME: &'static str = "test::expand_item";
}

struct Reducer;
impl Task for Reducer {
    type Input = (u64, Processed);
    type Output = u64;
    const NAME: &'static str = "test::reducer";
}

struct FireAndForget;
impl Task for FireAndForget {
    type Input = Output;
    type Output = ();
    const NAME: &'static str = "test::fire_and_forget";
}

// Helper to extract ops from a builder without dispatching

/// Access the internal ops list via a test-only dispatch that captures
/// the serialised pipeline from labels.
fn extract_ops(labels: &HashMap<String, String>) -> Vec<StepOp> {
    let raw = labels.get("_pipeline").expect("missing _pipeline label");
    orchis_core::ops::deserialize_ops(raw).expect("invalid pipeline JSON")
}

#[tokio::test]
async fn sequential_chain() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<TaskA>()
        .then::<TaskB>()
        .dispatch(Input("hello".to_owned()))
        .await
        .unwrap();

    // The handle's task_id should be the last op's id.
    assert_eq!(handle.task_id(), "id-1");
}

#[tokio::test]
async fn fan_out_collect() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<ProduceItems>()
        .fan_out()
        .then::<ProcessItem>()
        .collect()
        .then::<Aggregate>()
        .dispatch(Input("go".to_owned()))
        .await
        .unwrap();

    // 5 ops: ProduceItems, FanOut, ProcessItem, Collect, Aggregate
    // IDs: id-0, id-1, id-2, id-3, id-4
    assert_eq!(handle.task_id(), "id-4");
}

#[tokio::test]
async fn fan_out_backfills_terminator_index() {
    use orchis_core::ops::StepOp;
    use std::sync::{Arc as StdArc, Mutex};

    // Capture dispatched labels to inspect ops.
    let captured: StdArc<Mutex<Option<HashMap<String, String>>>> = StdArc::new(Mutex::new(None));
    let captured_clone = StdArc::clone(&captured);

    struct CaptureBroker {
        counter: AtomicU64,
        captured: StdArc<Mutex<Option<HashMap<String, String>>>>,
    }

    impl Broker for CaptureBroker {
        fn generate_id(&self) -> String {
            let n = self.counter.fetch_add(1, Ordering::Relaxed);
            format!("id-{n}")
        }

        fn dispatch(
            &self,
            _task_name: &str,
            _task_id: &str,
            _payload: Bytes,
            labels: HashMap<String, String>,
        ) -> impl Future<Output = Result<(), BrokerError>> + Send {
            *self.captured.lock().unwrap() = Some(labels);
            async { Ok(()) }
        }

        fn set_result(
            &self,
            _task_id: &str,
            _result: TaskResult,
        ) -> impl Future<Output = Result<(), BrokerError>> + Send {
            async { Ok(()) }
        }

        fn get_result(
            &self,
            _task_id: &str,
        ) -> impl Future<Output = Result<Option<TaskResult>, BrokerError>> + Send {
            async { Ok(None) }
        }

        fn wait_result(
            &self,
            _task_id: &str,
            _timeout: Option<Duration>,
        ) -> impl Future<Output = Result<TaskResult, BrokerError>> + Send {
            async { Err(BrokerError::Timeout) }
        }
    }

    let broker = Arc::new(CaptureBroker {
        counter: AtomicU64::new(0),
        captured: captured_clone,
    });

    let _handle = Pipeline::new(Arc::clone(&broker))
        .first::<ProduceItems>()
        .fan_out()
        .then::<ProcessItem>()
        .collect()
        .then::<Aggregate>()
        .dispatch(Input("x".to_owned()))
        .await
        .unwrap();

    let labels = captured.lock().unwrap().take().unwrap();
    let ops = extract_ops(&labels);

    assert_eq!(ops.len(), 5);

    // ops[0] = ThenOp(ProduceItems)
    assert!(matches!(&ops[0], StepOp::Then(t) if t.task_name == "test::produce_items"));

    // ops[1] = FanOutOp with terminator_index = 3 (CollectOp)
    match &ops[1] {
        StepOp::FanOut(fo) => assert_eq!(fo.terminator_index, 3),
        other => panic!("expected FanOutOp, got {other:?}"),
    }

    // ops[2] = ThenOp(ProcessItem)
    assert!(matches!(&ops[2], StepOp::Then(t) if t.task_name == "test::process_item"));

    // ops[3] = CollectOp
    assert!(matches!(&ops[3], StepOp::Collect(_)));

    // ops[4] = ThenOp(Aggregate)
    assert!(matches!(&ops[4], StepOp::Then(t) if t.task_name == "test::aggregate"));
}

#[tokio::test]
async fn fan_out_reduce() {
    let broker = MockBroker::new();
    let _handle = Pipeline::new(Arc::clone(&broker))
        .first::<ProduceItems>()
        .fan_out()
        .then::<ProcessItem>()
        .reduce::<Reducer, u64>(0u64)
        .dispatch(Input("r".to_owned()))
        .await
        .unwrap();

    // 4 ops: ProduceItems, FanOut, ProcessItem, Reduce
    // Last op task_id = id-3
    assert_eq!(_handle.task_id(), "id-3");
}

struct SinkProcessed;
impl Task for SinkProcessed {
    type Input = Processed;
    type Output = ();
    const NAME: &'static str = "test::sink_processed";
}

#[tokio::test]
async fn fan_out_sink() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<ProduceItems>()
        .fan_out()
        .then::<ProcessItem>()
        .sink::<SinkProcessed>()
        .dispatch(Input("s".to_owned()))
        .await
        .unwrap();

    // 4 ops: ProduceItems, FanOut, ProcessItem, Sink
    assert_eq!(handle.task_id(), "id-3");
}

struct ProcessProcessed;
impl Task for ProcessProcessed {
    type Input = Processed;
    type Output = Output;
    const NAME: &'static str = "test::process_processed";
}

#[tokio::test]
async fn flat_map_with_correct_types() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<ProduceItems>()
        .fan_out()
        .flat_map::<ExpandItem, Processed>()
        .then::<ProcessProcessed>()
        .collect()
        .dispatch(Input("fm".to_owned()))
        .await
        .unwrap();

    // 5 ops: ProduceItems, FanOut, FlatMap(ExpandItem), Then(ProcessProcessed), Collect
    assert_eq!(handle.task_id(), "id-4");
}

#[tokio::test]
async fn sequential_sink() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<TaskA>()
        .then::<TaskB>()
        .sink::<FireAndForget>()
        .dispatch(Input("sink".to_owned()))
        .await
        .unwrap();

    // 3 ops: TaskA, TaskB, Sink
    assert_eq!(handle.task_id(), "id-2");
}

struct AuditTask;
impl Task for AuditTask {
    type Input = Middle;
    type Output = ();
    const NAME: &'static str = "test::audit";
}

#[tokio::test]
async fn sequential_tap() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<TaskA>()
        .tap::<AuditTask>()
        .then::<TaskB>()
        .dispatch(Input("tap".to_owned()))
        .await
        .unwrap();

    // 3 ops: TaskA, Tap(Audit), TaskB
    // tap doesn't change the output type, so last op is TaskB at id-2
    assert_eq!(handle.task_id(), "id-2");
}

struct WaitAfterTask;
impl Task for WaitAfterTask {
    type Input = ();
    type Output = Output;
    const NAME: &'static str = "test::wait_after";
}

#[tokio::test]
async fn fan_out_wait() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<ProduceItems>()
        .fan_out()
        .then::<ProcessItem>()
        .wait()
        .then::<WaitAfterTask>()
        .dispatch(Input("wait".to_owned()))
        .await
        .unwrap();

    // 5 ops: ProduceItems, FanOut, ProcessItem, Wait, WaitAfterTask
    assert_eq!(handle.task_id(), "id-4");
}

struct AuditProcessed;
impl Task for AuditProcessed {
    type Input = Processed;
    type Output = ();
    const NAME: &'static str = "test::audit_processed";
}

#[tokio::test]
async fn fan_out_tap() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<ProduceItems>()
        .fan_out()
        .then::<ProcessItem>()
        .tap::<AuditProcessed>()
        .collect()
        .dispatch(Input("fan_tap".to_owned()))
        .await
        .unwrap();

    // 5 ops: ProduceItems, FanOut, ProcessItem, Tap(Audit), Collect
    assert_eq!(handle.task_id(), "id-4");
}

struct AfterRace;
impl Task for AfterRace {
    type Input = Processed;
    type Output = Output;
    const NAME: &'static str = "test::after_race";
}

#[tokio::test]
async fn fan_out_race() {
    let broker = MockBroker::new();
    let handle = Pipeline::new(Arc::clone(&broker))
        .first::<ProduceItems>()
        .fan_out()
        .then::<ProcessItem>()
        .race()
        .then::<AfterRace>()
        .dispatch(Input("race".to_owned()))
        .await
        .unwrap();

    // 5 ops: ProduceItems, FanOut, ProcessItem, Race, AfterRace
    assert_eq!(handle.task_id(), "id-4");
}
