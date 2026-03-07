//! Round-trip serialisation tests for all StepOp variants.

use std::collections::HashMap;

use orchis_core::ops::{
    CollectOp, FanOutOp, FlatMapOp, RaceOp, ReduceOp, SinkOp, StepOp, TapOp, ThenOp, WaitOp,
    deserialize_ops, serialize_ops,
};

fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn then_op_round_trip() {
    let op = StepOp::Then(ThenOp {
        task_id: "tid-1".to_owned(),
        task_name: "app::scan".to_owned(),
        extra_labels: labels(&[("queue", "fast")]),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"then""#));
}

#[test]
fn fan_out_op_round_trip() {
    let op = StepOp::FanOut(FanOutOp {
        task_id: "tid-2".to_owned(),
        terminator_index: 5,
        chunk_size: None,
        extra_labels: HashMap::new(),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"fan_out""#));
}

#[test]
fn flat_map_op_round_trip() {
    let op = StepOp::FlatMap(FlatMapOp {
        task_id: "tid-3".to_owned(),
        task_name: "app::expand".to_owned(),
        extra_labels: HashMap::new(),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"flat_map""#));
}

#[test]
fn collect_op_round_trip() {
    let op = StepOp::Collect(CollectOp {
        task_id: "tid-4".to_owned(),
        extra_labels: HashMap::new(),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"collect""#));
}

#[test]
fn reduce_op_round_trip() {
    let op = StepOp::Reduce(ReduceOp {
        task_id: "tid-5".to_owned(),
        task_name: "app::fold".to_owned(),
        initial: serde_json::json!(0),
        extra_labels: labels(&[("priority", "high")]),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"reduce""#));
}

#[test]
fn sink_op_round_trip() {
    let op = StepOp::Sink(SinkOp {
        task_id: "tid-6".to_owned(),
        task_name: "app::notify".to_owned(),
        extra_labels: HashMap::new(),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"sink""#));
}

#[test]
fn tap_op_round_trip() {
    let op = StepOp::Tap(TapOp {
        task_id: "tid-7".to_owned(),
        task_name: "app::audit".to_owned(),
        extra_labels: HashMap::new(),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"tap""#));
}

#[test]
fn wait_op_round_trip() {
    let op = StepOp::Wait(WaitOp {
        task_id: "tid-8".to_owned(),
        extra_labels: HashMap::new(),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"wait""#));
}

#[test]
fn race_op_round_trip() {
    let op = StepOp::Race(RaceOp {
        task_id: "tid-9".to_owned(),
        extra_labels: HashMap::new(),
    });
    let json = serde_json::to_string(&op).unwrap();
    let restored: StepOp = serde_json::from_str(&json).unwrap();
    assert_eq!(op, restored);
    assert!(json.contains(r#""op_type":"race""#));
}

#[test]
fn serialize_deserialize_ops_list() {
    let ops = vec![
        StepOp::Then(ThenOp {
            task_id: "a".to_owned(),
            task_name: "step_a".to_owned(),
            extra_labels: HashMap::new(),
        }),
        StepOp::FanOut(FanOutOp {
            task_id: "b".to_owned(),
            terminator_index: 3,
            chunk_size: None,
            extra_labels: HashMap::new(),
        }),
        StepOp::Then(ThenOp {
            task_id: "c".to_owned(),
            task_name: "step_c".to_owned(),
            extra_labels: HashMap::new(),
        }),
        StepOp::Collect(CollectOp {
            task_id: "d".to_owned(),
            extra_labels: HashMap::new(),
        }),
    ];

    let json = serialize_ops(&ops).unwrap();
    let restored = deserialize_ops(&json).unwrap();
    assert_eq!(ops, restored);
}

#[test]
fn step_op_task_id_accessor() {
    let op = StepOp::Then(ThenOp {
        task_id: "my-id".to_owned(),
        task_name: "t".to_owned(),
        extra_labels: HashMap::new(),
    });
    assert_eq!(op.task_id(), "my-id");
}
