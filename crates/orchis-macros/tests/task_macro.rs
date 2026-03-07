//! Tests for the #[task] proc macro.

use orchis_core::Task;
use orchis_macros::task;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Input(u64);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Output(String);

#[task("test::my_task")]
async fn my_task(input: Input) -> Output {
    Output(format!("processed {}", input.0))
}

#[test]
fn macro_generates_task_struct() {
    assert_eq!(MyTaskTask::NAME, "test::my_task");
}

#[test]
fn macro_preserves_function() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = rt.block_on(my_task(Input(42)));
    assert_eq!(result, Output("processed 42".to_owned()));
}

#[test]
fn macro_types_match_function_signature() {
    fn assert_input<T: Task<Input = Input>>() {}
    fn assert_output<T: Task<Output = Output>>() {}
    assert_input::<MyTaskTask>();
    assert_output::<MyTaskTask>();
}

#[task("test::snake_case_name")]
async fn compute_result(input: Input) -> Output {
    Output(format!("{}", input.0))
}

#[test]
fn macro_generates_pascal_case_struct() {
    assert_eq!(ComputeResultTask::NAME, "test::snake_case_name");
    // Verify the function is preserved alongside the struct.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = rt.block_on(compute_result(Input(7)));
    assert_eq!(result, Output("7".to_owned()));
}
