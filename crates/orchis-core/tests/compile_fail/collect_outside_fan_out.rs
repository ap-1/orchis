//! Calling .collect() outside a fan-out context must fail.

use orchis_core::{Pipeline, Task};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct A(u32);

#[derive(Serialize, Deserialize)]
struct B(String);

struct StepA;
impl Task for StepA {
    type Input = A;
    type Output = B;
    const NAME: &'static str = "step_a";
}

fn main() {
    let broker = todo!();
    let _ = Pipeline::new(broker).first::<StepA>().collect(); // ERROR: Sequential has no .collect()
}
