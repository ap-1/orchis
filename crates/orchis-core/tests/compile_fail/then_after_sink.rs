//! Calling .then() after .sink() must fail because Terminated has no .then().

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

struct StepSink;
impl Task for StepSink {
    type Input = B;
    type Output = ();
    const NAME: &'static str = "step_sink";
}

struct StepC;
impl Task for StepC {
    type Input = ();
    type Output = A;
    const NAME: &'static str = "step_c";
}

fn main() {
    let broker = todo!();
    let _ = Pipeline::new(broker)
        .first::<StepA>()
        .sink::<StepSink>()
        .then::<StepC>(); // ERROR: Terminated has no .then()
}
