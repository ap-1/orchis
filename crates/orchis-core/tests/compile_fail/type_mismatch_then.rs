//! Calling .then::<T>() where T::Input != previous Output must fail.

use orchis_core::{Pipeline, Task};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct Alpha(u32);

#[derive(Serialize, Deserialize)]
struct Beta(String);

#[derive(Serialize, Deserialize)]
struct Gamma(bool);

struct StepA;
impl Task for StepA {
    type Input = Alpha;
    type Output = Beta;
    const NAME: &'static str = "step_a";
}

struct StepB;
impl Task for StepB {
    type Input = Gamma; // Gamma != Beta
    type Output = Alpha;
    const NAME: &'static str = "step_b";
}

fn main() {
    let broker = todo!();
    let _ = Pipeline::new(broker).first::<StepA>().then::<StepB>(); // ERROR: Gamma != Beta
}
