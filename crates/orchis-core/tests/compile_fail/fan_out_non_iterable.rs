//! Calling .fan_out() when the previous output is not iterable must fail.

use orchis_core::{Pipeline, Task};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct NotIterable(u32);

struct ProduceOne;
impl Task for ProduceOne {
    type Input = ();
    type Output = NotIterable; // not IntoIterator
    const NAME: &'static str = "produce_one";
}

struct Consume;
impl Task for Consume {
    type Input = NotIterable;
    type Output = ();
    const NAME: &'static str = "consume";
}

fn main() {
    let broker = todo!();
    let _ = Pipeline::new(broker)
        .first::<ProduceOne>()
        .fan_out() // ERROR: NotIterable is not IntoIterator
        .then::<Consume>();
}
