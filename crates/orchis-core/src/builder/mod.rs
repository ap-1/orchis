//! Typed pipeline builder using the typestate pattern.
//!
//! The builder uses separate types per state so that invalid
//! combinator sequences are compile errors, not runtime exceptions.
//! Output types are threaded through generic parameters so that
//! mismatched task signatures are caught by the type checker.
//!
//! # Typestate Transitions
//!
//! ```text
//! Pipeline<B>
//!   .first::<T>()  ->  Sequential<B, T::Input, T::Output>
//!
//! Sequential<B, In, Out>
//!   .then::<T>()   ->  Sequential<B, In, T::Output>       where T::Input = Out
//!   .fan_out()     ->  FanOutHead<B, In, Out::Item>        where Out: IntoIterator
//!   .sink::<T>()   ->  Terminated<B, In>                   where T::Input = Out
//!   .dispatch(In)  ->  TaskHandle<B, Out>
//!
//! FanOutHead<B, In, Item>
//!   .then::<T>()     ->  InFanOut<B, In, Item, T::Output>  where T::Input = Item
//!   .flat_map::<T>() ->  InFanOut<B, In, Item, T::Output>  where T::Input = Item
//!   .sink::<T>()     ->  Terminated<B, In>                  where T::Input = Item
//!
//! InFanOut<B, In, Item, Out>
//!   .then::<T>()     ->  InFanOut<B, In, Item, T::Output>  where T::Input = Out
//!   .flat_map::<T>() ->  InFanOut<B, In, Item, NewItem>    where T::Input = Out
//!   .collect()       ->  Sequential<B, In, Vec<Out>>
//!   .reduce::<T>()   ->  Sequential<B, In, Acc>
//!   .sink::<T>()     ->  Terminated<B, In>                  where T::Input = Out
//!
//! Terminated<B, In>
//!   .dispatch(In)  ->  TaskHandle<B, ()>
//! ```

mod core;
mod dispatch;
pub mod fan_out;
pub mod pipeline;
pub mod sequential;
pub mod terminated;

pub use self::fan_out::{FanOutHead, InFanOut};
pub use self::pipeline::Pipeline;
pub use self::sequential::Sequential;
pub use self::terminated::Terminated;
