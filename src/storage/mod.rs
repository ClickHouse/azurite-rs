//! Storage layer for persistence.

mod extent;
mod gc;
mod metadata;

pub use extent::*;
pub use gc::*;
pub use metadata::*;
