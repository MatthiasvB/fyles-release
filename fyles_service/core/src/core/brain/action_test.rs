#[cfg(any(test, feature = "test-support"))]
use crate::core::brain::{types::BrainRequest, ActionInterceptor};

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
pub enum TestAction {
    RegisterActionInterceptor(BrainRequest<ActionInterceptor, ()>),
}
