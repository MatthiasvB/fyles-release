#[cfg(any(test, feature = "test-support"))]
use crate::core::brain::{ActionInterceptor, types::BrainRequest};

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
pub enum TestAction {
    RegisterActionInterceptor(BrainRequest<ActionInterceptor, ()>),
}
