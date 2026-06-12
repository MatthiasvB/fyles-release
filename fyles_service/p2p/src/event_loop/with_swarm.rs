use crate::event_loop::{EventLoopSender, InnerEvent, LocalNetworkSwarm};
use async_trait::async_trait;
use tokio::sync::oneshot;
use tracing::{instrument, trace, Span};

pub trait GetInnerEventSender<T: LocalNetworkSwarm> {
    fn get_inner_event_sender(&self) -> &EventLoopSender<T>;
}

#[async_trait]
pub trait WithSwarm<T> {
    fn with_swarm(&self, block: impl FnOnce(&mut T) + Send + Sync + 'static);

    async fn with_swarm_res<R, F>(&self, block: F) -> R
    where
        R: Send + Sync + 'static,
        // explicit lifetime 'a needed only due to usage of async_trait and the way it desugars this signature
        F: for<'a> FnOnce(&'a mut T) -> R + Send + Sync + 'static;
}

#[async_trait]
impl<S: LocalNetworkSwarm + 'static, T: GetInnerEventSender<S> + Sync> WithSwarm<S> for T {
    fn with_swarm(&self, block: impl FnOnce(&mut S) + Send + Sync + 'static) {
        trace!("Sending workload to swarm");
        let _ = self.get_inner_event_sender().send(InnerEvent::DoWithSwarm {
            block: Box::new(block),
            span: Span::current(),
        });
    }

    #[instrument(skip_all, level = "trace")]
    async fn with_swarm_res<R, F>(&self, block: F) -> R
    where
        R: Send + Sync + 'static,
        // explicit lifetime 'a needed only due to usage of async_trait and the way it desugars this signature
        F: for<'a> FnOnce(&'a mut S) -> R + Send + Sync + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        trace!("Sending workload to swarm");
        let _ = self.get_inner_event_sender().send(InnerEvent::DoWithSwarm {
            block: Box::new(|swarm| {
                let res = block(swarm);
                let _ = sender.send(res);
            }),
            span: Span::current(),
        });
        trace!("Sent");
        receiver.await.expect("Response channel not to be dropped")
    }
}
