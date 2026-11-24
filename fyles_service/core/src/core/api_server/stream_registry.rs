use std::{
    collections::HashMap,
    future::Future,
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::{select, sync::Mutex, sync::mpsc::error::TrySendError};
use tokio_stream::{StreamExt, StreamMap};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::{Status, Streaming};
use tracing::{Instrument, span, trace, warn};
use uuid::Uuid;
use zip_clone::ZipClone;

use crate::library::util::util::TimeoutLock;

pub type StreamId = Uuid;

pub struct StreamRegistryInner<S: Clone, C> {
    out_streams: HashMap<StreamId, tokio::sync::mpsc::Sender<S>>,
    stream_cmd_sender: tokio::sync::mpsc::Sender<StreamCommand<C>>,
    shutdown: CancellationToken,
}

#[allow(clippy::large_enum_variant)]
enum StreamCommand<C> {
    Add(StreamId, Streaming<C>),
    Remove(StreamId),
}

pub struct StreamRegistry<S: Clone, C>(Arc<Mutex<StreamRegistryInner<S, C>>>);

impl<S: Clone + Send + 'static, C: Send + 'static> StreamRegistry<S, C> {
    pub(super) fn new<H>(message_handler: H, shutdown: CancellationToken) -> Self
    where
        H: Fn(Option<(StreamId, Result<C, Status>)>) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + 'static,
    {
        let (stream_cmd_sender, mut stream_cmd_receiver) = tokio::sync::mpsc::channel(5);
        let registry = Self(Arc::new(Mutex::new(StreamRegistryInner {
            out_streams: Default::default(),
            stream_cmd_sender,
            shutdown,
        })));
        let mut in_streams: StreamMap<StreamId, Streaming<C>> = Default::default();
        tokio::task::spawn(
            async move {
                let handle_command =
                    |cmd: StreamCommand<C>, in_streams: &mut StreamMap<StreamId, Streaming<C>>| {
                        trace!("Received stream command");
                        match cmd {
                            StreamCommand::Add(stream_id, stream) => {
                                in_streams.insert(stream_id, stream);
                                trace!("Added stream {}", stream_id);
                            }
                            StreamCommand::Remove(stream_id) => {
                                in_streams.remove(&stream_id);
                                trace!("Removed stream {}", stream_id);
                            }
                        }
                    };

                loop {
                    trace!("Waiting for stream command or incoming message");
                    if in_streams.is_empty() {
                        // If no streams are registered, only wait for commands
                        if let Some(cmd) = stream_cmd_receiver.recv().await {
                            handle_command(cmd, &mut in_streams);
                        } else {
                            // Channel closed, exit the loop
                            break;
                        }
                    } else {
                        // If streams are registered, wait for either commands or messages
                        select! {
                            Some(cmd) = stream_cmd_receiver.recv() => {
                                handle_command(cmd, &mut in_streams);
                            }
                            message = in_streams.next() => {
                                trace!("Received incoming message");
                                let fut = message_handler(message);
                                tokio::task::spawn(fut);
                            }
                        }
                    }
                }
            }
            .instrument(span!(tracing::Level::INFO, "StreamRegistryLoop")),
        );
        registry
    }

    async fn register_stream(&self, id: StreamId, stream: Streaming<C>) -> () {
        self.timeout_lock()
            .await
            .stream_cmd_sender
            .send(StreamCommand::Add(id, stream))
            .await
            .expect("Receiver not to be dropped");
    }

    fn unregister_stream(&self, id: StreamId)
    where
        S: Send + 'static,
    {
        let c = self.clone();
        tokio::spawn(async move {
            let mut l = c.timeout_lock().await;
            l.out_streams.remove(&id);
            l.stream_cmd_sender
                .send(StreamCommand::Remove(id))
                .await
                .expect("Receiver not to be dropped");
        });
    }

    pub(super) async fn new_connection(&self, incoming: Streaming<C>) -> DroppableStream<S, C> {
        let (sender, receiver) = tokio::sync::mpsc::channel(5);
        let stream_id = uuid::Uuid::new_v4();
        self.register_stream(stream_id, incoming).await;
        let mut registry = self.timeout_lock().await;
        registry.out_streams.insert(stream_id, sender);
        DroppableStream {
            registry: self.clone(),
            messages: receiver,
            ended: Box::pin(registry.shutdown.clone().cancelled_owned()),
            id: stream_id,
        }
    }

    /// Fan a message out to every attached client.
    ///
    /// Never blocks on a slow client: this is local IPC, so a client that cannot keep up with its
    /// buffer is broken rather than busy, and awaiting it would stall every other window and
    /// back-pressure the brain. Such a connection is dropped instead — the client reconnects and
    /// re-bootstraps, which is the same recovery as any other stream loss.
    pub(super) async fn send(&self, message: S) {
        let mut overflowed = Vec::new();
        {
            let registry = self.timeout_lock().await;
            for ((id, connection), message) in registry.out_streams.iter().zip_clone(message) {
                match connection.try_send(message) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        warn!("Stream {id} is not draining its messages, dropping it");
                        overflowed.push(*id);
                    }
                    Err(TrySendError::Closed(_)) => {}
                }
            }
        }
        for id in overflowed {
            self.unregister_stream(id);
        }
    }
}

impl<S: Clone, C> Clone for StreamRegistry<S, C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: Clone, C> Deref for StreamRegistry<S, C> {
    type Target = Arc<Mutex<StreamRegistryInner<S, C>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Ends with the server's generation. A stream still open would keep its connection, and with it
/// the graceful shutdown of the server, waiting forever.
pub struct DroppableStream<S: Clone + Send + 'static, C: Send + 'static> {
    id: StreamId,
    registry: StreamRegistry<S, C>,
    messages: tokio::sync::mpsc::Receiver<S>,
    ended: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<S: Clone + Send + 'static, C: Send + 'static> tokio_stream::Stream for DroppableStream<S, C> {
    type Item = S;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        self.messages.poll_recv(cx)
    }
}

impl<S: Clone + Send + 'static, C: Send + 'static> Drop for DroppableStream<S, C> {
    fn drop(&mut self) {
        trace!("Dropping stream {}", self.id);
        self.registry.unregister_stream(self.id);
    }
}
