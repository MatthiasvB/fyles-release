use async_trait::async_trait;
use futures::Stream;
use fyles_core::library::util::util::TimeoutLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio_util::time::DelayQueue;
use tracing::{debug, error, warn};

pub struct RequestForResend<R, ContactId> {
    pub request: R,
    pub contact_id: Option<ContactId>,
    pub idempotent: bool,
}

pub struct RequestForResendAfterSessionError<R, ContactId> {
    pub request: R,
    pub contact_id: Option<ContactId>,
    pub requires_new_session: bool,
}

pub struct RemovedRequest<ExternalRequestId, R> {
    pub external_request_id: ExternalRequestId,
    pub request: Option<R>,
}

#[async_trait]
pub trait RequestRegistry<Request: Clone> {
    type RequestId;
    type Session: Eq;
    type ContactId: Clone;
    type ExternalRequestId;
    type NodeId;
    async fn register_request_id(
        &self,
        request_id: Self::RequestId,
        node_id: Self::NodeId,
    ) -> Self::ExternalRequestId;
    async fn register_request(
        &self,
        session: Self::Session,
        id: Self::RequestId,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: Request,
        idempotent: bool,
    ) -> Self::ExternalRequestId;
    async fn remove_request(
        &self,
        id: Self::RequestId,
    ) -> Option<RemovedRequest<Self::ExternalRequestId, Request>>;
    async fn get_request_for_resend(
        &self,
        id: Self::RequestId,
    ) -> Option<RequestForResend<Request, Self::ContactId>>;
    /// If a session error occurs, we will have to establish a new session before sending the request again.
    ///
    /// If there are multiple requests in flight, we may get reports of multiple session errors, all pertaining to
    /// the same compromised session. We do not want to re-establish a session for each one of them. In such cases,
    /// pass in the current session and this function will compare it to the session this request was sent with.
    /// Only if the sessions are equal will this function report that the session should be re-established.
    async fn get_request_for_resend_after_session_error(
        &self,
        current_session: Option<Self::Session>,
        id: Self::RequestId,
    ) -> Option<RequestForResendAfterSessionError<Request, Self::ContactId>>;

    async fn check_request_metadata(
        &self,
        request_id: &Self::RequestId,
    ) -> Option<(usize, Self::ExternalRequestId)>;

    async fn update_request_id(
        &self,
        old_request_id: &Self::RequestId,
        new_request_id: Self::RequestId,
    ) -> Result<Self::ExternalRequestId, ()>;

    async fn get_external_request_id(
        &self,
        request_id: &Self::RequestId,
    ) -> Option<Self::ExternalRequestId>;
}

enum Command<K, NodeId> {
    Register { request_id: K, node_id: NodeId },
    UnRegister { request_id: K },
    ResetTimeout { request_id: K },
}

pub struct TimeoutRequestRegistryWorker<ExternalRequestId: Hash, NodeId> {
    register: HashMap<ExternalRequestId, (tokio_util::time::delay_queue::Key, NodeId)>,
    timeouts: DelayQueue<ExternalRequestId>,
    timeout: Duration,
    receiver: Option<mpsc::UnboundedReceiver<Command<ExternalRequestId, NodeId>>>,
}

#[derive(Clone)]
pub struct TimeoutRequestRegistryHandle<ExternalRequestId, NodeId> {
    sender: mpsc::UnboundedSender<Command<ExternalRequestId, NodeId>>,
}

impl<ExternalRequestId, NodeId> TimeoutRequestRegistryHandle<ExternalRequestId, NodeId> {
    fn register(&self, request_id: ExternalRequestId, node_id: NodeId) {
        if let Err(_) = self.sender.send(Command::Register {
            request_id,
            node_id,
        }) {
            error!("failed to register request");
        }
    }

    fn unregister(&self, request_id: ExternalRequestId) {
        if let Err(_) = self.sender.send(Command::UnRegister { request_id }) {
            error!("failed to unregister request");
        }
    }

    fn reset_timeout(&self, request_id: ExternalRequestId) {
        if let Err(_) = self.sender.send(Command::ResetTimeout { request_id }) {
            error!("failed to reset timeout");
        }
    }
}

struct RegisteredRequest<ExternalRequestId, ContactId, Request, Session> {
    external_id: ExternalRequestId,
    retry_count: usize,
    data: Option<RegisteredRequestData<ContactId, Request, Session>>,
}

struct RegisteredRequestData<ContactId, Request, Session> {
    contact_id: Option<ContactId>,
    request: Request,
    idempotent: bool,
    session: Session,
}

impl<ExternalRequestId, ContactId, Request, Session>
    RegisteredRequest<ExternalRequestId, ContactId, Request, Session>
{
    pub fn only_id(external_id: ExternalRequestId) -> Self {
        Self {
            external_id,
            retry_count: 0,
            data: None,
        }
    }

    pub fn with_data(
        external_id: ExternalRequestId,
        contact_id: Option<ContactId>,
        request: Request,
        idempotent: bool,
        session: Session,
    ) -> Self {
        Self {
            external_id,
            retry_count: 0,
            data: Some(RegisteredRequestData {
                contact_id,
                request,
                idempotent,
                session,
            }),
        }
    }
}

pub struct TimeoutRequestRegistry<
    NodeId,
    RequestId: Hash,
    ExternalRequestId: Hash,
    Request,
    ContactId,
    Session,
> {
    data: Mutex<
        TimeoutRequestRegistryData<RequestId, ExternalRequestId, Request, ContactId, Session>,
    >,
    worker_handle: TimeoutRequestRegistryHandle<ExternalRequestId, NodeId>,
}

pub struct TimeoutRequestRegistryData<
    RequestId: Hash,
    ExternalRequestId: Hash,
    Request,
    ContactId,
    Session,
> {
    register: HashMap<RequestId, RegisteredRequest<ExternalRequestId, ContactId, Request, Session>>,
    next_external_request_id: usize,
}

impl<
    NodeId,
    RequestId: Hash,
    ExternalRequestId: Hash + Clone + Eq + Send + 'static,
    Request,
    ContactId,
    Session,
> TimeoutRequestRegistry<NodeId, RequestId, ExternalRequestId, Request, ContactId, Session>
{
    /// Returns both a [`TimeoutRequestRegistry`] and the associated [`TimeoutRequestRegistryWorker`]. Users
    /// must take care to poll the [`Stream`] implementing worker in order to receive [`ExternalRequestId`]s of
    /// requests that timed out. Applications need to handle these timeouts.
    ///
    /// If the worker is not polled (fast enough), unbounded senders communicating with the worker
    /// will accumulate in-flight request registrations and eventually result in OOM
    pub fn new(
        timeout: Duration,
    ) -> (
        Self,
        TimeoutRequestRegistryWorker<ExternalRequestId, NodeId>,
    ) {
        let (worker, handle) = TimeoutRequestRegistryWorker::<_, _>::new(timeout);
        (
            Self {
                data: TimeoutRequestRegistryData {
                    register: Default::default(),
                    next_external_request_id: 0,
                }
                .into(),
                worker_handle: handle,
            },
            worker,
        )
    }
}

impl<K: Hash + Eq + Clone + Send + 'static, NodeId> TimeoutRequestRegistryWorker<K, NodeId> {
    fn new(timeout: Duration) -> (Self, TimeoutRequestRegistryHandle<K, NodeId>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = TimeoutRequestRegistryWorker {
            register: HashMap::new(),
            timeouts: DelayQueue::new(),
            timeout,
            receiver: Some(receiver),
        };
        (task, TimeoutRequestRegistryHandle { sender })
    }
}

impl<K: Hash + Eq + Clone + Debug + Send + Unpin + 'static, NodeId: Unpin> Stream
    for TimeoutRequestRegistryWorker<K, NodeId>
{
    type Item = (K, NodeId);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(receiver) = self.receiver.as_mut() {
            match receiver.poll_recv(cx) {
                Poll::Ready(ready) => {
                    match ready {
                        None => {
                            warn!(
                                "All senders for command registry dropped, not registering further requests"
                            );
                            self.receiver = None;
                        }
                        Some(command) => {
                            match command {
                                Command::Register {
                                    request_id,
                                    node_id,
                                } => {
                                    let timeout = self.timeout;
                                    let timeout_key =
                                        self.timeouts.insert(request_id.clone(), timeout);
                                    self.register.insert(request_id, (timeout_key, node_id));
                                }
                                Command::UnRegister { request_id } => {
                                    if let Some((timeout_key, _)) =
                                        self.register.remove(&request_id)
                                    {
                                        self.timeouts.remove(&timeout_key);
                                    }
                                }
                                Command::ResetTimeout { request_id } => {
                                    if let Some(timeout_key) =
                                        self.register.get(&request_id).map(|(key, _)| key).copied()
                                    {
                                        let timeout = self.timeout;
                                        self.timeouts.reset(&timeout_key, timeout);
                                    }
                                }
                            };
                        }
                    };
                }
                Poll::Pending => {}
            }
        }
        match self.timeouts.poll_expired(cx) {
            Poll::Ready(maybe_expired) => match maybe_expired {
                None => {
                    if self.receiver.is_none() {
                        debug!(
                            "Command registry senders dropped and all timeouts passed, ceasing operation"
                        );
                        return Poll::Ready(None);
                    }
                }
                Some(expired) => {
                    let request_id = expired.into_inner();
                    let Some((_, node_id)) = self.register.remove(&request_id) else {
                        error!(?request_id, "Request expired for which node ID is unknown");
                        return Poll::Pending;
                    };
                    return Poll::Ready(Some((request_id, node_id)));
                }
            },
            Poll::Pending => {}
        }
        Poll::Pending
    }
}

#[async_trait]
impl<Request, RequestId, Session, ContactId, NodeId> RequestRegistry<Request>
    for TimeoutRequestRegistry<NodeId, RequestId, usize, Request, ContactId, Session>
where
    Request: Clone + Send + Sync,
    Session: Eq + Send + Sync,
    ContactId: Clone + Send + Sync,
    RequestId: Hash + Eq + Send + Sync,
    NodeId: Send,
{
    type RequestId = RequestId;

    type Session = Session;

    type ContactId = ContactId;
    type NodeId = NodeId;

    type ExternalRequestId = usize;

    async fn register_request_id(&self, request_id: RequestId, node_id: NodeId) -> usize {
        let mut data = self.data.timeout_lock().await;
        let id = data.next_external_request_id;
        data.next_external_request_id += 1;
        data.register
            .insert(request_id, RegisteredRequest::only_id(id));
        self.worker_handle.register(id, node_id);
        id
    }

    async fn register_request(
        &self,
        session: Session,
        request_id: RequestId,
        node_id: NodeId,
        contact_id: Option<ContactId>,
        request: Request,
        idempotent: bool,
    ) -> usize {
        let mut data = self.data.timeout_lock().await;
        let id = data.next_external_request_id;
        data.next_external_request_id += 1;
        data.register.insert(
            request_id,
            RegisteredRequest::with_data(id, contact_id, request, idempotent, session),
        );
        self.worker_handle.register(id, node_id);
        id
    }

    async fn remove_request(&self, id: RequestId) -> Option<RemovedRequest<usize, Request>> {
        let mut data = self.data.timeout_lock().await;
        let request_data = data.register.remove(&id);
        if let Some(ref request_data) = request_data {
            self.worker_handle.unregister(request_data.external_id);
        }
        request_data.map(|registered_request| RemovedRequest {
            external_request_id: registered_request.external_id,
            request: registered_request.data.map(|it| it.request),
        })
    }

    async fn get_request_for_resend(
        &self,
        id: RequestId,
    ) -> Option<RequestForResend<Request, ContactId>> {
        let mut data = self.data.timeout_lock().await;
        let request_data = data.register.entry(id);
        let Entry::Occupied(mut request_data) = request_data else {
            return None;
        };

        let for_resend = request_data.get().data.as_ref().and_then(|it| {
            it.idempotent.then(|| RequestForResend {
                request: it.request.clone(),
                contact_id: it.contact_id.as_ref().cloned(),
                idempotent: it.idempotent,
            })
        });

        if for_resend.is_some() {
            request_data.get_mut().retry_count += 1;
            warn!(request_retry_count=request_data.get().retry_count, "Requested to resend request, retry count increased");
            self.worker_handle
                .reset_timeout(request_data.get().external_id);
        }

        for_resend
    }

    async fn get_request_for_resend_after_session_error(
        &self,
        current_session: Option<Session>,
        id: RequestId,
    ) -> Option<RequestForResendAfterSessionError<Request, ContactId>> {
        let mut data = self.data.timeout_lock().await;
        let request_data = data.register.entry(id);
        let Entry::Occupied(mut request_data) = request_data else {
            return None;
        };

        let for_resend =
            request_data
                .get()
                .data
                .as_ref()
                .map(|it| RequestForResendAfterSessionError {
                    request: it.request.clone(),
                    contact_id: it.contact_id.as_ref().cloned(),
                    requires_new_session: current_session.map(|c| c == it.session).unwrap_or(false),
                });

        if for_resend.is_some() {
            request_data.get_mut().retry_count += 1;
            warn!(request_retry_count=request_data.get().retry_count, "Requested to resend request after session error, retry count increased");
            self.worker_handle
                .reset_timeout(request_data.get().external_id);
        }

        for_resend
    }

    async fn check_request_metadata(&self, request_id: &RequestId) -> Option<(usize, usize)> {
        let data = self.data.timeout_lock().await;
        data.register
            .get(request_id)
            .map(|it| (it.retry_count, it.external_id))
    }

    async fn update_request_id(
        &self,
        old_request_id: &RequestId,
        new_request_id: RequestId,
    ) -> Result<usize, ()> {
        let mut data = self.data.timeout_lock().await;
        
        {
            if let Some(request) = data.register.remove(old_request_id) {
                let external_id = request.external_id;
                data.register.insert(new_request_id, request);
                Ok(external_id)
            } else {
                Err(())
            }
        }
    }

    async fn get_external_request_id(&self, request_id: &RequestId) -> Option<usize> {
        let data = self.data.timeout_lock().await;
        data.register.get(request_id).map(|it| it.external_id)
    }
}
