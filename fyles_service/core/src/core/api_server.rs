use crate::core::listen::ListenPort;
use crate::core::settings::SettingsMutation;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming, async_trait};
use tracing::{Instrument, debug, error, info, span, warn};

use crate::Endpoint;
use crate::core::api_server::stream_registry::{DroppableStream, StreamId, StreamRegistry};
use crate::core::api_server::tcp_token_interceptor::bearer_auth_interceptor;
use crate::core::backup::{self, NewBackupPassword, Password, RestoreRequest};
use crate::core::brain::types::ContactRegistration;
use crate::core::domain_models::{ContactIdParseError, GivenName, RemoteFilerequest};
use crate::core::domain_proto_conversion::SelfContactDeserializationError;
use crate::library::wire::api::client_message::Message;
use crate::library::wire::api::file_request_service_server::FileRequestServiceServer;
use crate::library::wire::api::node_status::State;
use crate::library::wire::api::node_status_service_server::{
    NodeStatusService, NodeStatusServiceServer,
};
use crate::library::wire::api::resolve_filerequest_location_response::Location;
use crate::library::wire::api::{
    BackupDataRequest, BackupDataResponse, ClientMessage, ContactShareInvalidProto,
    DecodeSignedContactRequest, DecodeSignedContactResponse, DecodedContactProto,
    DeletePendingFileRequest, DeletePendingFileResponse, DeleteReceivedFileRequest,
    DeleteReceivedFileResponse, DeleteRemoteFileRequestRequest, DeleteRemoteFileRequestResponse,
    EvaluateBackupPasswordRequest, EvaluateBackupPasswordResponse, GetAllPendingFilesRequest,
    GetAllPendingFilesResponse, GetAllRemoteFileRequestsRequest, GetAllRemoteFileRequestsResponse,
    GetBackupPasswordStatusRequest, GetBackupPasswordStatusResponse,
    GetContactShareChallengeRequest, GetContactShareChallengeResponse, GetFullSelfContactRequest,
    GetFullSelfContactResponse, GetListenStatusRequest, GetListenStatusResponse,
    GetNodePeerIdRequest, GetNodePeerIdResponse, GetNodeStatusRequest, GetNodeStatusResponse,
    GetPendingFileRequest, GetPendingFileResponse, GetRemoteFileRequestRequest,
    GetRemoteFileRequestResponse, GetSelfContactDisplayRequest, GetSelfContactDisplayResponse,
    GetSelfContactInviteChallengeRequest, GetSelfContactInviteChallengeResponse,
    GetSettingsRequest, HealthCheckRequest, HealthCheckResponse, ListPendingFilesRequest,
    ListPendingFilesResponse, ListReceivedFilesRequest, ListReceivedFilesResponse,
    ListRemoteFileRequestsRequest, ListRemoteFileRequestsResponse, NodeInfo, NodeStats, NodeStatus,
    RegisterContactRequest, RegisterContactResponse, ResolveFilerequestLocationRequest,
    ResolveFilerequestLocationResponse, RestoreDataRequest, RestoreDataResponse, ServerMessage,
    SetBackupPasswordRequest, SetBackupPasswordResponse, SetListenPortRequest,
    SetListenPortResponse, SettingsRequestEnvelope, SettingsResponseEnvelope,
    SharePublicSelfContactRequest, SharePublicSelfContactResponse,
    SnoozeBackupPasswordVerificationRequest, SnoozeBackupPasswordVerificationResponse,
    StorePendingFileRequest, StorePendingFileResponse, StoreRemoteFileRequestRequest,
    StoreRemoteFileRequestResponse, UnregisterContactShareChallengeRequest,
    UnregisterContactShareChallengeResponse, UnregisterSelfContactInviteChallengeRequest,
    UnregisterSelfContactInviteChallengeResponse, UpdateIdentityRequest, UpdateIdentityResponse,
    UpdateRemoteFileRequestRequest, UpdateRemoteFileRequestResponse, UpdateSelfContactNameRequest,
    UpdateSelfContactNameResponse, UseContactShareChallengeRequest,
    UseContactShareChallengeResponse, UseSelfContactInviteRequest, UseSelfContactInviteResponse,
    VerifyBackupPasswordRequest, VerifyBackupPasswordResponse, WaitForReadyRequest,
    WaitForReadyResponse, decode_signed_contact_response,
};
use crate::library::wire::{
    RunnableFilerequestServer,
    api::{
        CreateFileRequestRequest, CreateFileRequestResponse, DeleteContactRequest,
        DeleteContactResponse, DeleteFileRequestRequest, DeleteFileRequestResponse,
        FileRequestProto, FileRequestRequest, FileRequestResponse, GetAllContactsRequest,
        GetAllContactsResponse, GetAllFileRequestsRequest, GetAllFileRequestsResponse,
        GetContactNameRequest, GetContactNameResponse, GetContactNamesRequest,
        GetContactNamesResponse, GetContactRequest, GetContactResponse,
        GetEntitlementStatusRequest, GetEntitlementStatusResponse, GetLicenseIdentityRequest,
        GetLicenseIdentityResponse, GetVersionRequest, GetVersionResponse, LicenseCheckAck,
        ListOwnedDevicesRequest, ListOwnedDevicesResponse, OwnedDeviceProto,
        PrepareLicenseCheckRequest, PrepareLicenseCheckResponse, RemoveOwnedDeviceRequest,
        RemoveOwnedDeviceResponse, RenameThisDeviceRequest, RenameThisDeviceResponse,
        ShutdownRequest, ShutdownResponse, SubmitLicenseCheckResponseRequest, UpdateContactRequest,
        UpdateContactResponse, UpdateFileRequestRequest, UpdateFileRequestResponse,
        file_request_service_server::FileRequestService,
    },
};

use super::brain::action_client::ClientAction;
use super::brain::error::FilerequestError;
use super::brain::types::{BrainRequest, FilerequestResult};
use super::brain::{self, action::BrainAction};

use super::domain_models::{CreateFilerequest, Filerequest, FilerequestLocation, PeerIdWrapper};
use super::domain_proto_conversion::{decode_share, parse_contact_id};

pub mod stream_registry;
mod tcp_token_interceptor;

#[derive(Clone)]
pub struct ApiServer {
    action_sender: mpsc::Sender<BrainAction>,
    stream_registry: Arc<StreamRegistry<Result<ServerMessage, Status>, ClientMessage>>,
    endpoint: Endpoint,
    internal_data_dir: PathBuf,
    shutdown: CancellationToken,
}

impl ApiServer {
    async fn ask_brain<T, U>(
        &self,
        request: T,
        action: fn(BrainRequest<T, FilerequestResult<U>>) -> ClientAction,
    ) -> Result<U, Status> {
        let (request, response) = BrainRequest::with_receiver(request);
        self.action_sender
            .send(BrainAction::Client(action(request)))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;
        response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?
            .map_err(|e| match e {
                FilerequestError::InputError(message) => Status::failed_precondition(message),
                other => {
                    error!("{other:?}");
                    Status::internal(other.to_string())
                }
            })
    }

    pub fn new(
        action_sender: mpsc::Sender<BrainAction>,
        endpoint: Endpoint,
        internal_data_dir: PathBuf,
        mut receiver: Receiver<ServerMessage>,
        shutdown: CancellationToken,
    ) -> Self {
        let sender_clone = action_sender.clone();
        let stream_registry = Arc::new(StreamRegistry::new(
            move |message: Option<(StreamId, Result<ClientMessage, Status>)>| {
                let action_sender = action_sender.clone();
                Box::pin(async move {
                    if let Some((stream_id, msg_result)) = message {
                        // Handle incoming messages here
                        match msg_result {
                            Ok(msg) => {
                                let Message::Heartbeat(heartbeat) =
                                    msg.message.expect("Message is non-optional");
                                // Not logged: arrives every few seconds from every client.
                                if action_sender
                                    .send(BrainAction::Client(ClientAction::ClientHeartbeat {
                                        foreground: heartbeat.foreground,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    error!("Could not hand a client heartbeat to the brain");
                                }
                            }
                            Err(e) => {
                                warn!("Error on stream (this may just be a closed connection, which can be ignored) {}: {:?}", stream_id, e);
                            }
                        }
                    }
                }.instrument(span!(tracing::Level::INFO, "StreamMessageHandler")))
            },
            shutdown.clone(),
        ));
        let registry_clone = stream_registry.clone();
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                registry_clone.send(Ok(message)).await;
            }
        });
        Self {
            action_sender: sender_clone,
            stream_registry,
            internal_data_dir,
            endpoint,
            shutdown,
        }
    }
}

#[async_trait]
impl RunnableFilerequestServer for ApiServer {
    async fn run(self: Box<Self>) {
        let shutdown = self.shutdown.clone();
        match &self.endpoint {
            Endpoint::Tcp { host, port } => {
                // let addr = format!("{}:{}", host, port)
                //     .parse()
                //     .expect("Cannot parse address");
                let addr =
                    SocketAddr::new(host.parse().expect("Cannot parse host"), port.unwrap_or(0));

                let listener = TcpListener::bind(addr)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to bind TCP {}: {}", addr, e));
                let chosen_port = listener
                    .local_addr()
                    .expect("Failed to get local address")
                    .port();
                let incoming = TcpListenerStream::new(listener);

                let config_file_path = self.internal_data_dir.join("api_server_config");
                debug!("Writing API server config to {:?}", config_file_path);
                let mut tcp_config_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(config_file_path)
                    .expect("Cannot open token file");

                tcp_config_file
                    .write_all(format!("{chosen_port}:").as_bytes())
                    .expect("Writing port to file");

                let (auth, token) = bearer_auth_interceptor();
                tcp_config_file
                    .write_all(token.as_bytes())
                    .expect("Writing token to file");
                info!("Server listening on tcp://{}:{}", host, chosen_port);
                Server::builder()
                    .add_service(FileRequestServiceServer::with_interceptor(
                        *self.clone(),
                        auth.clone(),
                    ))
                    .add_service(NodeStatusServiceServer::with_interceptor(*self, auth))
                    .serve_with_incoming_shutdown(incoming, shutdown.cancelled_owned())
                    .await
                    .expect("API server to serve");
            }
            Endpoint::Uds { path } => {
                #[cfg(not(unix))]
                {
                    // IPC on Windows is a fucking mess. Until tokio as upstream support for
                    // UDS on Windows, we cannot support this.
                    panic!("Unix Domain Sockets are not supported on this platform");
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    use tokio::net::UnixListener;
                    use tokio_stream::wrappers::UnixListenerStream;

                    let listener = {
                        let _ = std::fs::remove_file(path);
                        UnixListener::bind(path)
                            .unwrap_or_else(|e| panic!("Failed to bind UDS {}: {}", path, e))
                    };

                    // Tighten permissions (rw for owner, optional group)
                    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660));

                    info!("Server listening on unix://{}", path);

                    let incoming = UnixListenerStream::new(listener);

                    Server::builder()
                        .add_service(FileRequestServiceServer::new(*self.clone()))
                        .add_service(NodeStatusServiceServer::new(*self))
                        .serve_with_incoming_shutdown(incoming, shutdown.cancelled_owned())
                        .await
                        .expect("Api server to serve");
                }
            }
        };
    }
}

#[tonic::async_trait]
impl FileRequestService for ApiServer {
    async fn get_file_request(
        &self,
        request: Request<FileRequestRequest>,
    ) -> Result<Response<FileRequestResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::ReadFilerequest(
                brain::types::BrainRequest {
                    request: request.into_inner().id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(filerequest) => Ok(Response::new(FileRequestResponse {
                file_request: Some(filerequest.into()),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn create_file_request(
        &self,
        request: Request<CreateFileRequestRequest>,
    ) -> Result<Response<CreateFileRequestResponse>, Status> {
        let inner = request.into_inner();
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::CreateFilerequest(
                brain::types::BrainRequest {
                    request: CreateFilerequest {
                        title: inner.title.clone(),
                        description: inner.description.clone(),
                        is_active: true, // Default to active
                        access: inner.access.clone().try_into()?,
                    },
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(id) => Ok(Response::new(CreateFileRequestResponse {
                file_request: Some(FileRequestProto {
                    id: id.clone().0,
                    title: inner.title,
                    description: inner.description,
                    is_active: true,
                    access: inner.access,
                }),
            })),
            Err(e @ FilerequestError::FolderClaimed { .. }) => {
                Err(Status::already_exists(e.to_string()))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    async fn get_entitlement_status(
        &self,
        _request: Request<GetEntitlementStatusRequest>,
    ) -> Result<Response<GetEntitlementStatusResponse>, Status> {
        let (request, response) = BrainRequest::with_receiver(());
        self.action_sender
            .send(BrainAction::Client(ClientAction::GetEntitlements(request)))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(entitlements) => Ok(Response::new(entitlements.into())),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_license_identity(
        &self,
        _request: Request<GetLicenseIdentityRequest>,
    ) -> Result<Response<GetLicenseIdentityResponse>, Status> {
        let (request, response) = BrainRequest::with_receiver(());
        self.action_sender
            .send(BrainAction::Client(ClientAction::GetLicenseIdentity(
                request,
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(identity) => Ok(Response::new(GetLicenseIdentityResponse { identity })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn prepare_license_check(
        &self,
        request: Request<PrepareLicenseCheckRequest>,
    ) -> Result<Response<PrepareLicenseCheckResponse>, Status> {
        let (request, response) = BrainRequest::with_receiver(request.into_inner().force);
        self.action_sender
            .send(BrainAction::Client(ClientAction::PrepareLicenseCheck(
                request,
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(prepared) => Ok(Response::new(prepared.into())),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn submit_license_check_response(
        &self,
        request: Request<SubmitLicenseCheckResponseRequest>,
    ) -> Result<Response<LicenseCheckAck>, Status> {
        let inner = request.into_inner();
        let http_status = u16::try_from(inner.http_status)
            .map_err(|_| Status::invalid_argument("http_status out of range"))?;

        let (request, response) = BrainRequest::with_receiver((http_status, inner.body));
        self.action_sender
            .send(BrainAction::Client(
                ClientAction::SubmitLicenseCheckResponse(request),
            ))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        // The resulting entitlement state is delivered by the EntitlementsChanged push, so
        // this response is a bare acknowledgement.
        match result {
            Ok(_) => Ok(Response::new(LicenseCheckAck {})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::Shutdown(
                brain::types::BrainRequest {
                    request: (),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send shutdown action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(_) => Ok(Response::new(ShutdownResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn update_file_request(
        &self,
        request: Request<UpdateFileRequestRequest>,
    ) -> Result<Response<UpdateFileRequestResponse>, Status> {
        let inner = request.into_inner();
        let file_request = inner
            .file_request
            .ok_or_else(|| Status::invalid_argument("Missing file_request"))?;

        let filerequest = Filerequest::try_from(file_request.clone())?;
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::UpdateFilerequest(
                brain::types::BrainRequest {
                    request: filerequest,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(_) => Ok(Response::new(UpdateFileRequestResponse {
                file_request: Some(file_request),
            })),
            Err(e @ FilerequestError::FolderClaimed { .. }) => {
                Err(Status::already_exists(e.to_string()))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete_file_request(
        &self,
        request: Request<DeleteFileRequestRequest>,
    ) -> Result<Response<DeleteFileRequestResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::DeleteFilerequest(
                brain::types::BrainRequest {
                    request: request.into_inner().id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(_) => Ok(Response::new(DeleteFileRequestResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_all_file_requests(
        &self,
        _request: Request<GetAllFileRequestsRequest>,
    ) -> Result<Response<GetAllFileRequestsResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::ListFilerequests(
                brain::types::BrainRequest {
                    request: (),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(filerequests) => Ok(Response::new(GetAllFileRequestsResponse {
                file_requests: filerequests.iter().map(Into::into).collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_contact_name(
        &self,
        request: Request<GetContactNameRequest>,
    ) -> Result<Response<GetContactNameResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetContactName(
                brain::types::BrainRequest {
                    request: parse_contact_id(request.into_inner().id)?,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(name) => Ok(Response::new(GetContactNameResponse {
                name: name.to_string(), // or name.as_ref().to_owned()
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_contact_names(
        &self,
        request: Request<GetContactNamesRequest>,
    ) -> Result<Response<GetContactNamesResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetContactNames(
                brain::types::BrainRequest {
                    request: request
                        .into_inner()
                        .ids
                        .into_iter()
                        .map(parse_contact_id)
                        .collect::<Result<_, _>>()?,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(names) => Ok(Response::new(GetContactNamesResponse {
                names: names
                    .iter()
                    .map(|(id, name)| (id.to_string(), name.clone()))
                    .collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_contact(
        &self,
        request: Request<GetContactRequest>,
    ) -> Result<Response<GetContactResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetContact(
                brain::types::BrainRequest {
                    request: parse_contact_id(request.into_inner().id)?,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(contact) => Ok(Response::new(GetContactResponse {
                contact: Some(contact.into()),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_all_contacts(
        &self,
        _request: Request<GetAllContactsRequest>,
    ) -> Result<Response<GetAllContactsResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::ListContacts(
                brain::types::BrainRequest {
                    request: (),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(contacts) => Ok(Response::new(GetAllContactsResponse {
                contacts: contacts.into_iter().map(Into::into).collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn update_contact(
        &self,
        request: Request<UpdateContactRequest>,
    ) -> Result<Response<UpdateContactResponse>, Status> {
        let inner = request.into_inner();
        let contact = inner
            .contact
            .ok_or_else(|| Status::invalid_argument("Missing contact"))?;
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::UpdateContact(
                brain::types::BrainRequest {
                    request: contact.try_into().map_err(|e: ContactIdParseError| {
                        Status::invalid_argument(e.to_string())
                    })?,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(_) => Ok(Response::new(UpdateContactResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete_contact(
        &self,
        request: Request<DeleteContactRequest>,
    ) -> Result<Response<DeleteContactResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::DeleteContact(
                brain::types::BrainRequest {
                    request: parse_contact_id(request.into_inner().id)?,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(_) => Ok(Response::new(DeleteContactResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_owned_devices(
        &self,
        _request: Request<ListOwnedDevicesRequest>,
    ) -> Result<Response<ListOwnedDevicesResponse>, Status> {
        let (request, response) = BrainRequest::with_receiver(());
        self.action_sender
            .send(BrainAction::Client(ClientAction::ListOwnedDevices(request)))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;
        let result = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;
        match result {
            Ok(devices) => Ok(Response::new(ListOwnedDevicesResponse {
                devices: devices
                    .into_iter()
                    .map(|(peer_id, name)| OwnedDeviceProto {
                        peer_id: peer_id.0,
                        name,
                    })
                    .collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn rename_this_device(
        &self,
        request: Request<RenameThisDeviceRequest>,
    ) -> Result<Response<RenameThisDeviceResponse>, Status> {
        let name = GivenName::try_from(request.into_inner().name)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let (request, response) = BrainRequest::with_receiver(name);
        self.action_sender
            .send(BrainAction::Client(ClientAction::RenameThisDevice(request)))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;
        let result = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;
        match result {
            Ok(_) => Ok(Response::new(RenameThisDeviceResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn remove_owned_device(
        &self,
        request: Request<RemoveOwnedDeviceRequest>,
    ) -> Result<Response<RemoveOwnedDeviceResponse>, Status> {
        let (request, response) =
            BrainRequest::with_receiver(PeerIdWrapper(request.into_inner().peer_id));
        self.action_sender
            .send(BrainAction::Client(ClientAction::RemoveOwnedDevice(
                request,
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;
        let result = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;
        match result {
            Ok(_) => Ok(Response::new(RemoveOwnedDeviceResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn update_self_contact_name(
        &self,
        request: Request<UpdateSelfContactNameRequest>,
    ) -> Result<Response<UpdateSelfContactNameResponse>, Status> {
        let name = GivenName::try_from(request.into_inner().name)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let (request, response_sender) = BrainRequest::with_receiver(name);

        self.action_sender
            .send(BrainAction::Client(ClientAction::UpdateSelfContactName(
                request,
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_sender
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(_) => Ok(Response::new(UpdateSelfContactNameResponse {
                success: true,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn wait_for_ready(
        &self,
        _: Request<WaitForReadyRequest>,
    ) -> Result<Response<WaitForReadyResponse>, Status> {
        let (request, response_sender) = BrainRequest::with_receiver(());

        self.action_sender
            .send(BrainAction::Client(ClientAction::WaitForReady(request)))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let successfully_started = response_sender
            .await
            .map_err(|_| Status::resource_exhausted("Too many requests"))?;

        Ok(Response::new(WaitForReadyResponse {
            successfully_started,
        }))
    }

    async fn get_full_self_contact(
        &self,
        _: Request<GetFullSelfContactRequest>,
    ) -> Result<Response<GetFullSelfContactResponse>, Status> {
        let (request, response_sender) = BrainRequest::with_receiver(());

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetFullSelfContact(
                request,
            )))
            .await
            .expect("Sending to go okay");

        let response = response_sender.await.expect("Sender not to be dropped");

        let invite = response.map_err(|_| Status::internal("Unable to get self-contact invite"))?;

        Ok(Response::new(GetFullSelfContactResponse {
            invite: Some(invite.into()),
        }))
    }

    async fn update_identity(
        &self,
        new_self_contact: Request<UpdateIdentityRequest>,
    ) -> Result<Response<UpdateIdentityResponse>, Status> {
        let invite = new_self_contact
            .into_inner()
            .invite
            .ok_or_else(|| Status::invalid_argument("Missing invite in UpdateIdentityRequest"))?
            .try_into()
            .map_err(|e: SelfContactDeserializationError| {
                Status::invalid_argument(e.to_string())
            })?;
        let (request, response_sender) = BrainRequest::with_receiver(invite);

        self.action_sender
            .send(BrainAction::Client(ClientAction::UpdateIdentity(request)))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_sender
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(_) => Ok(Response::new(UpdateIdentityResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_self_contact_display(
        &self,
        _: Request<GetSelfContactDisplayRequest>,
    ) -> Result<Response<GetSelfContactDisplayResponse>, Status> {
        let (request, response_sender) = BrainRequest::with_receiver(());

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetSelfContactDisplay(
                request,
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_sender
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(self_contact) => Ok(Response::new(GetSelfContactDisplayResponse {
                self_contact: Some(self_contact.into()),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn share_public_self_contact(
        &self,
        _: Request<SharePublicSelfContactRequest>,
    ) -> Result<Response<SharePublicSelfContactResponse>, Status> {
        let (request, response_sender) = BrainRequest::with_receiver(());

        self.action_sender
            .send(BrainAction::Client(ClientAction::SharePublicSelfContact(
                request,
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_sender
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(share) => {
                Ok(Response::new(SharePublicSelfContactResponse {
                    sharable_contact: Some(share.try_into().map_err(
                        |e: bincode::error::EncodeError| Status::internal(e.to_string()),
                    )?),
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn register_contact(
        &self,
        request: Request<RegisterContactRequest>,
    ) -> Result<Response<RegisterContactResponse>, Status> {
        let inner = request.into_inner();
        let (request, response_receiver) = BrainRequest::with_receiver(ContactRegistration {
            share: decode_share(inner.sharable_contact)?,
            user_edited_name: inner.user_edited_name,
        });

        self.action_sender
            .send(BrainAction::Client(ClientAction::RegisterContact(request)))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(()) => Ok(Response::new(RegisterContactResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn decode_signed_contact(
        &self,
        request: Request<DecodeSignedContactRequest>,
    ) -> Result<Response<DecodeSignedContactResponse>, Status> {
        let share = decode_share(request.into_inner().sharable_contact)?;
        let (request, response_receiver) = BrainRequest::with_receiver(share);

        self.action_sender
            .send(BrainAction::Client(ClientAction::DecodeSignedContact(
                request,
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let decoded = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        Ok(Response::new(DecodeSignedContactResponse {
            result: Some(match decoded {
                Some(contact) => {
                    decode_signed_contact_response::Result::Decoded(DecodedContactProto {
                        contact_id: contact.id.to_string(),
                        name: contact.name,
                    })
                }
                None => {
                    decode_signed_contact_response::Result::Invalid(ContactShareInvalidProto {})
                }
            }),
        }))
    }

    async fn store_remote_file_request(
        &self,
        request: Request<StoreRemoteFileRequestRequest>,
    ) -> Result<Response<StoreRemoteFileRequestResponse>, Status> {
        let inner = request.into_inner();
        let remote_fr = match inner.remote_file_request {
            Some(proto) => proto
                .try_into()
                .map_err(|e: ContactIdParseError| Status::invalid_argument(e.to_string()))?,
            None => return Err(Status::invalid_argument("Missing remote file request")),
        };

        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::CreateRemoteFilerequest(
                brain::types::BrainRequest {
                    request: remote_fr,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(id) => Ok(Response::new(StoreRemoteFileRequestResponse {
                id: id.clone().0,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_remote_file_request(
        &self,
        request: Request<GetRemoteFileRequestRequest>,
    ) -> Result<Response<GetRemoteFileRequestResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetRemoteFilerequest(
                brain::types::BrainRequest {
                    request: request.into_inner().id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(remote_fr) => Ok(Response::new(GetRemoteFileRequestResponse {
                remote_file_request: Some(remote_fr.into()),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_remote_file_requests(
        &self,
        request: Request<ListRemoteFileRequestsRequest>,
    ) -> Result<Response<ListRemoteFileRequestsResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(
                ClientAction::GetRemoteFilerequestsByContact(brain::types::BrainRequest {
                    request: parse_contact_id(request.into_inner().contact_id)?,
                    response_sender: Mutex::new(Some(response_sender)),
                }),
            ))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(remote_frs) => Ok(Response::new(ListRemoteFileRequestsResponse {
                remote_file_requests: remote_frs.into_iter().map(Into::into).collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_all_remote_file_requests(
        &self,
        _request: Request<GetAllRemoteFileRequestsRequest>,
    ) -> Result<Response<GetAllRemoteFileRequestsResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetAllRemoteFilerequests(
                brain::types::BrainRequest {
                    request: (),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result {
            Ok(remote_frs) => Ok(Response::new(GetAllRemoteFileRequestsResponse {
                remote_file_requests: remote_frs.into_iter().map(Into::into).collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete_remote_file_request(
        &self,
        request: Request<DeleteRemoteFileRequestRequest>,
    ) -> Result<Response<DeleteRemoteFileRequestResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::DeleteRemoteFilerequest(
                brain::types::BrainRequest {
                    request: request.into_inner().id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(_) => Ok(Response::new(DeleteRemoteFileRequestResponse {
                success: true,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn store_pending_file(
        &self,
        request: Request<StorePendingFileRequest>,
    ) -> Result<Response<StorePendingFileResponse>, Status> {
        let inner = request.into_inner();
        // let pending_files = inner.pending_files.iter().map(|f| CreatePendingFiles {
        //     file_paths: f.file_paths.clone(),
        //     target_filerequest_id: f.target_file_request_id.clone(),
        // }).collect::<Vec<_>>();
        // let pending_files = CreatePendingFiles {
        //     file_paths: inner.pending_files.unwrap().file_paths.clone(),
        //     target_filerequest_id: inner.pending_files.iter().map(|f| f.target_file_request_id.clone()).collect(),
        // };

        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::CreatePendingFiles(
                brain::types::BrainRequest {
                    request: inner.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(ids) => Ok(Response::new(StorePendingFileResponse {
                ids: ids.iter().cloned().map(|id| id.0).collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_pending_file(
        &self,
        request: Request<GetPendingFileRequest>,
    ) -> Result<Response<GetPendingFileResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetPendingFile(
                brain::types::BrainRequest {
                    request: request.into_inner().id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(pending_file) => Ok(Response::new(GetPendingFileResponse {
                pending_file: Some(pending_file.into()),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_pending_files(
        &self,
        request: Request<ListPendingFilesRequest>,
    ) -> Result<Response<ListPendingFilesResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetPendingFiles(
                brain::types::BrainRequest {
                    request: request.into_inner().target_file_request_id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(pending_files) => Ok(Response::new(ListPendingFilesResponse {
                pending_files: pending_files.iter().map(Into::into).collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_all_pending_files(
        &self,
        _request: Request<GetAllPendingFilesRequest>,
    ) -> Result<Response<GetAllPendingFilesResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetAllPendingFiles(
                brain::types::BrainRequest {
                    request: (),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(pending_files) => Ok(Response::new(GetAllPendingFilesResponse {
                pending_files: pending_files.iter().map(Into::into).collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete_pending_file(
        &self,
        request: Request<DeletePendingFileRequest>,
    ) -> Result<Response<DeletePendingFileResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::DeletePendingFile(
                brain::types::BrainRequest {
                    request: request.into_inner().id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(_) => Ok(Response::new(DeletePendingFileResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn update_remote_file_request(
        &self,
        request: Request<UpdateRemoteFileRequestRequest>,
    ) -> Result<Response<UpdateRemoteFileRequestResponse>, Status> {
        let inner = request.into_inner();
        let remote_fr = match inner.remote_file_request {
            Some(proto) => {
                let remote_fr: RemoteFilerequest = proto
                    .try_into()
                    .map_err(|e: ContactIdParseError| Status::invalid_argument(e.to_string()))?;
                (remote_fr.id, remote_fr.name)
            }
            None => return Err(Status::invalid_argument("Missing remote file request")),
        };

        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::UpdateRemoteFilerequest(
                brain::types::BrainRequest {
                    request: remote_fr,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .inspect_err(|e| error!("Receiver error: {e}"))
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(()) => Ok(Response::new(UpdateRemoteFileRequestResponse {
                success: true,
            })),
            Err(e) => {
                error!("Filerequest error: {e}");
                Ok(Response::new(UpdateRemoteFileRequestResponse {
                    success: false,
                }))
            }
        }
    }

    async fn get_node_peer_id(
        &self,
        _request: Request<GetNodePeerIdRequest>,
    ) -> Result<Response<GetNodePeerIdResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetNodePeerId(
                brain::types::BrainRequest {
                    request: (),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(peer_id) => Ok(Response::new(GetNodePeerIdResponse {
                peer_id: peer_id.clone(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_received_files(
        &self,
        request: Request<ListReceivedFilesRequest>,
    ) -> Result<Response<ListReceivedFilesResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(
                ClientAction::ListReceivedFilesForRequest(brain::types::BrainRequest {
                    request: request.into_inner().remote_file_request_id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                }),
            ))
            .await
            .map_err(|_| Status::internal("dispatch failed"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("no response"))?;

        match result {
            Ok(received_files) => Ok(Response::new(ListReceivedFilesResponse {
                received_files: received_files.into_iter().map(Into::into).collect(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete_received_file(
        &self,
        request: Request<DeleteReceivedFileRequest>,
    ) -> Result<Response<DeleteReceivedFileResponse>, Status> {
        debug!("API Server: delete_received_file called");
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::DeleteReceivedFile(
                brain::types::BrainRequest {
                    request: request.into_inner().id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("dispatch failed"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("no response"))?;

        match result {
            Ok(_) => Ok(Response::new(DeleteReceivedFileResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn resolve_filerequest_location(
        &self,
        request: Request<ResolveFilerequestLocationRequest>,
    ) -> Result<Response<ResolveFilerequestLocationResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(
                ClientAction::ResolveFilerequestLocation(brain::types::BrainRequest {
                    request: request.into_inner().filerequest_id.into(),
                    response_sender: Mutex::new(Some(response_sender)),
                }),
            ))
            .await
            .map_err(|_| Status::internal("dispatch failed"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("no response"))?;

        match result {
            Ok(resolved) => {
                let location = match resolved {
                    FilerequestLocation::Absolute(path) => Location::AbsolutePath(path),
                    FilerequestLocation::Relative(path) => Location::RelativePath(path),
                };
                Ok(Response::new(ResolveFilerequestLocationResponse {
                    location: Some(location),
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_self_contact_invite_challenge(
        &self,
        _request: Request<GetSelfContactInviteChallengeRequest>,
    ) -> Result<Response<GetSelfContactInviteChallengeResponse>, Status> {
        let (request, response) = BrainRequest::with_receiver(());
        self.action_sender
            .send(BrainAction::Client(
                ClientAction::RegisterSelfContactInviteChallenge(request),
            ))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;
        let challenge = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        Ok(Response::new(GetSelfContactInviteChallengeResponse {
            challenge: challenge.0,
        }))
    }

    async fn unregister_self_contact_invite_challenge(
        &self,
        request: Request<UnregisterSelfContactInviteChallengeRequest>,
    ) -> Result<Response<UnregisterSelfContactInviteChallengeResponse>, Status> {
        let (request, response) =
            BrainRequest::with_receiver(request.into_inner().challenge.into());
        self.action_sender
            .send(BrainAction::Client(
                ClientAction::UnregisterSelfContactInviteChallenge(request),
            ))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;
        let _ = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;
        Ok(Response::new(
            UnregisterSelfContactInviteChallengeResponse {},
        ))
    }

    async fn use_self_contact_invite(
        &self,
        request: Request<UseSelfContactInviteRequest>,
    ) -> Result<Response<UseSelfContactInviteResponse>, Status> {
        let inner = request.into_inner();
        let (request, response) =
            BrainRequest::with_receiver((inner.challenge.into(), inner.peer_id.into()));

        self.action_sender
            .send(BrainAction::Client(
                ClientAction::UseSelfContactInviteChallenge(request),
            ))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        Ok(Response::new(UseSelfContactInviteResponse {}))
    }

    async fn get_contact_share_challenge(
        &self,
        _request: Request<GetContactShareChallengeRequest>,
    ) -> Result<Response<GetContactShareChallengeResponse>, Status> {
        let (request, response) = BrainRequest::with_receiver(());
        self.action_sender
            .send(BrainAction::Client(
                ClientAction::RegisterContactShareChallenge(request),
            ))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;
        let challenge = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        Ok(Response::new(GetContactShareChallengeResponse {
            challenge: challenge.0,
        }))
    }

    async fn unregister_contact_share_challenge(
        &self,
        request: Request<UnregisterContactShareChallengeRequest>,
    ) -> Result<Response<UnregisterContactShareChallengeResponse>, Status> {
        let (request, response) =
            BrainRequest::with_receiver(request.into_inner().challenge.into());
        self.action_sender
            .send(BrainAction::Client(
                ClientAction::UnregisterContactShareChallenge(request),
            ))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;
        let _ = response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;
        Ok(Response::new(UnregisterContactShareChallengeResponse {}))
    }

    async fn use_contact_share_challenge(
        &self,
        request: Request<UseContactShareChallengeRequest>,
    ) -> Result<Response<UseContactShareChallengeResponse>, Status> {
        let inner = request.into_inner();
        let (request, response) =
            BrainRequest::with_receiver((inner.challenge.into(), inner.peer_id.into()));

        self.action_sender
            .send(BrainAction::Client(ClientAction::UseContactShareChallenge(
                request,
            )))
            .await
            .map_err(|_| Status::internal("Failed to send action"))?;

        response
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        Ok(Response::new(UseContactShareChallengeResponse {}))
    }

    async fn backup_data(
        &self,
        _request: Request<BackupDataRequest>,
    ) -> Result<Response<BackupDataResponse>, Status> {
        let outcome = self.ask_brain((), ClientAction::BackupData).await?;
        Ok(Response::new(outcome.into()))
    }

    async fn restore_data(
        &self,
        request: Request<RestoreDataRequest>,
    ) -> Result<Response<RestoreDataResponse>, Status> {
        let request = request.into_inner();
        let restore = RestoreRequest {
            uri: request.backup_file_path.into(),
            password: request.password.map(Password::new),
        };
        let outcome = self.ask_brain(restore, ClientAction::RestoreData).await?;
        Ok(Response::new(outcome.into()))
    }

    async fn get_backup_password_status(
        &self,
        _request: Request<GetBackupPasswordStatusRequest>,
    ) -> Result<Response<GetBackupPasswordStatusResponse>, Status> {
        let status = self
            .ask_brain((), ClientAction::GetBackupPasswordStatus)
            .await?;
        Ok(Response::new(status.into()))
    }

    async fn evaluate_backup_password(
        &self,
        request: Request<EvaluateBackupPasswordRequest>,
    ) -> Result<Response<EvaluateBackupPasswordResponse>, Status> {
        let password = Password::new(request.into_inner().password);
        Ok(Response::new(backup::check_password(&password).into()))
    }

    async fn set_backup_password(
        &self,
        request: Request<SetBackupPasswordRequest>,
    ) -> Result<Response<SetBackupPasswordResponse>, Status> {
        let password = NewBackupPassword::try_from(Password::new(request.into_inner().password))
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.ask_brain(password, ClientAction::SetBackupPassword)
            .await?;
        Ok(Response::new(SetBackupPasswordResponse {}))
    }

    async fn verify_backup_password(
        &self,
        request: Request<VerifyBackupPasswordRequest>,
    ) -> Result<Response<VerifyBackupPasswordResponse>, Status> {
        let password = Password::new(request.into_inner().password);
        let correct = self
            .ask_brain(password, ClientAction::VerifyBackupPassword)
            .await?;
        Ok(Response::new(VerifyBackupPasswordResponse { correct }))
    }

    async fn snooze_backup_password_verification(
        &self,
        _request: Request<SnoozeBackupPasswordVerificationRequest>,
    ) -> Result<Response<SnoozeBackupPasswordVerificationResponse>, Status> {
        self.ask_brain((), ClientAction::SnoozeBackupPasswordVerification)
            .await?;
        Ok(Response::new(SnoozeBackupPasswordVerificationResponse {}))
    }

    async fn get_settings(
        &self,
        _request: Request<GetSettingsRequest>,
    ) -> Result<Response<SettingsResponseEnvelope>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetSettings(
                brain::types::BrainRequest {
                    request: (),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send get settings action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(response) => Ok(Response::new(SettingsResponseEnvelope {
                response: response.0.clone(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn mutate_settings(
        &self,
        request: Request<SettingsRequestEnvelope>,
    ) -> Result<Response<SettingsResponseEnvelope>, Status> {
        let request_bytes = request.into_inner().request;
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::MutateSettings(
                brain::types::BrainRequest {
                    request: SettingsMutation(request_bytes),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send mutate settings action"))?;

        let result = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        match result.as_ref() {
            Ok(response) => Ok(Response::new(SettingsResponseEnvelope {
                response: response.0.clone(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_listen_status(
        &self,
        _request: Request<GetListenStatusRequest>,
    ) -> Result<Response<GetListenStatusResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::GetListenStatus(
                brain::types::BrainRequest {
                    request: (),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send get listen status action"))?;

        let status = response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?;

        Ok(Response::new(status.into()))
    }

    async fn set_listen_port(
        &self,
        request: Request<SetListenPortRequest>,
    ) -> Result<Response<SetListenPortResponse>, Status> {
        let port = ListenPort::try_from(request.into_inner().port)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let (response_sender, response_receiver) = oneshot::channel();

        self.action_sender
            .send(BrainAction::Client(ClientAction::SetListenPort(
                brain::types::BrainRequest {
                    request: port,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .map_err(|_| Status::internal("Failed to send set listen port action"))?;

        response_receiver
            .await
            .map_err(|_| Status::internal("Failed to receive response"))?
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(SetListenPortResponse {}))
    }

    type StreamStream = DroppableStream<Result<ServerMessage, Status>, ClientMessage>;

    async fn stream(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let inner = request.into_inner();
        let out = self.stream_registry.new_connection(inner).await;
        Ok(Response::new(out))
    }
}

#[tonic::async_trait]
impl NodeStatusService for ApiServer {
    async fn get_node_status(
        &self,
        _request: Request<GetNodeStatusRequest>,
    ) -> Result<Response<GetNodeStatusResponse>, Status> {
        let (request, receiver) = BrainRequest::with_receiver(());
        self.action_sender
            .send(BrainAction::Client(ClientAction::GetNodeStatus(request)))
            .await
            .map_err(|_| Status::internal("Failed to request node status"))?;
        match receiver
            .await
            .map_err(|_| Status::internal("Failed to get node status"))?
            .as_ref()
        {
            Err(e) => Err(Status::internal(e.to_string())),
            Ok(node_info) => Ok(Response::new(GetNodeStatusResponse {
                status: Some(NodeStatus {
                    state: State::Running.into(),  // need to track, assume running for now
                    error_message: "".to_string(), // leave empty for now
                    info: Some(NodeInfo {
                        peer_id: node_info.peer_id.to_string(),
                        multiaddrs: node_info.external_addresses.clone(),
                        version: "0.0.1_rust".to_string(), // need to obtain from build process
                        metadata: Default::default(),      // leave empty for now
                        start_timestamp: node_info.start_timestamp as _,
                    }),
                    stats: Some(NodeStats {
                        uptime_seconds: 0, // uneccesary, need to update protobuf spec
                        connected_peers: node_info.connected_peers as _, // need to get from node info
                        known_peers: 0,         // uncertain what this should mean
                        bandwidth_in_bytes: 0,  // not needed for now
                        bandwidth_out_bytes: 0, // not needed for now
                        cpu_usage: 0.,          // not needed for now
                        memory_usage_bytes: 0,  // not needed for now
                    }),
                    // get current time
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|duration| duration.as_millis() as _)
                        .unwrap_or(0),
                }),
            })),
        }
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            is_healthy: true,
            message: "OK".to_string(),
        }))
    }
}

#[cfg(any(test, feature = "test-support"))]
pub struct MockRunnableFilerequestServer;

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl RunnableFilerequestServer for MockRunnableFilerequestServer {
    async fn run(self: Box<Self>) {}
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl FileRequestService for MockRunnableFilerequestServer {
    async fn get_file_request(
        &self,
        _request: Request<FileRequestRequest>,
    ) -> Result<Response<FileRequestResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn create_file_request(
        &self,
        _request: Request<CreateFileRequestRequest>,
    ) -> Result<Response<CreateFileRequestResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    async fn get_entitlement_status(
        &self,
        _request: Request<GetEntitlementStatusRequest>,
    ) -> Result<Response<GetEntitlementStatusResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_license_identity(
        &self,
        _request: Request<GetLicenseIdentityRequest>,
    ) -> Result<Response<GetLicenseIdentityResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn prepare_license_check(
        &self,
        _request: Request<PrepareLicenseCheckRequest>,
    ) -> Result<Response<PrepareLicenseCheckResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn submit_license_check_response(
        &self,
        _request: Request<SubmitLicenseCheckResponseRequest>,
    ) -> Result<Response<LicenseCheckAck>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn list_owned_devices(
        &self,
        _request: Request<ListOwnedDevicesRequest>,
    ) -> Result<Response<ListOwnedDevicesResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn rename_this_device(
        &self,
        _request: Request<RenameThisDeviceRequest>,
    ) -> Result<Response<RenameThisDeviceResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn remove_owned_device(
        &self,
        _request: Request<RemoveOwnedDeviceRequest>,
    ) -> Result<Response<RemoveOwnedDeviceResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn update_file_request(
        &self,
        _request: Request<UpdateFileRequestRequest>,
    ) -> Result<Response<UpdateFileRequestResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn delete_file_request(
        &self,
        _request: Request<DeleteFileRequestRequest>,
    ) -> Result<Response<DeleteFileRequestResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_all_file_requests(
        &self,
        _request: Request<GetAllFileRequestsRequest>,
    ) -> Result<Response<GetAllFileRequestsResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_contact_name(
        &self,
        _request: Request<GetContactNameRequest>,
    ) -> Result<Response<GetContactNameResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_contact_names(
        &self,
        _request: Request<GetContactNamesRequest>,
    ) -> Result<Response<GetContactNamesResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_contact(
        &self,
        _request: Request<GetContactRequest>,
    ) -> Result<Response<GetContactResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_all_contacts(
        &self,
        _request: Request<GetAllContactsRequest>,
    ) -> Result<Response<GetAllContactsResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn update_contact(
        &self,
        _request: Request<UpdateContactRequest>,
    ) -> Result<Response<UpdateContactResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn delete_contact(
        &self,
        _request: Request<DeleteContactRequest>,
    ) -> Result<Response<DeleteContactResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn update_self_contact_name(
        &self,
        _request: Request<UpdateSelfContactNameRequest>,
    ) -> Result<Response<UpdateSelfContactNameResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn wait_for_ready(
        &self,
        _: Request<WaitForReadyRequest>,
    ) -> Result<Response<WaitForReadyResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_full_self_contact(
        &self,
        _: Request<GetFullSelfContactRequest>,
    ) -> Result<Response<GetFullSelfContactResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn update_identity(
        &self,
        _new_self_contact: Request<UpdateIdentityRequest>,
    ) -> Result<Response<UpdateIdentityResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_self_contact_display(
        &self,
        _: Request<GetSelfContactDisplayRequest>,
    ) -> Result<Response<GetSelfContactDisplayResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn share_public_self_contact(
        &self,
        _: Request<SharePublicSelfContactRequest>,
    ) -> Result<Response<SharePublicSelfContactResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn register_contact(
        &self,
        _request: Request<RegisterContactRequest>,
    ) -> Result<Response<RegisterContactResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn decode_signed_contact(
        &self,
        _request: Request<DecodeSignedContactRequest>,
    ) -> Result<Response<DecodeSignedContactResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn store_remote_file_request(
        &self,
        _request: Request<StoreRemoteFileRequestRequest>,
    ) -> Result<Response<StoreRemoteFileRequestResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_remote_file_request(
        &self,
        _request: Request<GetRemoteFileRequestRequest>,
    ) -> Result<Response<GetRemoteFileRequestResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn list_remote_file_requests(
        &self,
        _request: Request<ListRemoteFileRequestsRequest>,
    ) -> Result<Response<ListRemoteFileRequestsResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_all_remote_file_requests(
        &self,
        _request: Request<GetAllRemoteFileRequestsRequest>,
    ) -> Result<Response<GetAllRemoteFileRequestsResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn delete_remote_file_request(
        &self,
        _request: Request<DeleteRemoteFileRequestRequest>,
    ) -> Result<Response<DeleteRemoteFileRequestResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn store_pending_file(
        &self,
        _request: Request<StorePendingFileRequest>,
    ) -> Result<Response<StorePendingFileResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_pending_file(
        &self,
        _request: Request<GetPendingFileRequest>,
    ) -> Result<Response<GetPendingFileResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn list_pending_files(
        &self,
        _request: Request<ListPendingFilesRequest>,
    ) -> Result<Response<ListPendingFilesResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_all_pending_files(
        &self,
        _request: Request<GetAllPendingFilesRequest>,
    ) -> Result<Response<GetAllPendingFilesResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn delete_pending_file(
        &self,
        _request: Request<DeletePendingFileRequest>,
    ) -> Result<Response<DeletePendingFileResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn update_remote_file_request(
        &self,
        _request: Request<UpdateRemoteFileRequestRequest>,
    ) -> Result<Response<UpdateRemoteFileRequestResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_node_peer_id(
        &self,
        _request: Request<GetNodePeerIdRequest>,
    ) -> Result<Response<GetNodePeerIdResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn list_received_files(
        &self,
        _request: Request<ListReceivedFilesRequest>,
    ) -> Result<Response<ListReceivedFilesResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn delete_received_file(
        &self,
        _request: Request<DeleteReceivedFileRequest>,
    ) -> Result<Response<DeleteReceivedFileResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn resolve_filerequest_location(
        &self,
        _request: Request<ResolveFilerequestLocationRequest>,
    ) -> Result<Response<ResolveFilerequestLocationResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_self_contact_invite_challenge(
        &self,
        _request: Request<GetSelfContactInviteChallengeRequest>,
    ) -> Result<Response<GetSelfContactInviteChallengeResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn unregister_self_contact_invite_challenge(
        &self,
        _request: Request<UnregisterSelfContactInviteChallengeRequest>,
    ) -> Result<Response<UnregisterSelfContactInviteChallengeResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn use_self_contact_invite(
        &self,
        _request: Request<UseSelfContactInviteRequest>,
    ) -> Result<Response<UseSelfContactInviteResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_contact_share_challenge(
        &self,
        _request: Request<GetContactShareChallengeRequest>,
    ) -> Result<Response<GetContactShareChallengeResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn unregister_contact_share_challenge(
        &self,
        _request: Request<UnregisterContactShareChallengeRequest>,
    ) -> Result<Response<UnregisterContactShareChallengeResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn backup_data(
        &self,
        _request: tonic::Request<BackupDataRequest>,
    ) -> Result<Response<BackupDataResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn restore_data(
        &self,
        _request: tonic::Request<RestoreDataRequest>,
    ) -> Result<Response<RestoreDataResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_backup_password_status(
        &self,
        _request: tonic::Request<GetBackupPasswordStatusRequest>,
    ) -> Result<Response<GetBackupPasswordStatusResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn evaluate_backup_password(
        &self,
        _request: tonic::Request<EvaluateBackupPasswordRequest>,
    ) -> Result<Response<EvaluateBackupPasswordResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn set_backup_password(
        &self,
        _request: tonic::Request<SetBackupPasswordRequest>,
    ) -> Result<Response<SetBackupPasswordResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn verify_backup_password(
        &self,
        _request: tonic::Request<VerifyBackupPasswordRequest>,
    ) -> Result<Response<VerifyBackupPasswordResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn snooze_backup_password_verification(
        &self,
        _request: tonic::Request<SnoozeBackupPasswordVerificationRequest>,
    ) -> Result<Response<SnoozeBackupPasswordVerificationResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn use_contact_share_challenge(
        &self,
        _request: Request<UseContactShareChallengeRequest>,
    ) -> Result<Response<UseContactShareChallengeResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_settings(
        &self,
        _request: Request<GetSettingsRequest>,
    ) -> Result<Response<SettingsResponseEnvelope>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn mutate_settings(
        &self,
        _request: Request<SettingsRequestEnvelope>,
    ) -> Result<Response<SettingsResponseEnvelope>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn get_listen_status(
        &self,
        _request: Request<GetListenStatusRequest>,
    ) -> Result<Response<GetListenStatusResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    async fn set_listen_port(
        &self,
        _request: Request<SetListenPortRequest>,
    ) -> Result<Response<SetListenPortResponse>, Status> {
        todo!("Mock don't do nothing")
    }

    type StreamStream = DroppableStream<Result<ServerMessage, Status>, ClientMessage>;

    async fn stream(
        &self,
        _request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        todo!("Mock don't do nothing")
    }
}
