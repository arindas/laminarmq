use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use hyper::{Body, Request};

extern crate laminarmq;

use laminarmq::{
    common::{cache::NoOpCache, serde_compat::bincode},
    storage::{
        commit_log::{
            segmented_log::{segment::Config as SegmentConfig, Config, MetaWithIdx, SegmentedLog},
            CommitLog, Record,
        },
        impls::{
            common::DiskBackedSegmentStorageProvider,
            in_mem::{segment::InMemSegmentStorageProvider, storage::InMemStorage},
            tokio::storage::std_seek_read::{
                StdSeekReadFileStorage, StdSeekReadFileStorageProvider,
            },
        },
    },
};
use serde::{Deserialize, Serialize};
use std::{
    env,
    fmt::Debug,
    future::Future,
    io,
    net::SocketAddr,
    rc::Rc,
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::{
    signal,
    sync::{mpsc, oneshot, AcquireError, RwLock, Semaphore},
    task,
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing::{error, error_span, info, info_span, Instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub type InMemSegLog = SegmentedLog<
    InMemStorage,
    (),
    crc32fast::Hasher,
    u32,
    usize,
    bincode::BinCode,
    InMemSegmentStorageProvider<u32>,
    NoOpCache<usize, ()>,
>;

#[allow(unused)]
#[derive(Clone)]
struct AppState {
    message_tx: mpsc::Sender<Message>,
}

#[derive(Debug)]
pub enum ChannelError {
    SendError,
    RecvError,
}

impl AppState {
    pub async fn enqueue_request(
        &self,
        request: AppRequest,
    ) -> Result<oneshot::Receiver<ResponseResult>, ChannelError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        let message = Message::Connection { resp_tx, request };

        self.message_tx
            .send(message)
            .await
            .map_err(|_| ChannelError::SendError)?;

        Ok(resp_rx)
    }
}

pub struct CommitLogServerConfig {
    message_buffer_size: usize,
    max_connections: usize,
}

#[allow(unused)]
const IN_MEMORY_SEGMENTED_LOG_CONFIG: Config<u32, usize> = Config {
    segment_config: SegmentConfig {
        max_store_size: 1048576, // = 1MiB
        max_store_overflow: 524288,
        max_index_size: 1048576,
    },
    initial_index: 0,
    num_index_cached_read_segments: None,
};

const PERSISTENT_SEGMENTED_LOG_CONFIG: Config<u32, u64> = Config {
    segment_config: SegmentConfig {
        max_store_size: 10000000, // ~ 10MB
        max_store_overflow: 10000000 / 2,
        max_index_size: 10000000,
    },
    initial_index: 0,
    num_index_cached_read_segments: None,
};

const DEFAULT_STORAGE_DIRECTORY: &str = "./.storage/laminarmq_tokio_commit_log_server/commit_log";

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "laminarmq_tokio_commit_log_server=debug,tower_http=debug".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let storage_directory =
        env::var("STORAGE_DIRECTORY").unwrap_or(DEFAULT_STORAGE_DIRECTORY.into());

    let (join_handle, message_tx) = CommitLogServer::orchestrate(
        CommitLogServerConfig {
            message_buffer_size: 1024,
            max_connections: 512,
        },
        || async {
            let disk_backed_storage_provider =
                DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
                    storage_directory,
                    StdSeekReadFileStorageProvider,
                )
                .unwrap();

            SegmentedLog::<
                StdSeekReadFileStorage,
                (),
                crc32fast::Hasher,
                u32,
                u64,
                bincode::BinCode,
                _,
                NoOpCache<usize, ()>,
            >::new(
                PERSISTENT_SEGMENTED_LOG_CONFIG,
                disk_backed_storage_provider,
            )
            .await
            .unwrap()
        },
    );

    // Compose the routes
    let app = Router::new()
        .route("/index_bounds", get(index_bounds))
        .route("/records/:index", get(read))
        .route("/records", post(append))
        .route("/rpc/truncate", post(truncate))
        // Add middleware to all routes
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|error: BoxError| async move {
                    if error.is::<tower::timeout::error::Elapsed>() {
                        Ok(StatusCode::REQUEST_TIMEOUT)
                    } else {
                        Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Unhandled internal error: {}", error),
                        ))
                    }
                }))
                .timeout(Duration::from_secs(10))
                .layer(TraceLayer::new_for_http())
                .into_inner(),
        )
        .with_state(AppState {
            message_tx: message_tx.clone(),
        });

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    tracing::debug!("listening on {}", addr);

    hyper::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

    message_tx.send(Message::Terminate).await.unwrap();

    tokio::task::spawn_blocking(|| join_handle.join())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    info!("Exiting application.");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("signal received, starting graceful shutdown");
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexBoundsResponse {
    highest_index: u32,
    lowest_index: u32,
}

pub struct StringError(String);

impl From<String> for StringError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl IntoResponse for StringError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0).into_response()
    }
}

async fn index_bounds(
    State(state): State<AppState>,
) -> Result<Json<IndexBoundsResponse>, StringError> {
    let resp_rx = state
        .enqueue_request(AppRequest::IndexBounds)
        .await
        .map_err(|err| format!("error sending request to commit_log_server: {:?}", err))?;

    let response = resp_rx
        .await
        .map_err(|err| format!("error receiving response: {:?}", err))??;

    if let AppResponse::IndexBounds(index_bounds_response) = response {
        Ok(Json(index_bounds_response))
    } else {
        Err(StringError("invalid response type".into()))
    }
}

async fn read(
    Path(index): Path<u32>,
    State(state): State<AppState>,
) -> Result<Vec<u8>, StringError> {
    let resp_rx = state
        .enqueue_request(AppRequest::Read { index })
        .await
        .map_err(|err| format!("error sending request to commit_log_server: {:?}", err))?;

    let response = resp_rx
        .await
        .map_err(|err| format!("error receiving response: {:?}", err))??;

    if let AppResponse::Read { record_value } = response {
        Ok(record_value)
    } else {
        Err(StringError("invalid response type".into()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendResponse {
    write_index: u32,
}

async fn append(
    State(state): State<AppState>,
    request: Request<Body>,
) -> Result<Json<AppendResponse>, StringError> {
    let resp_rx = state
        .enqueue_request(AppRequest::Append {
            record_value: request.into_body(),
        })
        .await
        .map_err(|err| format!("error sending request to commit_log_server: {:?}", err))?;

    let response = resp_rx
        .await
        .map_err(|err| format!("error receiving reponse: {:?}", err))??;

    if let AppResponse::Append(append_reponse) = response {
        Ok(Json(append_reponse))
    } else {
        Err(StringError("invalid response type".into()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TruncateRequest {
    truncate_index: u32,
}

async fn truncate(
    State(state): State<AppState>,
    Json(truncate_request): Json<TruncateRequest>,
) -> Result<(), StringError> {
    let resp_rx = state
        .enqueue_request(AppRequest::Truncate(truncate_request))
        .await
        .map_err(|err| format!("error sending request to commit_log_server: {:?}", err))?;

    let response = resp_rx
        .await
        .map_err(|err| format!("error receiving response: {:?}", err))??;

    if let AppResponse::Truncate = response {
        Ok(())
    } else {
        Err(StringError("invalid response type".into()))
    }
}

#[derive(Debug)]
pub enum AppResponse {
    IndexBounds(IndexBoundsResponse),
    Read { record_value: Vec<u8> },
    Append(AppendResponse),
    Truncate,
}

#[derive(Debug)]
pub enum AppRequest {
    IndexBounds,
    Read { index: u32 },
    Append { record_value: Body },
    Truncate(TruncateRequest),
}

type ResponseResult = Result<AppResponse, String>;

pub enum Message {
    Connection {
        resp_tx: oneshot::Sender<ResponseResult>,
        request: AppRequest,
    },

    Terminate,
}

#[allow(unused)]
pub struct CommitLogServer<CL> {
    message_rx: mpsc::Receiver<Message>,
    commit_log: CL,
    max_connections: usize,
}

impl<CL> CommitLogServer<CL> {
    pub fn new(
        message_rx: mpsc::Receiver<Message>,
        commit_log: CL,
        max_connections: usize,
    ) -> Self {
        Self {
            message_rx,
            commit_log,
            max_connections,
        }
    }
}

#[derive(Debug)]
pub enum CommitLogServerError<CLE> {
    ConnPermitAcquireError(AcquireError),
    CommitLogError(CLE),
    IoError(io::Error),
    ResponseSendError,
}

pub type CommitLogServerResult<T, CLE> = Result<T, CommitLogServerError<CLE>>;

impl<CL> CommitLogServer<CL>
where
    CL: CommitLog<MetaWithIdx<(), u32>, Vec<u8>, Idx = u32> + 'static,
{
    pub async fn handle_request(
        commit_log: Rc<RwLock<CL>>,
        request: AppRequest,
    ) -> Result<AppResponse, CommitLogServerError<CL::Error>> {
        match request {
            AppRequest::IndexBounds => {
                let commit_log = commit_log.read().await;

                Ok(AppResponse::IndexBounds(IndexBoundsResponse {
                    highest_index: commit_log.highest_index(),
                    lowest_index: commit_log.lowest_index(),
                }))
            }

            AppRequest::Read { index: idx } => commit_log
                .read()
                .await
                .read(&idx)
                .await
                .map(|Record { metadata: _, value }| AppResponse::Read {
                    record_value: value,
                })
                .map_err(CommitLogServerError::CommitLogError),

            AppRequest::Append { record_value } => commit_log
                .write()
                .await
                .append(Record {
                    metadata: MetaWithIdx {
                        metadata: (),
                        index: None,
                    },
                    value: record_value,
                })
                .await
                .map(|write_index| AppResponse::Append(AppendResponse { write_index }))
                .map_err(CommitLogServerError::CommitLogError),

            AppRequest::Truncate(TruncateRequest {
                truncate_index: idx,
            }) => commit_log
                .write()
                .await
                .truncate(&idx)
                .await
                .map(|_| AppResponse::Truncate)
                .map_err(CommitLogServerError::CommitLogError),
        }
    }

    pub async fn serve(self) {
        let (mut message_rx, commit_log, max_connections) =
            (self.message_rx, self.commit_log, self.max_connections);

        let conn_semaphore = Rc::new(Semaphore::new(max_connections));
        let commit_log = Rc::new(RwLock::new(commit_log));

        let commit_log_copy = commit_log.clone();

        let local = task::LocalSet::new();

        local
            .run_until(async move {
                while let Some(Message::Connection { resp_tx, request }) = message_rx.recv().await {
                    let (conn_semaphore, commit_log_copy) =
                        (conn_semaphore.clone(), commit_log_copy.clone());

                    task::spawn_local(
                        async move {
                            let response = async move {
                                let _semaphore_permit = conn_semaphore
                                    .acquire()
                                    .await
                                    .map_err(CommitLogServerError::ConnPermitAcquireError)?;

                                let commit_log = commit_log_copy;

                                let response = Self::handle_request(commit_log, request).await?;

                                Ok::<_, CommitLogServerError<CL::Error>>(response)
                            }
                            .await
                            .map_err(|err| format!("{:?}", err));

                            if let Err(err) = resp_tx.send(response) {
                                error!("error sending response: {:?}", err)
                            }
                        }
                        .instrument(error_span!("commit_log_server_handler_task")),
                    );
                }
            })
            .await;

        match Rc::into_inner(commit_log) {
            Some(commit_log) => match commit_log.into_inner().close().await {
                Ok(_) => {}
                Err(err) => error!("error closing commit_log: {:?}", err),
            },
            None => error!("unable to unrwap commit_log Rc"),
        };

        info!("Closed commit_log.");
    }

    pub fn orchestrate<CLP, CLF>(
        server_config: CommitLogServerConfig,
        commit_log_provider: CLP,
    ) -> (JoinHandle<Result<(), io::Error>>, mpsc::Sender<Message>)
    where
        CLP: FnOnce() -> CLF + Send + 'static,
        CLF: Future<Output = CL>,
        CL::Error: Send + 'static,
    {
        let CommitLogServerConfig {
            message_buffer_size,
            max_connections,
        } = server_config;

        let (message_tx, message_rx) = mpsc::channel::<Message>(message_buffer_size);

        (
            thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread().build()?;

                rt.block_on(
                    async move {
                        let commit_log_server = CommitLogServer::new(
                            message_rx,
                            commit_log_provider().await,
                            max_connections,
                        );

                        commit_log_server.serve().await;

                        info!("Done serving requests.");
                    }
                    .instrument(info_span!("commit_log_server")),
                );

                Ok(())
            }),
            message_tx,
        )
    }
}
