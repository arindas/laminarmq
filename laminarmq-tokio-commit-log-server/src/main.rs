use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use hyper::{Body, Request};
use laminarmq::{
    common::{cache::NoOpCache, serde_compat::bincode},
    storage::{
        commit_log::{
            segmented_log::{MetaWithIdx, SegmentedLog},
            CommitLog, Record,
        },
        impls::in_mem::{segment::InMemSegmentStorageProvider, storage::InMemStorage},
    },
};
use serde::{Deserialize, Serialize};
use std::{io, net::SocketAddr, rc::Rc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task,
};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
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

#[derive(Default, Clone)]
struct AppState {}

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
        .with_state(AppState::default());

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexBoundsResponse {
    highest_index: u32,
    lowest_index: u32,
}

async fn index_bounds(State(_state): State<AppState>) -> impl IntoResponse {
    let index_bounds_response = IndexBoundsResponse {
        highest_index: 0,
        lowest_index: 0,
    };

    Json(index_bounds_response)
}

async fn read(Path(_index): Path<u32>, State(_state): State<AppState>) -> impl IntoResponse {
    ""
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendResponse {
    write_index: u32,
}

async fn append(State(_state): State<AppState>, _request: Request<Body>) -> impl IntoResponse {
    Json(AppendResponse { write_index: 0 })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TruncateRequest {
    truncate_index: u32,
}

async fn truncate(
    State(_state): State<AppState>,
    Json(_truncate_request): Json<TruncateRequest>,
) -> impl IntoResponse {
    ""
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

pub struct Message {
    resp_tx: oneshot::Sender<AppResponse>,
    request: AppRequest,
}

#[allow(unused)]
pub struct CommitLogServer<CL> {
    message_rx: mpsc::Receiver<Message>,
    commit_log: CL,
}

impl<CL> CommitLogServer<CL> {
    pub fn new(message_rx: mpsc::Receiver<Message>, commit_log: CL) -> Self {
        Self {
            message_rx,
            commit_log,
        }
    }
}

#[derive(Debug)]
pub enum CommitLogServerError<CLE> {
    CommitLogError(CLE),
    IoError(io::Error),
}

impl<CL> CommitLogServer<CL>
where
    CL: CommitLog<MetaWithIdx<(), u32>, Vec<u8>, Idx = u32> + 'static,
{
    pub fn serve(self) -> Result<(), CommitLogServerError<CL::Error>> {
        let (mut message_rx, commit_log) = (self.message_rx, self.commit_log);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(CommitLogServerError::IoError)?;

        rt.block_on(async move {
            let local = task::LocalSet::new();

            local
                .run_until(async move {
                    let commit_log = Rc::new(RwLock::new(commit_log));

                    while let Some(Message { resp_tx, request }) = message_rx.recv().await {
                        let commit_log_copy = commit_log.clone();

                        task::spawn_local(async move {
                            let commit_log = commit_log_copy;
                            let response = match request {
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
                                    .map(|write_index| {
                                        AppResponse::Append(AppendResponse { write_index })
                                    })
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
                            }?;

                            resp_tx
                                .send(response)
                                .map_err(|_| {
                                    io::Error::new(
                                        io::ErrorKind::Other,
                                        "response_tx.send() failed",
                                    )
                                })
                                .map_err(CommitLogServerError::IoError)?;

                            Ok::<(), CommitLogServerError<CL::Error>>(())
                        });
                    }
                })
                .await;
        });

        Ok(())
    }
}
