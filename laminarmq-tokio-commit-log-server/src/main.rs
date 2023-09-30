use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use hyper::{Body, Request};
use laminarmq::common::serde_compat::bincode;
use laminarmq::{
    common::cache::NoOpCache,
    storage::{
        commit_log::{
            segmented_log::{MetaWithIdx, SegmentedLog},
            CommitLog,
        },
        impls::in_mem::{segment::InMemSegmentStorageProvider, storage::InMemStorage},
    },
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

trait AppState {
    type CL: CommitLog<MetaWithIdx<(), u32>, Vec<u8>>;

    fn commit_log_mut(&mut self) -> &mut Self::CL;

    fn commit_log(&self) -> &Self::CL;
}

struct ParameterizedAppState<CL> {
    commit_log: CL,
}

impl<CL> AppState for ParameterizedAppState<CL>
where
    CL: CommitLog<MetaWithIdx<(), u32>, Vec<u8>>,
{
    type CL = CL;

    fn commit_log_mut(&mut self) -> &mut Self::CL {
        &mut self.commit_log
    }

    fn commit_log(&self) -> &Self::CL {
        &self.commit_log
    }
}

type InMemSegLog = SegmentedLog<
    InMemStorage,
    (),
    crc32fast::Hasher,
    u32,
    usize,
    bincode::BinCode,
    InMemSegmentStorageProvider<u32>,
    NoOpCache<usize, ()>,
>;

#[derive(Default)]
struct BadCL;

impl AppState for BadCL {
    type CL = InMemSegLog;

    fn commit_log_mut(&mut self) -> &mut Self::CL {
        todo!()
    }

    fn commit_log(&self) -> &Self::CL {
        todo!()
    }
}

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

    let state = SharedState::<BadCL>::default();

    // Compose the routes
    let app = Router::new()
        .route("/rpc/truncate", post(truncate))
        .route("/records/:index", get(read))
        .route("/records", post(append))
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
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn append<S: AppState + 'static>(
    State(_state): State<SharedState<S>>,
    _request: Request<Body>,
) -> Result<impl IntoResponse, StatusCode> {
    Ok("")
}

async fn read<S: AppState>(
    Path(_index): Path<u32>,
    State(_state): State<SharedState<S>>,
) -> Result<impl IntoResponse, StatusCode> {
    Ok("")
}

#[derive(Debug, Serialize, Deserialize)]
struct TruncateRequest {
    truncate_index: u32,
}

async fn truncate<S: AppState>(
    State(_state): State<SharedState<S>>,
    Json(_truncate_request): Json<TruncateRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    Ok("")
}

type SharedState<S> = Arc<RwLock<S>>;
