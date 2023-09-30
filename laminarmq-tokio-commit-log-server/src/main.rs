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
        commit_log::segmented_log::SegmentedLog,
        impls::in_mem::{segment::InMemSegmentStorageProvider, storage::InMemStorage},
    },
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
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

async fn index_bounds(State(_state): State<AppState>) -> Json<IndexBoundsResponse> {
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

async fn append(State(_state): State<AppState>, _request: Request<Body>) -> Json<AppendResponse> {
    Json(AppendResponse { write_index: 0 })
}

#[derive(Debug, Serialize, Deserialize)]
struct TruncateRequest {
    truncate_index: u32,
}

async fn truncate(
    State(_state): State<AppState>,
    Json(_truncate_request): Json<TruncateRequest>,
) -> impl IntoResponse {
    ""
}
