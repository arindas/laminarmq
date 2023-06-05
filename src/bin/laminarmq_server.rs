use glommio::{executor, Latency, LocalExecutorBuilder, Placement, Shares};
use hyper::{service::service_fn, Body, Request, Response, StatusCode};
use laminarmq::server::{
    impls::glommio::hyper_compat::{ConnControl, HyperServer},
    Server,
};
use std::{convert::Infallible, num::NonZeroUsize, rc::Rc};
use tracing::{debug, info, instrument, subscriber, Level};
use tracing::{info_span, Instrument};
use tracing_subscriber::FmtSubscriber;

const THREAD_NAME: &str = "laminarmq_server_thread_0";

struct State;

#[cfg(not(tarpaulin_include))]
#[instrument(skip(_shared_state))]
async fn request_handler(
    _shared_state: Rc<State>,
    request: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let response = Err((String::with_capacity(0), StatusCode::NOT_FOUND));

    debug!("Response received.");

    match match response {
        Err((message, status)) if message.is_empty() => {
            Err((status.canonical_reason().unwrap_or_default().into(), status))
        }
        response => response,
    } {
        Ok(body) => Ok(Response::new(body)),
        Err((message, status)) => Ok(Response::builder()
            .status(status)
            .body(Body::from(message))
            .unwrap_or_default()),
    }
}

#[cfg(not(tarpaulin_include))]
#[cfg(target_os = "linux")]
fn main() {
    let fmt_subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    subscriber::set_global_default(fmt_subscriber).expect("setting default subscriber failed");

    let (signal_tx, mut signal_rx) = tokio::sync::mpsc::channel::<()>(1);

    ctrlc_async::set_async_handler(
        async move {
            info!("Received CTRL+C.");
            signal_tx
                .send(())
                .await
                .expect("unable to send on signal channel");
        }
        .instrument(info_span!("ctrlc_async_handler")),
    )
    .expect("Error setting Ctrl-C handler");

    LocalExecutorBuilder::new(Placement::Unbound)
        .name(THREAD_NAME)
        .spawn(|| async move {
            let shared_state = Rc::new(State {});

            let rpc_server_tq = executor().create_task_queue(
                Shares::default(),
                Latency::NotImportant,
                "rpc_server_tq",
            );

            let server = HyperServer {
                max_connections: NonZeroUsize::new(1024).unwrap(),
                conn_control: ConnControl::Blocking,
                task_q: rpc_server_tq,
            };

            let (socket_addr, server_task) = server
                .serve(service_fn(move |req| {
                    request_handler(shared_state.clone(), req)
                }))
                .expect("serve_http errored out.");

            let server_join_handle = server_task.detach();

            info!("Listening for HTTP requests on {:?}", socket_addr);

            signal_rx.recv().await;

            // stop the future that the the server is listening
            // on from being polled any further
            server_join_handle.cancel();
            server_join_handle.await; // join() on server task

            info!("Done Listening to requests.");
        })
        .expect("unable to spawn root future")
        .join()
        .unwrap_or_else(|_| panic!("failed to join -> {THREAD_NAME}"));
}
