use glommio::{executor, Latency, LocalExecutorBuilder, Placement, Shares};
use hyper::{Body, Request, Response, StatusCode};
use std::convert::Infallible;
use std::{rc::Rc, time::Duration};
use tracing::{debug, error, info, instrument, subscriber, Level};
use tracing_subscriber::FmtSubscriber;

use laminarmq::server::router::{single_node::Router, Router as _};

const THREAD_NAME: &str = "laminarmq_server_thread_0";

struct State {
    pub router: Router,
    pub _task_tx: (),
}

#[cfg(not(tarpaulin_include))]
#[instrument(skip(shared_state))]
async fn request_handler(
    shared_state: Rc<State>,
    request: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let response = if let Some(request) = shared_state.router.route(request).await {
        debug!("serving => {:?}", request);

        Ok(Body::from("Valid request!"))
    } else {
        error!("Request not routed.");

        Err((String::with_capacity(0), StatusCode::NOT_FOUND))
    };

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
    use hyper::service::service_fn;
    use laminarmq::server::{glommio_impl::hyper_compat::HyperServer, Server};
    use tracing::{info_span, Instrument};

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
            let shared_state = Rc::new(State {
                router: Router::default(),
                _task_tx: (),
            });

            let rpc_server_tq = executor().create_task_queue(
                Shares::default(),
                Latency::Matters(Duration::from_micros(10)),
                "rpc_server_tq",
            );

            let server = HyperServer::new(1024, rpc_server_tq, ([0, 0, 0, 0], 8080));

            server
                .serve(service_fn(move |req| {
                    request_handler(shared_state.clone(), req)
                }))
                .expect("serve_http errored out.");

            info!("Listening for HTTP requests on 0.0.0.0:8080");

            signal_rx.recv().await;

            info!("Done Listening to requests.");
        })
        .expect("unable to spawn root future")
        .join()
        .unwrap_or_else(|_| panic!("failed to join -> {THREAD_NAME}"));
}
