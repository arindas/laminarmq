use glommio::{executor, Latency, LocalExecutorBuilder, Placement, Shares};
use hyper::{Body, Request, Response, StatusCode};
use std::{convert::Infallible, rc::Rc, time::Duration};

use laminarmq::server::glommio_impl::hyper_compat::serve_http;
use laminarmq::server::router::{single_node::Router, Router as _};
const THREAD_NAME: &str = "laminarmq_server_thread_0";

struct State {
    pub router: Router,
    pub _task_tx: (),
}

async fn request_handler(
    shared_state: Rc<State>,
    request: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let request = shared_state.router.route(request).await;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(format!("{:?}\n", request)))
        .expect("unable to construct body."))
}

#[cfg(target_os = "linux")]
fn main() {
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

            serve_http(
                ([0, 0, 0, 0], 8080),
                move |req| request_handler(shared_state.clone(), req),
                1024,
                rpc_server_tq,
            )
            .expect("serve_http errored out.");

            std::future::pending::<()>().await;
        })
        .expect("unable to spawn root future")
        .join()
        .unwrap_or_else(|_| panic!("failed to join -> {THREAD_NAME}"));
}
