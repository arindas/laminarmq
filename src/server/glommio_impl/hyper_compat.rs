//! Module providing utilities for setting up a [`hyper`] server using [`glommio`].

use crate::server::tokio_compat::TokioIO;
use futures_lite::Future;
use glommio::{enclose, net::TcpListener, sync::Semaphore, TaskQueueHandle};
use hyper::{rt::Executor, server::conn::Http, service::service_fn, Body, Request, Response};
use std::{io, net::SocketAddr, rc::Rc};
use tracing::{error, error_span, instrument, Instrument};

/// [`hyper::rt::Executor`] implementation that executes futures by spawning them on a
/// [`glommio::TaskQueueHandle`].
#[derive(Clone)]
struct HyperExecutor {
    task_queue_handle: TaskQueueHandle,
}

impl<F> Executor<F> for HyperExecutor
where
    F: Future + 'static,
    F::Output: 'static,
{
    #[instrument(skip(self, f))]
    fn execute(&self, f: F) {
        if let Err(err) = glommio::spawn_local_into(f, self.task_queue_handle).map(|x| x.detach()) {
            error!(
                "Error: {:?} when spawning future on queue: {:?}",
                err, self.task_queue_handle
            );
        }
    }
}

/// Serves HTTP requests at the given address using the given parameters. All request handling
/// futures are spawned on the given [`TaskQueueHandle`].
pub async fn serve_http<S, F, R, A>(
    addr: A,
    service: S,
    max_connections: usize,
    task_queue_handle: TaskQueueHandle,
) -> io::Result<()>
where
    S: FnMut(Request<Body>) -> F + 'static + Copy,
    F: Future<Output = Result<Response<Body>, R>> + 'static,
    R: std::error::Error + 'static + Send + Sync,
    A: Into<SocketAddr>,
{
    let listener = TcpListener::bind(addr.into())?;
    let conn_control = Rc::new(Semaphore::new(max_connections as _));
    loop {
        match listener.accept().await {
            Err(x) => {
                return Err(x.into());
            }
            Ok(stream) => {
                let addr = stream.local_addr().unwrap();
                glommio::spawn_local(
                    enclose! {(conn_control) async move {
                        let _permit = conn_control.acquire_permit(1).await;

                        if let Err(x) = Http::new().with_executor(HyperExecutor{task_queue_handle})
                            .serve_connection(TokioIO(stream), service_fn(service)).await {
                            if !x.is_incomplete_message() {
                                error!("Stream from {:?} failed with error {:?}", addr, x);
                            }
                        }
                    }}
                    .instrument(error_span!("hyper_http_server")),
                )
                .detach();
            }
        }
    }
}
