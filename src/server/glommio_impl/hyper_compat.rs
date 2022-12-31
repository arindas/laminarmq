//! Module providing utilities for setting up a [`hyper`] server using [`glommio`].

use crate::server::tokio_compat::TokioIO;
use futures_lite::Future;
use glommio::{net::TcpListener, sync::Semaphore, TaskQueueHandle};
use hyper::{
    body::HttpBody, rt::Executor, server::conn::Http, service::service_fn, Request, Response,
};
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
        glommio::spawn_local_into(f, self.task_queue_handle)
            .map(|task| task.detach())
            .map_err(|spawn_error| {
                error!(
                    "Error: {:?} when spawning future on queue: {:?}",
                    spawn_error, self.task_queue_handle
                );
            })
            .ok();
    }
}

/// Serves HTTP requests at the given address using the given parameters. All request handling
/// futures are spawned on the given [`TaskQueueHandle`].
#[instrument(skip(addr, service))]
pub async fn serve_http<S, RespBd, F, FError, A>(
    addr: A,
    service: S,
    max_connections: usize,
    task_queue_handle: TaskQueueHandle,
) -> io::Result<()>
where
    S: FnMut(Request<hyper::Body>) -> F + 'static + Copy,
    RespBd: HttpBody + 'static,
    RespBd::Error: std::error::Error + Send + Sync,
    F: Future<Output = Result<Response<RespBd>, FError>> + 'static,
    FError: std::error::Error + 'static + Send + Sync,
    A: Into<SocketAddr>,
{
    let listener = TcpListener::bind(addr.into())?;
    let conn_control = Rc::new(Semaphore::new(max_connections as _));

    loop {
        let stream = listener.accept().await?;
        let addr = stream.local_addr()?;

        let scoped_conn_control = conn_control.clone();

        glommio::spawn_local_into(
            async move {
                let _permit = scoped_conn_control.acquire_permit(1).await;

                let http = Http::new().with_executor(HyperExecutor { task_queue_handle });
                let http = http.serve_connection(TokioIO(stream), service_fn(service));

                match http.await {
                    Err(err) if !err.is_incomplete_message() => {
                        error!("Stream from {:?} failed with error {:?}", addr, err)
                    }
                    _ => (),
                }
            }
            .instrument(error_span!("rpc-server-tq")),
            task_queue_handle,
        )
        .map(|task| task.detach())
        .map_err(|spawn_error| {
            error!(
                "Error: {:?} spawning task on queue: {:?}",
                spawn_error, task_queue_handle
            )
        })
        .ok();
    }
}
