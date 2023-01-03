//! Module providing utilities for setting up a [`hyper`] server using [`glommio`].

use crate::server::tokio_compat::TokioIO;
use futures_lite::Future;
use glommio::{net::TcpListener, sync::Semaphore, GlommioError, TaskQueueHandle as TaskQ};
use hyper::{rt::Executor, server::conn::Http, service::service_fn, Request, Response};
use std::{
    io::{
        self,
        ErrorKind::{ConnectionRefused, ConnectionReset},
    },
    net::SocketAddr,
    rc::Rc,
};
use tracing::{error, error_span, instrument, Instrument};

/// [`hyper::rt::Executor`] implementation that executes futures by spawning them on a
/// [`glommio::TaskQueueHandle`].
#[derive(Clone)]
struct HyperExecutor {
    task_q: TaskQ,
}

impl<F> Executor<F> for HyperExecutor
where
    F: Future + 'static,
    F::Output: 'static,
{
    #[instrument(skip(self, f))]
    fn execute(&self, f: F) {
        glommio::spawn_local_into(f, self.task_q)
            .map_err(|spawn_error| {
                error!(
                    "spawn_local_into -> {:?} ==> {:?}",
                    self.task_q, spawn_error
                );
            })
            .map(|task| task.detach())
            .ok();
    }
}

pub struct ConnResult(SocketAddr, Result<(), hyper::Error>);

impl From<ConnResult> for io::Result<()> {
    fn from(value: ConnResult) -> Self {
        match value.1 {
            Err(err) if !err.is_incomplete_message() => {
                error!("Stream from {:?} failed with error {:?}", value.0, err);
                Err(())
            }
            Err(_) => Err(()),
            _ => Ok(()),
        }
        .map_err(|_| io::Error::from(ConnectionReset))
    }
}

/// Serves HTTP requests at the given address using the given parameters. All request
/// handling futures are spawned on the given [`TaskQueueHandle`].
#[instrument(skip(addr, service))]
pub fn serve_http<S, RespBd, F, FError, A>(
    addr: A,
    service: S,
    max_connections: usize,
    task_q: TaskQ,
) -> io::Result<()>
where
    S: FnMut(Request<hyper::Body>) -> F + 'static + Clone,
    F: Future<Output = Result<Response<RespBd>, FError>> + 'static,
    FError: std::error::Error + 'static + Send + Sync,
    RespBd: hyper::body::HttpBody + 'static,
    RespBd::Error: std::error::Error + Send + Sync,
    A: Into<SocketAddr>,
{
    let listener = TcpListener::bind(addr.into())?;
    let conn_control = Rc::new(Semaphore::new(max_connections as _));

    HyperExecutor { task_q }.execute(
        async move {
            if max_connections == 0 {
                return Err::<(), GlommioError<()>>(io::Error::from(ConnectionRefused).into());
            }

            loop {
                let stream = listener.accept().await?;
                let addr = stream.local_addr()?;

                let scoped_conn_control = conn_control.clone();
                let captured_service = service.clone();

                HyperExecutor { task_q }.execute(
                    async move {
                        let _semaphore_permit = scoped_conn_control.acquire_permit(1).await?;

                        let http = Http::new()
                            .with_executor(HyperExecutor { task_q })
                            .serve_connection(TokioIO(stream), service_fn(captured_service));

                        let conn_res: io::Result<()> = ConnResult(addr, http.await).into();
                        conn_res
                    }
                    .instrument(error_span!("connection_handler")),
                );
            }
        }
        .instrument(error_span!("tcp_listener")),
    );

    Ok(())
}
