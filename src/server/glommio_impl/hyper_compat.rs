//! Module providing utilities for setting up a [`hyper`] server using [`glommio`].

use crate::server::{tokio_compat::TokioIO, Server};
use futures_lite::Future;
use glommio::{net::TcpListener, sync::Semaphore, GlommioError, TaskQueueHandle as TaskQ};
use hyper::{rt::Executor, server::conn::Http, Request, Response};
use std::{
    io::{
        self,
        ErrorKind::{ConnectionRefused, ConnectionReset},
    },
    net::SocketAddr,
    rc::Rc,
};
use tower_service::Service;
use tracing::{error, instrument, trace, trace_span, Instrument};

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
#[derive(Debug)]
pub struct HyperServer {
    pub max_connections: usize,
    pub task_q: TaskQ,
}

impl<S, RespBd, Error> Server<S> for HyperServer
where
    S: Service<Request<hyper::Body>, Response = Response<RespBd>, Error = Error> + Clone + 'static,
    Error: std::error::Error + 'static + Send + Sync,
    RespBd: hyper::body::HttpBody + 'static,
    RespBd::Error: std::error::Error + Send + Sync,
{
    type Result = io::Result<()>;

    #[instrument(skip(addr, service))]
    fn serve_http<A>(&self, addr: A, service: S) -> Self::Result
    where
        A: Into<SocketAddr>,
    {
        let max_connections = self.max_connections;
        let task_q = self.task_q;

        let addr: SocketAddr = addr.into();

        trace!("Attempting bind() on: {:?}", addr);
        let listener = TcpListener::bind(addr)?;

        let conn_control = Rc::new(Semaphore::new(max_connections as _));

        HyperExecutor { task_q }.execute(
            async move {
                if max_connections == 0 {
                    error!("max_connections = 0. Refusing connections.");
                    return Err::<(), GlommioError<()>>(io::Error::from(ConnectionRefused).into());
                }

                trace!("Start listening for client connections.");

                loop {
                    let stream = listener.accept().await?;
                    let addr = stream.local_addr()?;

                    trace!("Accepted a connection");

                    let scoped_conn_control = conn_control.clone();
                    let captured_service = service.clone();

                    HyperExecutor { task_q }.execute(
                        async move {
                            let _semaphore_permit = scoped_conn_control.acquire_permit(1).await?;

                            trace!("Acquired permit on conn_control, begin serving connection");

                            let http = Http::new()
                                .with_executor(HyperExecutor { task_q })
                                .serve_connection(TokioIO(stream), captured_service);

                            trace!("Obtained connection handle.");

                            let conn_res: io::Result<()> = ConnResult(addr, http.await).into();

                            trace!("Done serving connection");
                            conn_res
                        }
                        .instrument(trace_span!("connection_handler")),
                    );
                }
            }
            .instrument(trace_span!("tcp_listener")),
        );

        Ok(())
    }
}
