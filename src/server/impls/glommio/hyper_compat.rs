//! Module providing utilities for setting up a [`hyper`] server using [`glommio`].

use crate::common::tokio_compat::TokioIO;
use crate::server::Server;
use futures_lite::Future;
use glommio::{net::TcpListener, sync::Semaphore, GlommioError, TaskQueueHandle as TaskQ};
use hyper::{rt::Executor, server::conn::Http, Request, Response};
use std::{
    io::{
        self,
        ErrorKind::{ConnectionRefused, ConnectionReset, Other},
    },
    net::SocketAddr,
    num::NonZeroUsize,
    rc::Rc,
};
use tower_service::Service;
use tracing::{debug, error, instrument, trace_span, Instrument};

/// [`hyper::rt::Executor`] implementation that executes futures by spawning them on a
/// [`glommio::TaskQueueHandle`].
#[derive(Clone)]
struct HyperExecutor {
    task_q: TaskQ,
}

impl HyperExecutor {
    fn spawn<F>(&self, f: F) -> Result<glommio::Task<F::Output>, ()>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        glommio::spawn_local_into(f, self.task_q).map_err(|spawn_error| {
            error!(
                "spawn_local_into -> {:?} ==> {:?}",
                self.task_q, spawn_error
            );
        })
    }
}

impl<F> Executor<F> for HyperExecutor
where
    F: Future + 'static,
    F::Output: 'static,
{
    #[instrument(skip(self, f))]
    fn execute(&self, f: F) {
        self.spawn(f).map(|task| task.detach()).ok();
    }
}

/// Wrapper for Hyper HTTP connection result.
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

/// Mechanism of connection control in [`HyperServer`]
#[derive(Debug, Clone, Copy)]
pub enum ConnControl {
    /// Non blocking connection control: Refuses all connections past the threshold limit.
    NonBlocking,

    /// Blocking connection control: Blocks on new connections when the maximum connections
    /// control limit is reached. The blocked connections are resumed and serviced when
    /// the total number of connections come down from the threshold limit.
    Blocking,
}

/// Serves HTTP requests at the given address using the given parameters. All request
/// handling futures are spawned on the given [`TaskQ`].
#[derive(Debug)]
pub struct HyperServer {
    pub max_connections: NonZeroUsize,
    pub conn_control: ConnControl,
    pub task_q: TaskQ,
}

impl HyperServer {
    /// Creates a new [`HyperServer`] instance with the given maximum connections limit, connection
    /// control mechanism, and the task queue to schedule connection handler tasks on.
    pub fn with_max_connections_conn_control_and_task_q(
        max_connections: NonZeroUsize,
        conn_control: ConnControl,
        task_q: TaskQ,
    ) -> Self {
        Self {
            max_connections,
            conn_control,
            task_q,
        }
    }

    /// Creates a new [`HyperServer`] with the given connection control mechanism and task queue
    /// with the default maximum connections limit of `1024`.
    pub fn with_conn_control_and_task_q(conn_control: ConnControl, task_q: TaskQ) -> Self {
        Self::with_max_connections_conn_control_and_task_q(
            unsafe { NonZeroUsize::new_unchecked(1024) }, // SAFETY: 1024 > 0
            conn_control,
            task_q,
        )
    }

    /// Creates a new [`HyperServer`] with the given task queue, default maximum connections limit
    /// of `1024` and with the default connection control mechanism: [`ConnControl::Blocking`].
    pub fn with_task_q(task_q: TaskQ) -> Self {
        Self::with_conn_control_and_task_q(ConnControl::Blocking, task_q)
    }
}

impl<S, RespBd, Error> Server<S> for HyperServer
where
    S: Service<Request<hyper::Body>, Response = Response<RespBd>, Error = Error> + Clone + 'static,
    Error: std::error::Error + 'static + Send + Sync,
    RespBd: hyper::body::HttpBody + 'static,
    RespBd::Error: std::error::Error + Send + Sync,
{
    /// The socket address that this server listens on, and the connection listener task handle.
    type Result = io::Result<(SocketAddr, glommio::Task<Result<(), GlommioError<()>>>)>;

    #[instrument(skip(service))]
    fn serve(&self, service: S) -> Self::Result {
        let (max_connections, task_q, conn_control) =
            (self.max_connections.get(), self.task_q, self.conn_control);

        let listener = TcpListener::bind::<SocketAddr>(([0, 0, 0, 0], 0).into())?;
        let listener_local_addr = listener.local_addr()?;
        debug!("Binded on: {:?}", listener_local_addr);

        let conn_semaphore = Rc::new(Semaphore::new(max_connections as _));

        let spawn_result = HyperExecutor { task_q }.spawn(
            async move {
                debug!("Start listening for client connections.");

                loop {
                    let stream = listener.accept().await?;
                    let addr = stream.peer_addr()?;

                    debug!("Accepted a connection from: {addr:?}");

                    let conn_semaphore = conn_semaphore.clone();
                    let service = service.clone();

                    HyperExecutor { task_q }.execute(
                        async move {
                            let _semaphore_permit = match conn_control {
                                ConnControl::Blocking => conn_semaphore.acquire_permit(1).await,
                                _ => conn_semaphore.try_acquire_permit(1),
                            }
                            .map_err(|err| match err {
                                GlommioError::WouldBlock(_) => "Max connections limit crossed!",
                                _ => "Failed to acquire connection permit!",
                            })
                            .map_err(|err| error!(err))
                            .map_err(|_| io::Error::from(ConnectionRefused))?;

                            debug!("Acquired connection permit, begin serving connection.");

                            let http_connection = Http::new()
                                .with_executor(HyperExecutor { task_q })
                                .serve_connection(TokioIO(stream), service);

                            debug!("Obtained connection handle.");

                            let conn_res = http_connection.await;

                            debug!("Done serving connection.");

                            io::Result::<()>::from(ConnResult(addr, conn_res))
                        }
                        .instrument(trace_span!("connection_handler")),
                    );
                }
            }
            .instrument(trace_span!("tcp_listener")),
        );

        spawn_result
            .map(|spawned_task_handle| (listener_local_addr, spawned_task_handle))
            .map_err(|_| io::Error::new(Other, "error spawning tcp_listener"))
    }
}
