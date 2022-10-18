use futures_lite::Future;
use glommio::{enclose, net::TcpListener, sync::Semaphore};
use hyper::service::service_fn;
use hyper::{server::conn::Http, Body, Request, Response};
use log::error;
use std::net::SocketAddr;
use std::{io, rc::Rc};

use crate::server::tokio_compat::TokioIO;

#[derive(Clone)]
struct HyperExecutor;

impl<Fut> hyper::rt::Executor<Fut> for HyperExecutor
where
    Fut: Future + 'static,
    Fut::Output: 'static,
{
    fn execute(&self, fut: Fut) {
        glommio::spawn_local(fut).detach();
    }
}

pub async fn serve_http<S, F, R, A>(addr: A, service: S, max_connections: usize) -> io::Result<()>
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
                glommio::spawn_local(enclose! {(conn_control) async move {
                    let _permit = conn_control.acquire_permit(1).await;

                    if let Err(x) = Http::new().with_executor(HyperExecutor)
                        .serve_connection(TokioIO(stream), service_fn(service)).await {
                        if !x.is_incomplete_message() {
                            error!("Stream from {:?} failed with error {:?}", addr, x);
                        }
                    }
                }})
                .detach();
            }
        }
    }
}
