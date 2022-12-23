//! Module providing a compatiability layer between [`tokio`] and [`futures_lite`] IO.

use futures_lite::{AsyncRead, AsyncWrite};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::ReadBuf;

/// Wraps a `futures_lite::{AsyncRead, AsyncWrite}` IO comunication channel with a
/// `tokio::io::{AsyncRead, AsyncWrite}` implementation.
pub struct TokioIO<T>(pub T)
where
    T: AsyncRead + AsyncWrite + Unpin;

impl<T> tokio::io::AsyncWrite for TokioIO<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
}

impl<T> tokio::io::AsyncRead for TokioIO<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0)
            .poll_read(cx, buf.initialize_unfilled())
            .map(|n| {
                if let Ok(n) = n {
                    buf.advance(n);
                }

                Ok(())
            })
    }
}
