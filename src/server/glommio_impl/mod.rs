//! Module providing implementations of traits under [`crate::server`] specific to the [`glommio`]
//! runtime.

pub mod channel {
    //! Module providing [`glommio`] specific channel implementation.
    use crate::server::channel::{Receiver as BaseReceiver, Sender as BaseSender};
    use async_trait::async_trait;
    use glommio::{
        channels::local_channel::{LocalReceiver, LocalSender},
        GlommioError,
    };

    /// Wraps a [`LocalSender`]
    pub struct Sender<T>(LocalSender<T>);

    impl<T> BaseSender<T> for Sender<T> {
        type Error = GlommioError<T>;

        fn try_send(&self, item: T) -> Result<(), Self::Error> {
            self.0.try_send(item)
        }
    }

    /// Wraps a [`LocalReceiver`]
    pub struct Receiver<T>(LocalReceiver<T>);

    #[async_trait(?Send)]
    impl<T> BaseReceiver<T> for Receiver<T> {
        async fn recv(&self) -> Option<T> {
            self.0.recv().await
        }
    }

    /// Creates a new bounded channel using [`glommio::channels::local_channel::new_bounded`].
    pub fn new_bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
        let (send, recv) = glommio::channels::local_channel::new_bounded(size);
        (Sender(send), Receiver(recv))
    }

    /// Creates a new unbounded channel using [`glommio::channels::local_channel::new_unbounded`].
    pub fn new_unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let (send, recv) = glommio::channels::local_channel::new_unbounded();
        (Sender(send), Receiver(recv))
    }
}

pub mod worker {
    //! Module providing type specialization specific to the [`glommio`] runtime.
    use super::{
        super::{
            partition::Partition,
            worker::{Task, TaskResult},
        },
        channel,
    };

    /// Type alias for Response channel receiving end.
    pub type ResponseReceiver<Response, P> = channel::Receiver<TaskResult<Response, P>>;
    /// Type alias for Response channel sending end.
    pub type ResponseSender<Response, P> = channel::Sender<TaskResult<Response, P>>;

    /// [`Task`] specialization for [`glommio`] with [`ResponseSender`].
    pub type GlommioTask<P, Request, Response> =
        Task<P, Request, Response, ResponseSender<Response, P>>;

    /// Creates a new [`GlommioTask`] for servicing the given `Request`.
    ///
    /// ## Returns:
    /// - [`GlommioTask`]: [`Task`] to be executed by processor.
    /// - [`ResponseReceiver`]: receiving end of the response channel where the response
    /// will be received after the request is processed.
    pub fn new_task<P: Partition, Request, Response>(
        request: Request,
    ) -> (
        GlommioTask<P, Request, Response>,
        ResponseReceiver<Response, P>,
    ) {
        let (response_sender, response_receiver) = channel::new_bounded(1);

        (Task::new(request, response_sender), response_receiver)
    }
}

pub mod hyper_compat;
pub mod partition;
pub mod processor;
