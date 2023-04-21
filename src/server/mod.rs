//! Module providing abstractions for commit-log based message queue RPC server.

/// Trait for abstracting a RPC server implementation.
pub trait Server<Service> {
    type Result;

    /// Serves RPC requests using the provided `Service` instance.
    ///
    /// ## Implementation note:
    /// This method should setup a mechanism for scheduling
    /// request-serving tasks and return without blocking.
    fn serve(&self, service: Service) -> Self::Result;
}

pub mod channel {
    //! Module providing traits for representing channels. These traits have to be implemented
    //! for each async runtime channel implementation.

    use async_trait::async_trait;
    use std::error::Error;

    /// Trait representing the sending end of a channel.
    #[async_trait(?Send)]
    pub trait Sender<T> {
        type Error: Error;

        /// Sends the given valye of this channel. Asynchronously waits if there is no space
        /// to write an item to the channel.
        ///
        /// ## Errors
        /// Possible error situations could include:
        /// - unable to send item
        /// - receiving end dropped
        async fn send(&self, item: T) -> Result<(), Self::Error>;

        /// Sends the given value over this channel. This method is expected not to block
        /// and return immediately.
        ///
        /// ## Errors
        /// Possible error situations could include:
        /// - unable to send item
        /// - receiving end dropped
        fn try_send(&self, item: T) -> Result<(), Self::Error>;
    }

    /// Trait representing the receiving end of a channel.
    #[async_trait(?Send)]
    pub trait Receiver<T> {
        /// Asynchronously receives the next value in the channel. A None value indicates that
        /// no items are left to be received.
        async fn recv(&self) -> Option<T>;
    }
}

pub mod common;
pub mod impls;
