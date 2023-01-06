//! Module providing abstractions for commit-log based message queue RPC server.

pub mod channel {
    //! Module providing traits for representing channels. These traits have to be implemented
    //! for each async runtime channel implementation.

    use async_trait::async_trait;
    use std::error::Error;

    /// Trait representing the sending end of a channel.
    pub trait Sender<T> {
        type Error: Error;

        /// Sends the given value over this channel. This method is expected not to block and
        /// return immediately.
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

pub mod single_node;

pub mod partition;
pub mod router;
pub mod worker;

#[cfg(not(tarpaulin_include))]
pub mod tokio_compat;

#[cfg(target_os = "linux")]
pub mod glommio_impl;
