pub mod channel {
    use async_trait::async_trait;
    use std::error::Error;

    pub trait Sender<T> {
        type Error: Error;

        fn try_send(&self, item: T) -> Result<(), Self::Error>;
    }

    #[async_trait(?Send)]
    pub trait Receiver<T> {
        async fn recv(&self) -> Option<T>;
    }
}

pub mod partition;
pub mod worker;

pub mod tokio_compat;

#[cfg(target_os = "linux")]
pub mod glommio_impl;
