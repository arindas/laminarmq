pub mod channel {
    use std::error::Error;

    pub trait Sender<T> {
        type Error: Error;

        fn try_send(&self, item: T) -> Result<(), Self::Error>;
    }
}

pub mod partition;
pub mod tokio_compat;

#[cfg(target_os = "linux")]
pub mod glommio_impl;
