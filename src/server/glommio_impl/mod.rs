pub mod channel {
    use crate::server::channel::{Receiver as BaseReceiver, Sender as BaseSender};
    use async_trait::async_trait;
    use glommio::{
        channels::local_channel::{LocalReceiver, LocalSender},
        GlommioError,
    };

    pub struct Sender<T>(LocalSender<T>);

    impl<T> BaseSender<T> for Sender<T> {
        type Error = GlommioError<T>;

        fn try_send(&self, item: T) -> Result<(), Self::Error> {
            self.0.try_send(item)
        }
    }

    pub struct Receiver<T>(LocalReceiver<T>);

    #[async_trait(?Send)]
    impl<T> BaseReceiver<T> for Receiver<T> {
        async fn recv(&self) -> Option<T> {
            self.0.recv().await
        }
    }
}

pub mod hyper_compat;
