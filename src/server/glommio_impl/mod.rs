pub mod channel {
    use glommio::{channels::local_channel::LocalSender, GlommioError};

    use crate::server::channel::Sender as BaseSender;

    pub struct Sender<T>(LocalSender<T>);

    impl<T> BaseSender<T> for Sender<T> {
        type Error = GlommioError<T>;

        fn try_send(&self, item: T) -> Result<(), Self::Error> {
            self.0.try_send(item)
        }
    }
}

pub mod hyper_compat;
