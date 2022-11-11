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

    pub fn new_bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
        let (send, recv) = glommio::channels::local_channel::new_bounded(size);
        (Sender(send), Receiver(recv))
    }

    pub fn new_unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let (send, recv) = glommio::channels::local_channel::new_unbounded();
        (Sender(send), Receiver(recv))
    }
}

pub mod worker {
    use super::{
        super::{
            partition::{Partition, PartitionId},
            worker::{Task, TaskResult},
            Request,
        },
        channel,
    };

    pub type ResponseReceiver<P> = channel::Receiver<TaskResult<P>>;
    pub type ResponseSender<P> = channel::Sender<TaskResult<P>>;

    pub fn new_task<P: Partition>(
        partition_id: PartitionId,
        request: Request,
    ) -> (Task<P, ResponseSender<P>>, ResponseReceiver<P>) {
        let (response_sender, response_receiver) = channel::new_bounded(1);

        (
            Task::new(partition_id, request, response_sender),
            response_receiver,
        )
    }
}

pub mod hyper_compat;
pub mod processor;
