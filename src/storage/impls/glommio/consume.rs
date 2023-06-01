use super::super::super::AsyncConsume;
use glommio::TaskQueueHandle as TaskQ;
use std::{fmt::Debug, ops::Deref};
use tracing::{Instrument, Level};

pub struct ConsumeHandle<C>
where
    C: AsyncConsume + 'static,
{
    consumable: Option<C>,
    consume_method: ConsumeMethod,
    task_q: Option<TaskQ>,
}

#[derive(Clone, Copy, Debug)]
pub enum ConsumeMethod {
    Remove,
    Close,
}

impl ConsumeMethod {
    pub async fn consume<C>(&self, consumable: Option<C>)
    where
        C: AsyncConsume,
    {
        async move {
            match (&self, consumable) {
                (&ConsumeMethod::Remove, Some(consumable)) => consumable.remove().await,
                (&ConsumeMethod::Close, Some(consumable)) => consumable.close().await,
                _ => Ok(()),
            }
            .map_err(|error| tracing::error!("error during consuming: {:?}", error))
            .ok();
        }
        .instrument(tracing::span!(Level::ERROR, "consume"))
        .await;
    }
}

impl<C> ConsumeHandle<C>
where
    C: AsyncConsume,
{
    pub fn new(consumable: C) -> Self {
        Self {
            consumable: Some(consumable),
            consume_method: ConsumeMethod::Close,
            task_q: None,
        }
    }

    pub fn with_consume_method(consumable: C, consume_method: ConsumeMethod) -> Self {
        Self {
            consumable: Some(consumable),
            consume_method,
            task_q: None,
        }
    }

    pub fn with_consume_method_and_tq(
        consumable: C,
        consume_method: ConsumeMethod,
        task_q: TaskQ,
    ) -> Self {
        Self {
            consumable: Some(consumable),
            consume_method,
            task_q: Some(task_q),
        }
    }
}

impl<C> Deref for ConsumeHandle<C>
where
    C: AsyncConsume,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        // SAFETY: consumable is always a Some(_)
        // before AsyncConsume::drop() is called.
        unsafe { self.consumable.as_ref().unwrap_unchecked() }
    }
}

impl<C> Drop for ConsumeHandle<C>
where
    C: AsyncConsume + 'static,
{
    fn drop(&mut self) {
        let task_q = self.task_q;
        let consumable = self.consumable.take();
        let consume_method = self.consume_method;

        match task_q {
            Some(task_q) => {
                glommio::spawn_local_into(
                    async move {
                        consume_method.consume(consumable).await;
                    },
                    task_q,
                )
                .map(|x| x.detach())
                .ok();
            }
            None => {
                glommio::spawn_local(async move {
                    consume_method.consume(consumable).await;
                })
                .detach();
            }
        }
    }
}

mod mock {
    use super::AsyncConsume;
    use async_trait::async_trait;
    struct Mock;

    impl Mock {
        fn _tautology(&self) -> bool {
            true
        }
    }

    #[async_trait(?Send)]
    impl AsyncConsume for Mock {
        type ConsumeError = std::io::Error;

        async fn remove(self) -> Result<(), Self::ConsumeError> {
            Ok(())
        }

        async fn close(self) -> Result<(), Self::ConsumeError> {
            Ok(())
        }
    }

    #[test]
    fn test_mock_async_consume_drop_with_consume_handle() {
        use super::ConsumeHandle;
        use super::ConsumeMethod;

        glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
            .spawn(move || async move {
                let mock = ConsumeHandle::new(Mock);
                assert!(mock._tautology(), "Unexpected contradiction");

                let mock = ConsumeHandle::with_consume_method(Mock, ConsumeMethod::Remove);
                assert!(mock._tautology(), "Unexpected contradiction");

                let remove_op_task_q = glommio::executor().create_task_queue(
                    glommio::Shares::default(),
                    glommio::Latency::NotImportant,
                    "auto_consume_tq",
                );

                let mock = ConsumeHandle::with_consume_method_and_tq(
                    Mock,
                    ConsumeMethod::Remove,
                    remove_op_task_q,
                );
                assert!(mock._tautology(), "Unexpected contradiction");
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
