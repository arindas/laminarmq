use super::super::super::AsyncConsume;
use glommio::TaskQueueHandle as TaskQ;
use std::{fmt::Debug, ops::Deref};
use tracing::{Instrument, Level};

pub struct ConsumeHandle<C: AsyncConsume + 'static> {
    consumable: Option<C>,
    method: ConsumeMethod,
    task_q: Option<TaskQ>,
}

#[derive(Clone, Copy, Debug)]
pub enum ConsumeMethod {
    Remove,
    Close,
}

impl<C: AsyncConsume> ConsumeHandle<C> {
    pub async fn consume(consumable: Option<C>, method: ConsumeMethod) {
        async move {
            let result = if let Some(consumable) = consumable {
                match method {
                    ConsumeMethod::Remove => consumable.remove().await,
                    ConsumeMethod::Close => consumable.close().await,
                }
            } else {
                Ok(())
            };

            if let Err(error) = result {
                tracing::error!("error during consuming: {:?}", error);
            }
        }
        .instrument(tracing::span!(Level::ERROR, "consume"))
        .await;
    }

    pub fn new(consumable: C) -> Self {
        Self {
            consumable: Some(consumable),
            method: ConsumeMethod::Close,
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
            method: consume_method,
            task_q: Some(task_q),
        }
    }
}

impl<C: AsyncConsume> Deref for ConsumeHandle<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        unsafe { self.consumable.as_ref().unwrap_unchecked() }
    }
}

impl<C: AsyncConsume + 'static> Drop for ConsumeHandle<C> {
    fn drop(&mut self) {
        let task_q = self.task_q;
        let consumable = self.consumable.take();
        let method = self.method;

        match task_q {
            Some(task_q) => {
                glommio::spawn_local_into(
                    async move {
                        ConsumeHandle::consume(consumable, method).await;
                    },
                    task_q,
                )
                .map(|x| x.detach())
                .ok();
            }
            None => {
                glommio::spawn_local(async move {
                    ConsumeHandle::consume(consumable, method).await;
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
    fn test_mock() {
        use super::ConsumeHandle;
        use super::ConsumeMethod;

        glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
            .spawn(move || async move {
                let mock = ConsumeHandle::new(Mock);
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
