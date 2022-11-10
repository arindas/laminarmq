use super::{
    super::{
        channel::Sender,
        partition::{Partition, PartitionCreator, PartitionId},
        worker::{Processor as BaseProcessor, Task, TaskError, TaskResult},
        Request, Response,
    },
    worker::ResponseSender,
};
use glommio::{sync::RwLock, TaskQueueHandle};
use std::{collections::HashMap, rc::Rc};

pub struct Processor<P, PC>
where
    P: Partition,
    PC: PartitionCreator<P>,
{
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    task_queue: TaskQueueHandle,

    partition_creator: PC,
}

impl<P, PC> Processor<P, PC>
where
    P: Partition,
    PC: PartitionCreator<P>,
{
    pub fn new(task_queue: TaskQueueHandle, partition_creator: PC) -> Self {
        Self {
            partitions: Rc::new(RwLock::new(HashMap::new())),
            task_queue,
            partition_creator,
        }
    }
}

async fn handle_idempotent_requests<P: Partition>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
    request: Request,
) -> TaskResult<P> {
    partitions
        .read()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .get(&partition_id)
        .ok_or(TaskError::PartitionNotFound(partition_id))?
        .read()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .serve_idempotent(request)
        .await
        .map_err(TaskError::PartitionError)
}

async fn handle_requests<P: Partition>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
    request: Request,
) -> TaskResult<P> {
    partitions
        .read()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .get(&partition_id)
        .ok_or(TaskError::PartitionNotFound(partition_id))?
        .write()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .serve(request)
        .await
        .map_err(TaskError::PartitionError)
}

async fn handle_create_partition<P: Partition, PC: PartitionCreator<P>>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
    partition_creator: PC,
) -> TaskResult<P> {
    if partitions
        .read()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .get(&partition_id)
        .is_none()
    {
        let partition = partition_creator
            .new_partition(&partition_id)
            .await
            .map_err(TaskError::PartitionError)?;

        partitions
            .write()
            .await
            .map_err(|_| TaskError::LockAcqFailed)?
            .insert(partition_id, Rc::new(RwLock::new(partition)));
    }

    Ok(Response::PartitionCreated)
}

mod partition_remover {
    use super::super::super::{
        partition::{Partition, PartitionCreator, PartitionId},
        worker::{TaskError, TaskResult},
        Response,
    };
    use glommio::sync::RwLock;
    use std::{rc::Rc, time::Duration};

    pub(crate) enum PartitionRemainder<P: Partition> {
        Rc(Rc<RwLock<P>>),
        RwLock(RwLock<P>),
        Partition(P),
        PartitionConsumed,
    }

    pub(crate) struct PartitionRemover<P, PC>
    where
        P: Partition,
        PC: PartitionCreator<P>,
    {
        partition_remainder: Option<PartitionRemainder<P>>,
        partition_id: PartitionId,
        partition_creator: PC,

        retries: u32,
        wait_duration: Duration,
    }

    impl<P, PC> PartitionRemover<P, PC>
    where
        P: Partition,
        PC: PartitionCreator<P>,
    {
        pub(crate) fn with_retries_and_wait_duration(
            partition_remainder: PartitionRemainder<P>,
            partition_id: PartitionId,
            partition_creator: PC,
            retries: u32,
            wait_duration: Duration,
        ) -> Self {
            Self {
                partition_remainder: Some(partition_remainder),
                partition_id,
                partition_creator,
                retries,
                wait_duration,
            }
        }

        pub(crate) fn new(
            partition_remainder: PartitionRemainder<P>,
            partition_id: PartitionId,
            partition_creator: PC,
        ) -> Self {
            Self::with_retries_and_wait_duration(
                partition_remainder,
                partition_id,
                partition_creator,
                5,
                Duration::from_millis(100),
            )
        }

        async fn remove_remainder(
            &self,
            partition_remainder: PartitionRemainder<P>,
        ) -> Result<Option<PartitionRemainder<P>>, PartitionRemainder<P>> {
            match partition_remainder {
                PartitionRemainder::Rc(rc) => Rc::try_unwrap(rc)
                    .map(|x| Some(PartitionRemainder::RwLock(x)))
                    .map_err(PartitionRemainder::Rc),
                PartitionRemainder::RwLock(rwlock) => rwlock
                    .into_inner()
                    .map(|x| Some(PartitionRemainder::Partition(x)))
                    .map_err(|_| PartitionRemainder::PartitionConsumed),
                PartitionRemainder::Partition(partition) => partition
                    .remove()
                    .await
                    .map(|_| None)
                    .map_err(|_| PartitionRemainder::PartitionConsumed),
                PartitionRemainder::PartitionConsumed => {
                    if let Ok(partition) = self
                        .partition_creator
                        .new_partition(&self.partition_id)
                        .await
                    {
                        Ok(Some(PartitionRemainder::Partition(partition)))
                    } else {
                        Err(PartitionRemainder::PartitionConsumed)
                    }
                }
            }
        }

        pub(crate) async fn remove(mut self) -> TaskResult<P> {
            // Initial value of self.partition_remainder must be a Some
            let mut last_iter_ok = false;

            while let Some(remainder) = self.partition_remainder.take() {
                self.partition_remainder = match self.remove_remainder(remainder).await {
                    Err(partition_remainder) => {
                        last_iter_ok = false;

                        if self.retries.checked_sub(1).is_none() {
                            None
                        } else {
                            glommio::timer::sleep(self.wait_duration).await;
                            self.wait_duration *= 2;
                            Some(partition_remainder)
                        }
                    }
                    Ok(val) => {
                        last_iter_ok = true;
                        val
                    }
                }
            }

            if last_iter_ok {
                Ok(Response::PartitionRemoved)
            } else {
                Err(TaskError::PartitionLost(self.partition_id))
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{
            super::super::super::partition::in_memory::{
                Partition as InMemPartition, PartitionCreator as InMemPartitionCreator,
            },
            *,
        };
        use glommio::{LocalExecutorBuilder, Placement};
        use std::collections::HashMap;

        #[test]
        fn test_partition_remover() {
            LocalExecutorBuilder::new(Placement::Unbound)
                .spawn(|| async move {
                    let partition_container = Rc::new(RwLock::new(HashMap::<
                        PartitionId,
                        Rc<RwLock<InMemPartition>>,
                    >::new()));

                    let partition_id = PartitionId {
                        topic: "some_topic".to_string(),
                        partition_number: 0,
                    };

                    partition_container.write().await.unwrap().insert(
                        partition_id.clone(),
                        Rc::new(RwLock::new(InMemPartition::new())),
                    );

                    let partition = partition_container
                        .write()
                        .await
                        .unwrap()
                        .remove(&partition_id)
                        .unwrap();

                    PartitionRemover::new(
                        PartitionRemainder::Rc(partition),
                        partition_id,
                        InMemPartitionCreator,
                    )
                    .remove()
                    .await
                    .unwrap();
                })
                .unwrap();
        }
    }
}

async fn handle_remove_partition<P: Partition, PC: PartitionCreator<P>>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
    partition_creator: PC,
) -> TaskResult<P> {
    let partition = partitions
        .write()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .remove(&partition_id)
        .ok_or_else(|| TaskError::PartitionNotFound(partition_id.clone()))?;

    use partition_remover::{PartitionRemainder, PartitionRemover};

    PartitionRemover::new(
        PartitionRemainder::Rc(partition),
        partition_id,
        partition_creator,
    )
    .remove()
    .await
}

impl<P, PC> BaseProcessor<P, ResponseSender<P>> for Processor<P, PC>
where
    P: Partition + 'static,
    PC: PartitionCreator<P> + Clone + 'static,
{
    fn process(&self, task: Task<P, ResponseSender<P>>) {
        let (partitions, partition_creator) =
            (self.partitions.clone(), self.partition_creator.clone());

        let spawn_result = glommio::spawn_local_into(
            async move {
                let task_result = match &task.request {
                    Request::Read { offset: _ }
                    | Request::LowestOffset
                    | Request::HighestOffset => {
                        handle_idempotent_requests(partitions, task.partition_id, task.request)
                            .await
                    }

                    Request::RemoveExpired { expiry_duration: _ }
                    | Request::Append { record_bytes: _ } => {
                        handle_requests(partitions, task.partition_id, task.request).await
                    }

                    Request::CreatePartition => {
                        handle_create_partition(partitions, task.partition_id, partition_creator)
                            .await
                    }

                    Request::RemovePartition => {
                        handle_remove_partition(partitions, task.partition_id, partition_creator)
                            .await
                    }
                };

                if let Err(send_error) = task.response_sender.try_send(task_result) {
                    log::error!("Unable to send result back: {:?}", send_error);
                }
            },
            self.task_queue,
        );

        match spawn_result {
            Ok(task) => {
                task.detach();
            }
            Err(spawn_error) => {
                log::error!("Error detaching spawned task: {:?}", spawn_error);
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::super::worker::new_task;
    use super::Processor;
    use crate::server::{
        channel::Receiver,
        partition::{
            in_memory::{Partition, PartitionCreator},
            PartitionId,
        },
        worker::Processor as BaseProcessor,
        Request,
    };
    use glommio::{executor, Latency, LocalExecutorBuilder, Placement, Shares};

    #[test]
    fn test_processor() {
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let task_queue = executor().create_task_queue(
                    Shares::default(),
                    Latency::NotImportant,
                    "processor_tq",
                );

                let processor = Processor::new(task_queue, PartitionCreator);

                let partition_id_1 = PartitionId {
                    topic: "topic_1".to_string(),
                    partition_number: 1,
                };
                let partition_id_2 = PartitionId {
                    topic: "topic_2".to_string(),
                    partition_number: 2,
                };

                let (create_partition_task_1, recv_1) =
                    new_task::<Partition>(partition_id_1, Request::CreatePartition);

                let (create_partition_task_2, recv_2) =
                    new_task::<Partition>(partition_id_2, Request::CreatePartition);

                processor.process(create_partition_task_1);

                processor.process(create_partition_task_2);

                recv_1.recv().await.unwrap().unwrap();

                recv_2.recv().await.unwrap().unwrap();
            })
            .unwrap();
    }
}
