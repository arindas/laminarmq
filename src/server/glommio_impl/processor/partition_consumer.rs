use super::super::super::{
    partition::{Partition, PartitionCreator, PartitionId},
    worker::{TaskError, TaskResult},
};
use glommio::sync::RwLock;
use std::{rc::Rc, time::Duration};

pub(crate) enum PartitionRemainder<P: Partition> {
    Rc(Rc<RwLock<P>>),
    RwLock(RwLock<P>),
    Partition(P),
    PartitionConsumed,
}

pub(crate) enum ConsumeMethod {
    Close,
    Remove,
}

pub(crate) struct PartitionConsumer<P, PC>
where
    P: Partition,
    PC: PartitionCreator<P>,
{
    partition_remainder: Option<PartitionRemainder<P>>,
    partition_id: PartitionId,
    partition_creator: PC,

    consume_method: ConsumeMethod,

    retries: u32,
    wait_duration: Duration,
}

impl<P, PC> PartitionConsumer<P, PC>
where
    P: Partition,
    PC: PartitionCreator<P>,
{
    pub(crate) fn with_retries_and_wait_duration(
        partition_remainder: PartitionRemainder<P>,
        partition_id: PartitionId,
        partition_creator: PC,
        consume_method: ConsumeMethod,
        retries: u32,
        wait_duration: Duration,
    ) -> Self {
        Self {
            partition_remainder: Some(partition_remainder),
            partition_id,
            partition_creator,
            consume_method,
            retries,
            wait_duration,
        }
    }

    pub(crate) fn new(
        partition_remainder: PartitionRemainder<P>,
        partition_id: PartitionId,
        partition_creator: PC,
        consume_method: ConsumeMethod,
    ) -> Self {
        Self::with_retries_and_wait_duration(
            partition_remainder,
            partition_id,
            partition_creator,
            consume_method,
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
            PartitionRemainder::Partition(partition) => match &self.consume_method {
                ConsumeMethod::Remove => partition.remove(),
                ConsumeMethod::Close => partition.close(),
            }
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

    pub(crate) async fn consume(mut self) -> TaskResult<(), P> {
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
            Ok(())
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
    fn test_partition_consumer() {
        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async move {
                let partition_container = Rc::new(RwLock::new(HashMap::<
                    PartitionId,
                    Rc<RwLock<InMemPartition>>,
                >::new()));

                let partition_id = PartitionId {
                    topic: "some_topic".into(),
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

                PartitionConsumer::new(
                    PartitionRemainder::Rc(partition),
                    partition_id,
                    InMemPartitionCreator,
                    ConsumeMethod::Remove,
                )
                .consume()
                .await
                .unwrap();
            })
            .unwrap();
    }
}
