//! Module providing abstractions to safely clean up i.e. consume a partition.

use super::super::super::{
    partition::{Partition, PartitionCreator, PartitionId},
    worker::{TaskError, TaskResult},
};
use glommio::sync::RwLock;
use std::{rc::Rc, time::Duration};

/// [`PartitionRemainder`] represents the various states a partition can be in
/// the process of being consumed. It represents what is left of the partition
/// i.e the remainder.
pub(crate) enum PartitionRemainder<P: Partition> {
    Rc(Rc<RwLock<P>>),
    RwLock(RwLock<P>),
    Partition(P),
    PartitionConsumed,
}

/// Method of consuming a partition.
pub(crate) enum ConsumeMethod {
    /// Closes the partition.
    Close,

    /// Removes the partition.
    Remove,
}

/// Entity for managing the consumption of a [`Partition`].
pub(crate) struct PartitionConsumer<P, PC>
where
    P: Partition,
    PC: PartitionCreator<P>,
{
    partition_remainder: Option<PartitionRemainder<P>>,
    partition_id: PartitionId,
    partition_creator: PC,

    consume_method: ConsumeMethod,

    retries: i32,
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
        retries: i32,
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

    /// Invokes [`Self::with_retries_and_wait_duration`] with `retries = 5` and
    /// `wait_duration = 100ms`
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

    /// Maps the given [`PartitionRemainder`] to a more consumed state. If the given
    /// `partition_remainder` could be sucessfully mapped, an [`Ok`] value with
    /// mapped [`PartitionRemainder`] instance is returned. Otherwise an [`Err`]
    /// value with the corresponding error mapping is returned.
    ///
    /// An [`Ok(None)`] return value denotes that the [`PartitionRemainder`] could
    /// be completely consumed.
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

    /// Consumes the underlying [`PartitionRemainder`]. This method starts with the initial
    /// [`PartitionRemainder`] value and iteratively calls [`Self::remove_remainder`] on it
    /// until an [`Ok(None)`] is obtained.
    ///
    /// This method uses an exponential back-off method in the iteration with the provided
    /// `retries` and initial `wait_duration`. Every time an error occurs, we wait for
    /// `wait_duration` duration. The `wait_duration` is doubled after every error.
    ///
    /// Returns an error containing the `partition_id` if the partition could not be
    /// consumed within the given number of retries. [`Ok(())`] otherwise.
    pub(crate) async fn consume(mut self) -> TaskResult<(), P> {
        let mut partition_remainder = Ok(self.partition_remainder.take());

        loop {
            partition_remainder = match partition_remainder {
                Ok(Some(partition_remainder)) => self.remove_remainder(partition_remainder).await,
                Ok(None) => break,
                Err(_) if self.retries <= 0 => break,
                Err(partition_remainder) => {
                    glommio::timer::sleep(self.wait_duration).await;
                    self.retries -= 1;
                    self.wait_duration *= 2;
                    Ok(Some(partition_remainder))
                }
            };
        }

        partition_remainder
            .map(|_| ())
            .map_err(|_| TaskError::PartitionLost(self.partition_id))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::super::super::partition::single_node::in_memory::{
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
            .unwrap()
            .join()
            .unwrap();
    }
}
