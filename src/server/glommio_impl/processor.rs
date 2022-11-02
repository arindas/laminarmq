use super::super::{
    channel::Sender,
    partition::{Partition, PartitionCreator, PartitionId, Request, Response},
    worker::{Processor as BaseProcessor, Task, TaskError, TaskResult},
};
use glommio::{sync::RwLock, TaskQueueHandle};
use std::{collections::HashMap, rc::Rc};

pub struct Processor<P: Partition, PC: PartitionCreator<P>> {
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    task_queue: TaskQueueHandle,

    partition_creator: PC,
}

type ResponseSender<P> = super::channel::Sender<TaskResult<P>>;

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
        .ok_or_else(|| TaskError::PartitionNotFound(partition_id))?
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
        .ok_or_else(|| TaskError::PartitionNotFound(partition_id))?
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

async fn handle_remove_partition<P: Partition>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
) -> TaskResult<P> {
    let partition = partitions
        .write()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .remove(&partition_id)
        .ok_or_else(|| TaskError::PartitionNotFound(partition_id.clone()))?;

    let partition =
        Rc::try_unwrap(partition).map_err(|_| TaskError::PartitionInUse(partition_id.clone()))?;

    let partition = partition
        .into_inner()
        .map_err(|_| TaskError::PartitionInUse(partition_id))?;

    partition
        .remove()
        .await
        .map_err(TaskError::PartitionError)?;

    Ok(Response::PartitionRemoved)
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
                        handle_remove_partition(partitions, task.partition_id).await
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
