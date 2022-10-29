use std::{
    collections::HashMap,
    error::Error,
    fmt::{Debug, Display},
    rc::Rc,
};

use glommio::{sync::RwLock, TaskQueueHandle};

use super::super::{
    channel::Sender,
    partition::{Partition, PartitionId, Response},
    worker::{Processor as BaseProcessor, Task},
};

pub struct Processor<P: Partition> {
    partitions: HashMap<PartitionId, Rc<RwLock<P>>>,
    task_queue: TaskQueueHandle,
}

#[derive(Debug)]
pub enum ProcessorError<P: Partition, ResponseSender: Sender<Result<Response, P::Error>>> {
    WriteLock,
    ReadLock,

    PartitionOp(P::Error),

    SendError(ResponseSender::Error),
}

impl<P, ResponseSender> Display for ProcessorError<P, ResponseSender>
where
    P: Partition,
    ResponseSender: Sender<Result<Response, P::Error>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessorError::WriteLock => write!(f, "Unable to acquire write lock."),
            ProcessorError::ReadLock => write!(f, "Unable to acquire read lock."),
            ProcessorError::PartitionOp(error) => {
                write!(f, "Error during partition operation: {:?}", error)
            }
            ProcessorError::SendError(error) => {
                write!(f, "Error during sending back response: {:?}", error)
            }
        }
    }
}

impl<P, ResponseSender> Error for ProcessorError<P, ResponseSender>
where
    P: Partition + Debug,
    ResponseSender: Sender<Result<Response, P::Error>> + Debug,
{
}

impl<P, S> BaseProcessor<P, S> for Processor<P>
where
    P: Partition + 'static,
    S: Sender<Result<Response, P::Error>> + 'static,
{
    fn process(&self, task: Task<P::Error, S>) {
        if let Some(partition) = self.partitions.get(&task.partition_id).cloned() {
            let (request, response_sender) = (task.request, task.response_sender);

            let spawn_result = glommio::spawn_local_into(
                async move {
                    let result = if request.idempotent() {
                        partition
                            .read()
                            .await
                            .map_err(|err| {
                                log::error!("Error acquiring read lock: {:?}", err);
                                ProcessorError::<P, S>::ReadLock
                            })?
                            .serve_idempotent(request)
                            .await
                    } else {
                        partition
                            .write()
                            .await
                            .map_err(|err| {
                                log::error!("Error acquiring write lock: {:?}", err);
                                ProcessorError::<P, S>::WriteLock
                            })?
                            .serve(request)
                            .await
                    };

                    response_sender.try_send(result).map_err(|err| {
                        log::error!("Error sending back response: {:?}", err);
                        ProcessorError::SendError(err)
                    })?;

                    Ok::<(), ProcessorError<P, S>>(())
                },
                self.task_queue,
            );

            match spawn_result {
                Ok(task) => {
                    task.detach();
                }
                Err(error) => {
                    log::error!("Error spawning task: {:?}", error);
                }
            };
        }
    }
}
