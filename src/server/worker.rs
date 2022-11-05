use std::marker::PhantomData;

use super::{
    channel::{Receiver, Sender},
    partition::{Partition, PartitionId},
    Request, Response,
};

pub enum TaskError<P: Partition> {
    PartitionError(P::Error),
    PartitionNotFound(PartitionId),
    PartitionInUse(PartitionId),
    PartitionLost(PartitionId),
    LockAcqFailed,
}

pub type TaskResult<P> = Result<Response, TaskError<P>>;

pub struct Task<P: Partition, S: Sender<TaskResult<P>>> {
    pub partition_id: PartitionId,
    pub request: Request,

    pub response_sender: S,

    _phantom_data: PhantomData<P>,
}

pub trait Processor<P, S>
where
    P: Partition,
    S: Sender<TaskResult<P>>,
{
    fn process(&self, task: Task<P, S>);
}

pub struct Worker<P, S, Proc>
where
    P: Partition,
    S: Sender<TaskResult<P>>,
    Proc: Processor<P, S>,
{
    processor: Proc,

    _phantom_data: PhantomData<(P, S)>,
}

impl<P, S, Proc> Worker<P, S, Proc>
where
    P: Partition,
    S: Sender<TaskResult<P>>,
    Proc: Processor<P, S>,
{
    pub fn new(processor: Proc) -> Self {
        Self {
            processor,
            _phantom_data: PhantomData,
        }
    }

    pub async fn process_tasks<R>(&self, task_receiver: R)
    where
        R: Receiver<Task<P, S>>,
    {
        while let Some(task) = task_receiver.recv().await {
            self.processor.process(task);
        }
    }
}
