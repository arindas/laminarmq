use std::marker::PhantomData;

use super::{
    channel::{Receiver, Sender},
    partition::{Partition, PartitionId, Request, Response},
};

pub struct Task<E, S: Sender<Result<Response, E>>> {
    pub partition_id: PartitionId,
    pub request: Request,

    pub response_sender: S,

    _phantom_data: PhantomData<E>,
}

pub trait Processor<P, S>
where
    P: Partition,
    S: Sender<Result<Response, P::Error>>,
{
    fn process(&self, task: Task<P::Error, S>);
}

pub struct Worker<P, S, TaskProcessor>
where
    P: Partition,
    S: Sender<Result<Response, P::Error>>,
    TaskProcessor: Processor<P, S>,
{
    processor: TaskProcessor,

    _phantom_data: PhantomData<(P, S)>,
}

impl<P, S, TaskProcessor> Worker<P, S, TaskProcessor>
where
    P: Partition,
    S: Sender<Result<Response, P::Error>>,
    TaskProcessor: Processor<P, S>,
{
    pub fn new(processor: TaskProcessor) -> Self {
        Self {
            processor,
            _phantom_data: PhantomData,
        }
    }

    pub async fn process_tasks<R>(&self, task_receiver: R)
    where
        R: Receiver<Task<P::Error, S>>,
    {
        while let Some(task) = task_receiver.recv().await {
            self.processor.process(task);
        }
    }
}
