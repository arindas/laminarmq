use std::{error::Error, fmt::Display, marker::PhantomData};

use super::{
    channel::{Receiver, Sender},
    partition::{Partition, PartitionId},
};

#[derive(Debug)]
pub enum TaskError<P: Partition> {
    PartitionError(P::Error),
    PartitionNotFound(PartitionId),
    PartitionInUse(PartitionId),
    PartitionLost(PartitionId),
    LockAcqFailed,
}

impl<P: Partition> Display for TaskError<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskError::PartitionError(err) => write!(f, "Partition error: {:?}", err),
            TaskError::PartitionNotFound(partition) => {
                write!(f, "Partition with id {:?} not found.", partition)
            }
            TaskError::PartitionInUse(partition) => {
                write!(f, "Partition with id {:?} still in use.", partition)
            }
            TaskError::PartitionLost(partition) => {
                write!(f, "Partition entry for {:?} lost.", partition)
            }
            TaskError::LockAcqFailed => {
                write!(f, "Unable to acquire required locks for op.")
            }
        }
    }
}

impl<P: Partition + std::fmt::Debug> Error for TaskError<P> {}

pub type TaskResult<Response, P> = Result<Response, TaskError<P>>;

pub struct Task<P, Request, Response, S>
where
    P: Partition,
    S: Sender<TaskResult<Response, P>>,
{
    pub request: Request,
    pub response_sender: S,

    _phantom_data: PhantomData<(P, Response)>,
}

impl<P, Request, Response, S> Task<P, Request, Response, S>
where
    P: Partition,
    S: Sender<TaskResult<Response, P>>,
{
    pub fn new(request: Request, response_sender: S) -> Self {
        Self {
            request,
            response_sender,
            _phantom_data: PhantomData,
        }
    }
}

pub trait Processor<P, Request, Response, S>
where
    P: Partition,
    S: Sender<TaskResult<Response, P>>,
{
    fn process(&self, task: Task<P, Request, Response, S>);
}

pub struct Worker<P, Request, Response, S, Proc>
where
    P: Partition,
    S: Sender<TaskResult<Response, P>>,
    Proc: Processor<P, Request, Response, S>,
{
    processor: Proc,

    _phantom_data: PhantomData<(P, S, Request, Response)>,
}

impl<P, Request, Response, S, Proc> Worker<P, Request, Response, S, Proc>
where
    P: Partition,
    S: Sender<TaskResult<Response, P>>,
    Proc: Processor<P, Request, Response, S>,
{
    pub fn new(processor: Proc) -> Self {
        Self {
            processor,
            _phantom_data: PhantomData,
        }
    }

    pub async fn process_tasks<R>(&self, task_receiver: R)
    where
        R: Receiver<Task<P, Request, Response, S>>,
    {
        while let Some(task) = task_receiver.recv().await {
            self.processor.process(task);
        }
    }
}

pub mod single_node {
    use super::super::{
        partition::{single_node::PartitionRequest, PartitionId},
        single_node::Request,
    };

    pub enum ProcessorRequest {
        CreatePartition(PartitionId),
        RemovePartition(PartitionId),
        PartitionHierarchy,
    }

    pub enum WorkerRequest {
        Partition {
            partition: PartitionId,
            request: PartitionRequest,
        },
        Processor(ProcessorRequest),
    }

    impl From<Request> for WorkerRequest {
        fn from(request: Request) -> Self {
            match request {
                Request::RemoveExpired {
                    partition,
                    expiry_duration,
                } => Self::Partition {
                    partition,
                    request: PartitionRequest::RemoveExpired { expiry_duration },
                },
                Request::Read { partition, offset } => Self::Partition {
                    partition,
                    request: PartitionRequest::Read { offset },
                },
                Request::Append {
                    partition,
                    record_bytes,
                } => Self::Partition {
                    partition,
                    request: PartitionRequest::Append { record_bytes },
                },
                Request::LowestOffset { partition } => Self::Partition {
                    partition,
                    request: PartitionRequest::LowestOffset,
                },
                Request::HighestOffset { partition } => Self::Partition {
                    partition,
                    request: PartitionRequest::HighestOffset,
                },

                Request::CreatePartition(partition) => {
                    Self::Processor(ProcessorRequest::CreatePartition(partition))
                }
                Request::RemovePartition(partition) => {
                    Self::Processor(ProcessorRequest::RemovePartition(partition))
                }
                Request::PartitionHierachy => Self::Processor(ProcessorRequest::PartitionHierarchy),
            }
        }
    }

    impl From<WorkerRequest> for Request {
        fn from(worker_request: WorkerRequest) -> Self {
            match worker_request {
                WorkerRequest::Partition { partition, request } => match request {
                    PartitionRequest::RemoveExpired { expiry_duration } => Self::RemoveExpired {
                        partition,
                        expiry_duration,
                    },
                    PartitionRequest::Read { offset } => Self::Read { partition, offset },
                    PartitionRequest::Append { record_bytes } => Self::Append {
                        partition,
                        record_bytes,
                    },
                    PartitionRequest::LowestOffset => Self::LowestOffset { partition },
                    PartitionRequest::HighestOffset => Self::HighestOffset { partition },
                },
                WorkerRequest::Processor(request) => match request {
                    ProcessorRequest::CreatePartition(x) => Self::CreatePartition(x),
                    ProcessorRequest::RemovePartition(x) => Self::RemovePartition(x),
                    ProcessorRequest::PartitionHierarchy => Self::PartitionHierachy,
                },
            }
        }
    }
}
