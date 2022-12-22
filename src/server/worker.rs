//! Module providing abstraction for processing requests with a thread pre core architecture.
//!
//! ![execution-model](https://i.imgur.com/8QrCjD2.png)
//!
//! `laminarmq` uses the thread-per-core execution model where individual processor cores are limited to single threads.
//! This model encourages design that minimizes inter-thread contention and locks, thereby improving tail latencies in
//! software services. Read: [The Impact of Thread per Core Architecture on Application Tail Latency.](
//! https://helda.helsinki.fi//bitstream/handle/10138/313642/tpc_ancs19.pdf?sequence=1)
//!
//! In our case, each thread is responsible for servicing only a subset of the partitions. Requests pertaining to a specific
//! partition are always routed to the same thread. This greatly increases locality of requests. The routing mechanism
//! could be implemented in a several ways:
//! - Each thread listens on a unique port. We have a reverse proxy of sorts to forward requests to specific ports.
//! - We use eBPF to route request packets to threads
//!
//! Since each processor core is limited to a single thread, tasks in a thread need to be scheduled efficiently. Hence each
//! worker thread runs their own task scheduler. Tasks can be scheduled on different task queues and different task queues
//! can be provisioned with specific fractions of CPU time shares.

use std::{error::Error, fmt::Display, marker::PhantomData};

use super::{
    channel::{Receiver, Sender},
    partition::{Partition, PartitionId},
};

/// Error type associated with a [`TaskResult`].
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

/// Type alias for representing the [`Result`] of a processed [`Task`].
pub type TaskResult<Response, P> = Result<Response, TaskError<P>>;

/// [`Task`] is used to schedule a message queue RPC server `Request` for processing.
pub struct Task<P, Request, Response, S>
where
    P: Partition,
    S: Sender<TaskResult<Response, P>>,
{
    /// `Request to be processed.
    pub request: Request,

    /// Send end of the channel to send back response
    pub response_sender: S,

    _phantom_data: PhantomData<(P, Response)>,
}

impl<P, Request, Response, S> Task<P, Request, Response, S>
where
    P: Partition,
    S: Sender<TaskResult<Response, P>>,
{
    /// Creates a new [`Task`] from the given `Request` and `Response` [`Sender`].
    pub fn new(request: Request, response_sender: S) -> Self {
        Self {
            request,
            response_sender,
            _phantom_data: PhantomData,
        }
    }
}

/// Trait reprsenting a mechanism for processing [`Task`] instances.
pub trait Processor<P, Request, Response, S>
where
    P: Partition,
    S: Sender<TaskResult<Response, P>>,
{
    /// Processes the given [`Task`] and sends back the response on
    /// the given task's response send channel end.
    fn process(&self, task: Task<P, Request, Response, S>);
}

/// [`Worker`] receives [`Task`] instances from a source and processes then with
/// an underlying [`Processor`] instance.
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
    /// Creates a new [`Worker`] instance from the given [`Processor`].
    pub fn new(processor: Proc) -> Self {
        Self {
            processor,
            _phantom_data: PhantomData,
        }
    }

    /// Drains the given [`Task`] channel and processes each of them
    /// with the underlying [`Processor`].
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
    use std::ops::Deref;

    use super::super::{
        partition::{single_node::PartitionRequest, PartitionId},
        single_node::Request,
    };

    /// Adminstrative requests specific to a [`Processor`](super::Processor) and not a
    /// specific [`Partition`](crate::server::partition::Partition) instance.
    pub enum ProcessorRequest {
        CreatePartition(PartitionId),
        RemovePartition(PartitionId),
        PartitionHierarchy,
    }

    /// Request enumeration to generalize over [`ProcessorRequest`] and
    /// [`PartitionRequest`].
    pub enum WorkerRequest<T: Deref<Target = [u8]>> {
        Partition {
            partition: PartitionId,
            request: PartitionRequest<T>,
        },
        Processor(ProcessorRequest),
    }

    impl<T: Deref<Target = [u8]>> From<Request<T>> for WorkerRequest<T> {
        fn from(request: Request<T>) -> Self {
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

    impl<T: Deref<Target = [u8]>> From<WorkerRequest<T>> for Request<T> {
        fn from(worker_request: WorkerRequest<T>) -> Self {
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
