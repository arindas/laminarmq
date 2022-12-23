//! Module providing [`glommio`] specific single-node processor implementation.

use super::{
    super::{
        super::{
            channel::Sender,
            partition::{single_node::PartitionRequest, Partition, PartitionCreator, PartitionId},
            single_node::{Request, Response},
            worker::{
                single_node::{ProcessorRequest, WorkerRequest},
                Task, TaskError, TaskResult,
            },
        },
        worker::ResponseSender,
    },
    partition_consumer,
};
use glommio::{sync::RwLock, TaskQueueHandle};
use std::{borrow::Cow, collections::HashMap, ops::Deref, rc::Rc};
use tracing::{error, error_span, instrument, Instrument};

/// Single-node [`glommio`] specific processor implementation that schedules [`Task`] processing
/// futures on a dedicated [`glommio::TaskQueueHandle`]
#[derive(Clone)]
pub struct Processor<P, PC>
where
    P: Partition + 'static,
    PC: PartitionCreator<P> + Clone + 'static,
{
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    task_queue: TaskQueueHandle,

    partition_creator: PC,
}

impl<P, PC> Processor<P, PC>
where
    P: Partition + 'static,
    PC: PartitionCreator<P> + Clone + 'static,
{
    /// Creates a new [`Processor`] instance from the given [`TaskQueueHandle`] and
    /// [`PartitionCreator`] implementation.
    pub fn new(task_queue: TaskQueueHandle, partition_creator: PC) -> Self {
        Self {
            partitions: Rc::new(RwLock::new(HashMap::new())),
            task_queue,
            partition_creator,
        }
    }
}

async fn handle_idempotent_requests<X, T, P>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
    request: PartitionRequest<X>,
) -> TaskResult<Response<T>, P>
where
    X: Deref<Target = [u8]>,
    T: Deref<Target = [u8]>,
    P: Partition<Request = PartitionRequest<X>, Response = Response<T>>,
{
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

async fn handle_requests<X, T, P>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
    request: PartitionRequest<X>,
) -> TaskResult<Response<T>, P>
where
    X: Deref<Target = [u8]>,
    T: Deref<Target = [u8]>,
    P: Partition<Request = PartitionRequest<X>, Response = Response<T>>,
{
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

async fn handle_create_partition<
    X: Deref<Target = [u8]>,
    T: Deref<Target = [u8]>,
    P: Partition<Request = PartitionRequest<X>, Response = Response<T>>,
    PC: PartitionCreator<P>,
>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
    partition_creator: PC,
) -> TaskResult<Response<T>, P> {
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

async fn handle_remove_partition<X, T, P, PC>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
    partition_id: PartitionId,
    partition_creator: PC,
) -> TaskResult<Response<T>, P>
where
    X: Deref<Target = [u8]>,
    T: Deref<Target = [u8]>,
    P: Partition<Request = PartitionRequest<X>, Response = Response<T>>,
    PC: PartitionCreator<P>,
{
    let partition = partitions
        .write()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .remove(&partition_id)
        .ok_or_else(|| TaskError::PartitionNotFound(partition_id.clone()))?;

    partition_consumer::PartitionConsumer::new(
        partition_consumer::PartitionRemainder::Rc(partition),
        partition_id,
        partition_creator,
        partition_consumer::ConsumeMethod::Remove,
    )
    .consume()
    .await
    .map(|_| Response::PartitionRemoved)
}

async fn partition_hierachy<
    X: Deref<Target = [u8]>,
    T: Deref<Target = [u8]>,
    P: Partition<Request = PartitionRequest<X>, Response = Response<T>>,
>(
    partitions: Rc<RwLock<HashMap<PartitionId, Rc<RwLock<P>>>>>,
) -> TaskResult<Response<T>, P> {
    let mut topic_to_partitions = HashMap::<Cow<'static, str>, Vec<u64>>::new();

    for key in partitions
        .read()
        .await
        .map_err(|_| TaskError::LockAcqFailed)?
        .keys()
    {
        if let Some(partitions_in_topic) = topic_to_partitions.get_mut(&key.topic) {
            partitions_in_topic.push(key.partition_number);
        } else {
            topic_to_partitions.insert(key.topic.clone(), vec![key.partition_number]);
        }
    }

    Ok(Response::PartitionHierachy(topic_to_partitions))
}

impl<X, T, P, PC>
    super::super::super::worker::Processor<
        P,
        Request<X>,
        Response<T>,
        ResponseSender<Response<T>, P>,
    > for Processor<P, PC>
where
    X: Deref<Target = [u8]> + 'static,
    T: Deref<Target = [u8]> + 'static,
    P: Partition<Request = PartitionRequest<X>, Response = Response<T>> + 'static,
    PC: PartitionCreator<P> + Clone + 'static,
{
    #[instrument(skip(self, task))]
    fn process(&self, task: Task<P, Request<X>, Response<T>, ResponseSender<Response<T>, P>>) {
        let (partitions, partition_creator) =
            (self.partitions.clone(), self.partition_creator.clone());

        let spawn_result = glommio::spawn_local_into(
            async move {
                let task_result = match WorkerRequest::from(task.request) {
                    WorkerRequest::Partition { partition, request } => match &request {
                        PartitionRequest::Read { offset: _ }
                        | PartitionRequest::LowestOffset
                        | PartitionRequest::HighestOffset => {
                            handle_idempotent_requests(partitions, partition, request).await
                        }

                        PartitionRequest::RemoveExpired { expiry_duration: _ }
                        | PartitionRequest::Append { record_bytes: _ } => {
                            handle_requests(partitions, partition, request).await
                        }
                    },
                    WorkerRequest::Processor(request) => match request {
                        ProcessorRequest::CreatePartition(partition) => {
                            handle_create_partition(partitions, partition, partition_creator).await
                        }
                        ProcessorRequest::RemovePartition(partition) => {
                            handle_remove_partition(partitions, partition, partition_creator).await
                        }
                        ProcessorRequest::PartitionHierarchy => {
                            partition_hierachy(partitions).await
                        }
                    },
                };

                if let Err(send_error) = task.response_sender.try_send(task_result) {
                    error!("Unable to send result back: {:?}", send_error);
                }
            }
            .instrument(error_span!("single_node_processor_task_handler")),
            self.task_queue,
        );

        if let Err(spawn_error) = spawn_result.map(|task| task.detach()) {
            error!("Error detaching spawned task: {:?}", spawn_error);
        }
    }
}

impl<P, PC> Drop for Processor<P, PC>
where
    P: Partition + 'static,
    PC: PartitionCreator<P> + Clone + 'static,
{
    fn drop(&mut self) {
        let (partitions, partition_creator) =
            (self.partitions.clone(), self.partition_creator.clone());

        if let Ok(x) = glommio::spawn_local_into(
            async move {
                let partitions = partitions.write().await;

                if let Ok(mut partitions) = partitions {
                    for (partition_id, partition) in partitions.drain() {
                        partition_consumer::PartitionConsumer::new(
                            partition_consumer::PartitionRemainder::Rc(partition),
                            partition_id,
                            partition_creator.clone(),
                            partition_consumer::ConsumeMethod::Close,
                        )
                        .consume()
                        .await
                        .ok();
                    }
                }
            },
            self.task_queue,
        ) {
            x.detach();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, ops::Deref};

    use super::{
        super::super::{
            super::super::server::{
                channel::Receiver,
                glommio_impl::worker::{ResponseReceiver, ResponseSender},
                partition::{
                    single_node::{
                        in_memory::{Partition, PartitionCreator},
                        PartitionRequest,
                    },
                    PartitionId,
                },
                single_node::{Request, Response},
                worker::{
                    single_node::{ProcessorRequest, WorkerRequest},
                    Processor as BaseProcessor, Task, TaskError,
                },
            },
            worker::new_task,
        },
        Processor,
    };
    use glommio::{executor, Latency, LocalExecutorBuilder, Placement, Shares};

    fn new_single_node_in_memory_partition_task(
        worker_request: WorkerRequest<Cow<'static, [u8]>>,
    ) -> (
        Task<
            Partition,
            Request<Cow<'static, [u8]>>,
            Response<Cow<'static, [u8]>>,
            ResponseSender<Response<Cow<'static, [u8]>>, Partition>,
        >,
        ResponseReceiver<Response<Cow<'static, [u8]>>, Partition>,
    ) {
        new_task::<Partition, Request<Cow<'static, [u8]>>, Response<Cow<'static, [u8]>>>(
            worker_request.into(),
        )
    }

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
                    topic: "topic_1".into(),
                    partition_number: 1,
                };
                let partition_id_2 = PartitionId {
                    topic: "topic_2".into(),
                    partition_number: 2,
                };

                let (lowest_offset_task, recv) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Partition {
                        partition: partition_id_1.clone(),
                        request: PartitionRequest::LowestOffset,
                    });

                processor.process(lowest_offset_task);

                if let Some(Err(TaskError::PartitionNotFound(partition_id))) = recv.recv().await {
                    assert_eq!(partition_id_1, partition_id);
                } else {
                    assert!(
                        false,
                        "Wrong error type received for task when partition not found"
                    );
                }

                let (create_partition_task_1, recv_1) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Processor(
                        ProcessorRequest::CreatePartition(partition_id_1.clone()),
                    ));

                let (create_partition_task_2, recv_2) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Processor(
                        ProcessorRequest::CreatePartition(partition_id_2.clone()),
                    ));

                processor.process(create_partition_task_1);

                processor.process(create_partition_task_2);

                recv_1.recv().await.unwrap().unwrap();

                recv_2.recv().await.unwrap().unwrap();

                let sample_record_bytes: &[u8] = b"Lorem ipsum dolor sit amet.";

                let (append_record_task_1, recv_1) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Partition {
                        partition: partition_id_1.clone(),
                        request: PartitionRequest::Append {
                            record_bytes: sample_record_bytes.into(),
                        },
                    });

                processor.process(append_record_task_1);

                if let Some(Ok(Response::Append {
                    write_offset,
                    bytes_written,
                })) = recv_1.recv().await
                {
                    assert_eq!(write_offset, 0);
                    assert_eq!(bytes_written, sample_record_bytes.len());
                } else {
                    assert!(false, "Wrong response type for Append request.");
                }

                let (highest_offset_task_1, recv_1) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Partition {
                        partition: partition_id_1.clone(),
                        request: PartitionRequest::HighestOffset,
                    });

                let (highest_offset_task_2, recv_2) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Partition {
                        partition: partition_id_2.clone(),
                        request: PartitionRequest::HighestOffset,
                    });

                processor.process(highest_offset_task_1);
                processor.process(highest_offset_task_2);

                if let Some(Ok(Response::HighestOffset(highest_offset))) = recv_1.recv().await {
                    assert!(highest_offset > 0);
                } else {
                    assert!(false, "Wrong response type for HighestOffset request");
                }

                if let Some(Ok(Response::HighestOffset(highest_offset))) = recv_2.recv().await {
                    assert_eq!(highest_offset, 0);
                } else {
                    assert!(false, "Wrong response type for HighestOffset request");
                }

                let (read_record_task_1, recv_1) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Partition {
                        partition: partition_id_1.clone(),
                        request: PartitionRequest::Read { offset: 0 },
                    });

                processor.process(read_record_task_1);

                if let Some(Ok(Response::Read {
                    record,
                    next_offset: _,
                })) = recv_1.recv().await
                {
                    assert_eq!(record.value.deref(), sample_record_bytes);
                } else {
                    assert!(false, "Wrong response type for Read request");
                }

                let (partition_hierachy_task, recv) = new_single_node_in_memory_partition_task(
                    WorkerRequest::Processor(ProcessorRequest::PartitionHierarchy),
                );

                processor.process(partition_hierachy_task);

                if let Some(Ok(Response::PartitionHierachy(topic_to_partitions))) =
                    recv.recv().await
                {
                    assert_eq!(topic_to_partitions.get("topic_1".into()), Some(&vec![1]));
                    assert_eq!(topic_to_partitions.get("topic_2".into()), Some(&vec![2]));
                } else {
                    assert!(false, "Wrong response type for PartitionHierachy request");
                }

                let (remove_partition_task_1, recv_1) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Processor(
                        ProcessorRequest::RemovePartition(partition_id_1.clone()),
                    ));

                let (remove_partition_task_2, recv_2) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Processor(
                        ProcessorRequest::RemovePartition(partition_id_2.clone()),
                    ));
                processor.process(remove_partition_task_1);
                processor.process(remove_partition_task_2);

                recv_1.recv().await.unwrap().unwrap();
                recv_2.recv().await.unwrap().unwrap();

                // test drop
                let (create_partition_task_1, recv_1) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Processor(
                        ProcessorRequest::CreatePartition(partition_id_1.clone()),
                    ));

                let (create_partition_task_2, recv_2) =
                    new_single_node_in_memory_partition_task(WorkerRequest::Processor(
                        ProcessorRequest::CreatePartition(partition_id_2.clone()),
                    ));

                processor.process(create_partition_task_1);

                processor.process(create_partition_task_2);

                recv_1.recv().await.unwrap().unwrap();

                recv_2.recv().await.unwrap().unwrap();
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
