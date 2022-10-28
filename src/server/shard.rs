use std::{collections::HashMap, error::Error, marker::PhantomData, rc::Rc};

use glommio::sync::RwLock;

use super::{
    channel::{Receiver, Sender},
    partition::{Partition, PartitionId, Request, Response},
};

pub struct Task<'req, 'resp, E, S>
where
    S: Sender<Result<Response<'resp>, E>>,
    E: Error,
{
    pub partition_id: PartitionId,

    pub request: Request<'req>,
    pub response_sender: S,

    _phantom_data: PhantomData<(&'resp (), E)>,
}
pub struct Shard<'shard, 'partition, 'req, 'resp, P, E, S, Recv>
where
    P: Partition<'partition>,
    Recv: Receiver<Task<'req, 'resp, E, S>>,
    S: Sender<Result<Response<'resp>, E>>,
    E: std::error::Error,
    'shard: 'partition,
    'partition: 'resp,
{
    _partitions: HashMap<PartitionId, Rc<RwLock<P>>>,
    _receiver: Recv,

    _phantom_data: PhantomData<(&'shard &'partition &'req &'resp (), E, S)>,
}
