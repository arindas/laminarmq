#[async_trait::async_trait(?Send)]
pub trait Router<Request> {
    async fn route(&self, req: hyper::Request<hyper::Body>) -> Option<Request>;
}

pub mod single_node {
    use super::super::{
        partition::{PartitionId, DEFAULT_EXPIRY_DURATION},
        single_node::{Request, RequestKind},
    };
    use hyper::Method;
    use std::collections::HashMap;

    pub const ROUTES: &[(&str, Method, RequestKind)] = &[
        (
            "/api/v1/topics/:topic_id/partitions/:partition_id/records/:offset",
            Method::GET,
            RequestKind::Read,
        ),
        (
            "/api/v1/topics/:topic_id/partitions/:partition_id/records/",
            Method::POST,
            RequestKind::Append,
        ),
        (
            "/api/v1/topics/:topic_id/partitions/:partition_id/stat/lowest_offset",
            Method::GET,
            RequestKind::LowestOffset,
        ),
        (
            "/api/v1/topics/:topic_id/partitions/:partition_id/stat/highest_offset",
            Method::GET,
            RequestKind::HighestOffset,
        ),
        (
            "/api/v1/topics/:topic_id/partitions/:partition_id/remove_expired",
            Method::POST,
            RequestKind::RemoveExpired,
        ),
        (
            "/api/v1/hierachy",
            Method::GET,
            RequestKind::PartitionHierachy,
        ),
        (
            "/api/v1/topics/:topic_id/partitions/:partition_id",
            Method::POST,
            RequestKind::CreatePartition,
        ),
        (
            "/api/v1/topics/:topic_id/partitions/:partition_id",
            Method::DELETE,
            RequestKind::RemovePartition,
        ),
    ];

    pub type UriRouter = route_recognizer::Router<RequestKind>;

    pub fn route_map(routes: &[(&str, Method, RequestKind)]) -> HashMap<Method, UriRouter> {
        let mut map = HashMap::new();

        for (path, method, req_kind) in routes {
            if !map.contains_key(method) {
                map.insert(method.clone(), UriRouter::new());
            }

            if let Some(router) = map.get_mut(method) {
                router.add(path, *req_kind);
            }
        }

        map
    }

    pub struct Router(pub HashMap<Method, UriRouter>);

    #[async_trait::async_trait(?Send)]
    impl super::Router<Request> for Router {
        async fn route(&self, mut req: hyper::Request<hyper::Body>) -> Option<Request> {
            let route_match = self
                .0
                .get(req.method())
                .and_then(|x| x.recognize(req.uri().path()).ok())?;

            let params = route_match.params();
            let (topic_id, partition_number) = (
                params.find("topic_id"),
                params
                    .find("partition_id")
                    .and_then(|x| x.parse::<u64>().ok()),
            );

            macro_rules! partition_id {
                ($topic_id:ident, $partition_number:ident) => {
                    PartitionId {
                        topic: $topic_id?.to_owned().into(),
                        partition_number: $partition_number?,
                    }
                };
            }

            match (req.method(), route_match.handler()) {
                (&Method::GET, &&RequestKind::Read) => Some(Request::Read {
                    partition: partition_id!(topic_id, partition_number),
                    offset: params.find("offset").and_then(|x| x.parse::<u64>().ok())?,
                }),
                (&Method::POST, &&RequestKind::Append) => Some(Request::Append {
                    partition: partition_id!(topic_id, partition_number),
                    record_bytes: hyper::body::to_bytes(req.body_mut())
                        .await
                        .ok()?
                        .to_vec()
                        .into(),
                }),
                (&Method::GET, &&RequestKind::LowestOffset) => Some(Request::LowestOffset {
                    partition: partition_id!(topic_id, partition_number),
                }),
                (&Method::GET, &&RequestKind::HighestOffset) => Some(Request::HighestOffset {
                    partition: partition_id!(topic_id, partition_number),
                }),
                (&Method::POST, &&RequestKind::RemoveExpired) => Some(Request::RemoveExpired {
                    partition: partition_id!(topic_id, partition_number),
                    expiry_duration: DEFAULT_EXPIRY_DURATION,
                }),
                (&Method::GET, &&RequestKind::PartitionHierachy) => {
                    Some(Request::PartitionHierachy)
                }
                (&Method::POST, &&RequestKind::CreatePartition) => Some(Request::CreatePartition(
                    partition_id!(topic_id, partition_number),
                )),
                (&Method::DELETE, &&RequestKind::RemovePartition) => Some(
                    Request::RemovePartition(partition_id!(topic_id, partition_number)),
                ),
                _ => None,
            }
        }
    }
}
