#[async_trait::async_trait(?Send)]
pub trait Router<Request> {
    async fn route(&self, req: hyper::Request<hyper::Body>) -> Option<Request>;
}

pub mod single_node {
    use super::super::{
        partition::PartitionId,
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

            map.get_mut(method).map(|x| x.add(*path, *req_kind));
        }

        map
    }

    pub struct Router(pub HashMap<Method, UriRouter>);

    #[async_trait::async_trait(?Send)]
    impl super::Router<Request> for Router {
        async fn route(&self, req: hyper::Request<hyper::Body>) -> Option<Request> {
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

            match (req.method(), route_match.handler()) {
                (&Method::GET, &&RequestKind::LowestOffset) => Some(Request::LowestOffset {
                    partition: PartitionId {
                        topic: topic_id?.to_owned().into(),
                        partition_number: partition_number?,
                    },
                }),
                _ => None,
            }
        }
    }
}
