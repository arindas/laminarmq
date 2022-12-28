//! Module providing abstractions for routing HTTP requests to concrete RPC Request types.

/// Generic trait representing a router capable of routing HTTP requests to the given Request type.
#[async_trait::async_trait(?Send)]
pub trait Router<Request> {
    /// Routes a [`hyper::Request`] to a `Request`. A None value indicates that no corresponding
    /// RPC Request type was found for the given HTTP request.
    async fn route(&self, req: hyper::Request<hyper::Body>) -> Option<Request>;
}

pub mod single_node {
    //! Module responsible for routing single node HTTP requests to concrete single node RPC
    //! requests.
    use super::super::{
        partition::{single_node::DEFAULT_EXPIRY_DURATION, PartitionId},
        single_node::{Request, RequestKind},
    };
    use hyper::Method;
    use std::collections::HashMap;

    /// Application HTTP routes.
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

    /// Alias for representing a single node router.
    /// Routes from HTTP URI paths to [`RequestKind`] instances.
    pub type UriRouter = route_recognizer::Router<RequestKind>;

    /// Collects given routes into a [`HashMap<Method, UriRouter>`].
    /// This function groups all routes by method. For each method, the route paths and their
    /// corresponding request kinds are aggregated into an [`UriRouter`]. Finally the methods
    /// and their corresponding [`UriRouter`] are collected into a [`HashMap`].
    ///
    /// ## Returns
    /// - [`HashMap<Method, UriRouter>`]: A mapping from HTTP methods to their corresponding
    /// router for routing http uri paths to [`RequestKind`] instances.
    pub fn route_map<'a, I: Iterator<Item = &'a (&'a str, Method, RequestKind)>>(
        routes: I,
    ) -> HashMap<Method, UriRouter> {
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

    /// Single node HTTP to RPC Request router.
    pub struct Router(HashMap<Method, UriRouter>);

    impl Router {
        /// Creates a new router from the given [`HashMap<Method, UriRouter>`].
        pub fn with_route_map(route_map: HashMap<Method, UriRouter>) -> Self {
            Self(route_map)
        }

        /// Creates a new single node HTTP router from [`ROUTES`]
        pub fn new() -> Self {
            Self::with_route_map(route_map(ROUTES.iter()))
        }
    }

    impl Default for Router {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait::async_trait(?Send)]
    impl super::Router<Request<bytes::Bytes>> for Router {
        async fn route(
            &self,
            mut req: hyper::Request<hyper::Body>,
        ) -> Option<Request<bytes::Bytes>> {
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
                    record_bytes: hyper::body::to_bytes(req.body_mut()).await.ok()?,
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

    #[cfg(test)]
    mod tests {
        use hyper::{Body, Request as HyperRequest};

        use crate::server::{
            partition::single_node::DEFAULT_EXPIRY_DURATION,
            router::{single_node::Router, Router as _},
            single_node::Request,
        };

        #[test]
        fn test_router() {
            futures_lite::future::block_on(async {
                let router = Router::default();

                if let Some(Request::Read { partition, offset }) = router
                    .route(
                        HyperRequest::get("/api/v1/topics/some_topic/partitions/68419/records/109")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                {
                    assert_eq!(partition.topic, "some_topic");
                    assert_eq!(partition.partition_number, 68419);
                    assert_eq!(offset, 109);
                } else {
                    assert!(false, "Read request not routed!");
                }

                if let Some(Request::Append {
                    partition,
                    record_bytes,
                }) = router
                    .route(
                        HyperRequest::post("/api/v1/topics/some_topic/partitions/68419/records/")
                            .body(Body::from(bytes::Bytes::from_static(b"Hello World!")))
                            .unwrap(),
                    )
                    .await
                {
                    assert_eq!(partition.topic, "some_topic");
                    assert_eq!(partition.partition_number, 68419);
                    let bytes: &[u8] = b"Hello World!";
                    assert_eq!(record_bytes, bytes);
                } else {
                    assert!(false, "Append request not routed!")
                }

                if let Some(Request::LowestOffset { partition }) = router
                    .route(
                        HyperRequest::get(
                            "/api/v1/topics/some_topic/partitions/68419/stat/lowest_offset",
                        )
                        .body(Body::empty())
                        .unwrap(),
                    )
                    .await
                {
                    assert_eq!(partition.topic, "some_topic");
                    assert_eq!(partition.partition_number, 68419);
                } else {
                    assert!(false, "LowestOffset request not routed!");
                }

                if let Some(Request::HighestOffset { partition }) = router
                    .route(
                        HyperRequest::get(
                            "/api/v1/topics/some_topic/partitions/68419/stat/highest_offset",
                        )
                        .body(Body::empty())
                        .unwrap(),
                    )
                    .await
                {
                    assert_eq!(partition.topic, "some_topic");
                    assert_eq!(partition.partition_number, 68419);
                } else {
                    assert!(false, "HighestOffset request not routed!");
                }

                if let Some(Request::RemoveExpired {
                    partition,
                    expiry_duration,
                }) = router
                    .route(
                        HyperRequest::post(
                            "/api/v1/topics/some_topic/partitions/68419/remove_expired",
                        )
                        .body(Body::empty())
                        .unwrap(),
                    )
                    .await
                {
                    assert_eq!(partition.topic, "some_topic");
                    assert_eq!(partition.partition_number, 68419);
                    assert_eq!(expiry_duration, DEFAULT_EXPIRY_DURATION);
                } else {
                    assert!(false, "RemoveExpired request not routed!");
                }

                if let Some(Request::PartitionHierachy) = router
                    .route(
                        HyperRequest::get("/api/v1/hierachy")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                {
                } else {
                    assert!(false, "PartitionHierachy request not routed!");
                }

                if let Some(Request::CreatePartition(partition)) = router
                    .route(
                        HyperRequest::post("/api/v1/topics/some_topic/partitions/68419")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                {
                    assert_eq!(partition.topic, "some_topic");
                    assert_eq!(partition.partition_number, 68419);
                } else {
                    assert!(false, "CreatePartition request not routed!");
                }

                if let Some(Request::RemovePartition(partition)) = router
                    .route(
                        HyperRequest::delete("/api/v1/topics/some_topic/partitions/68419")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                {
                    assert_eq!(partition.topic, "some_topic");
                    assert_eq!(partition.partition_number, 68419);
                } else {
                    assert!(false, "RemovePartition request not routed!");
                }

                assert!(router
                    .route(HyperRequest::get("/bad/uri").body(Body::empty()).unwrap())
                    .await
                    .is_none());
            });
        }
    }
}
