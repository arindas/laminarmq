pub mod commit_log {
    pub mod segmented_log {
        use crate::{
            commit_log::{
                glommio_impl::{
                    prelude::ReadResult,
                    segmented_log::{store::Store, SegmentCreator},
                },
                prelude::{glommio_segmented_log, SegmentedLog},
            },
            server::partition::{
                single_node::commit_log::{
                    segmented_log::{partition_storage_path, PartitionConfig},
                    Partition, PartitionError,
                },
                PartitionId,
            },
        };
        use async_trait::async_trait;

        #[derive(Debug, Clone)]
        pub struct PartitionCreator {
            partition_config: PartitionConfig,
        }

        impl PartitionCreator {
            pub fn new(partition_config: PartitionConfig) -> Self {
                Self { partition_config }
            }
        }

        #[async_trait(?Send)]
        impl
            crate::server::partition::PartitionCreator<
                Partition<SegmentedLog<ReadResult, Store, SegmentCreator>>,
            > for PartitionCreator
        {
            async fn new_partition(
                &self,
                partition_id: &PartitionId,
            ) -> Result<
                Partition<SegmentedLog<ReadResult, Store, SegmentCreator>>,
                PartitionError<SegmentedLog<ReadResult, Store, SegmentCreator>>,
            > {
                glommio_segmented_log(
                    partition_storage_path(
                        self.partition_config.base_storage_directory.as_ref(),
                        partition_id,
                    ),
                    self.partition_config.segmented_log_config,
                )
                .await
                .map(Partition)
                .map_err(PartitionError::CommitLog)
            }
        }

        #[cfg(test)]
        pub mod tests {
            use crate::{
                commit_log::{
                    prelude::{SegmentConfig, SegmentedLogConfig},
                    segmented_log::store::common::RECORD_HEADER_LENGTH,
                },
                server::{
                    partition::{
                        single_node::{
                            commit_log::{segmented_log::PartitionConfig, PartitionError},
                            PartitionRequest,
                        },
                        Partition as _, PartitionCreator as _, PartitionId,
                    },
                    single_node::Response,
                },
            };

            use glommio::{LocalExecutorBuilder, Placement};

            use std::{fs, path::Path, time::Duration};

            #[test]
            fn test_partition() {
                const PARTITION_BASE_DIRECTORY: &str =
                "/tmp/laminarmq_server_glommio_impl_partition_single_node_commit_log_segmented_log";

                if Path::new(PARTITION_BASE_DIRECTORY).exists() {
                    fs::remove_dir_all(PARTITION_BASE_DIRECTORY).unwrap();
                }

                const RECORD_BYTES: &[u8] = b"Lorem ipsum dolor sit amet.";
                const SENTINEL: &[u8] = b"0";

                let max_store_bytes = bincode::serialized_size(&(0 as u64)).unwrap()
                    + (RECORD_BYTES.len() + RECORD_HEADER_LENGTH) as u64;

                let partition_config = PartitionConfig {
                    base_storage_directory: PARTITION_BASE_DIRECTORY.into(),
                    segmented_log_config: SegmentedLogConfig {
                        initial_offset: 0,
                        segment_config: SegmentConfig {
                            store_buffer_size: None,
                            max_store_bytes,
                        },
                    },
                };

                let partition_creator = super::PartitionCreator::new(partition_config);
                let partition_id = PartitionId {
                    topic: "topic".into(),
                    partition_number: 1,
                };

                LocalExecutorBuilder::new(Placement::Unbound)
                    .spawn(|| async move {
                        let mut partition = partition_creator
                            .new_partition(&partition_id)
                            .await
                            .unwrap();

                        assert!(matches!(
                            partition
                                .serve_idempotent(PartitionRequest::Read { offset: 0 })
                                .await,
                            Err(PartitionError::CommitLog(_))
                        ));

                        assert!(matches!(
                            partition
                                .serve_idempotent(PartitionRequest::Append {
                                    record_bytes: RECORD_BYTES.into()
                                })
                                .await,
                            Err(PartitionError::NotSupported)
                        ));

                        assert!(matches!(
                            partition
                                .serve_idempotent(PartitionRequest::RemoveExpired {
                                    expiry_duration: Duration::from_secs(0)
                                })
                                .await,
                            Err(PartitionError::NotSupported)
                        ));

                        partition
                            .serve(PartitionRequest::Append {
                                record_bytes: RECORD_BYTES.into(),
                            })
                            .await
                            .unwrap();

                        partition
                            .serve(PartitionRequest::Append {
                                record_bytes: SENTINEL.into(),
                            })
                            .await
                            .unwrap();

                        if let Ok(Response::LowestOffset(lowest_offset)) =
                            partition.serve(PartitionRequest::LowestOffset).await
                        {
                            assert_eq!(lowest_offset, 0);
                        } else {
                            assert!(false, "Wrong response type!");
                        }

                        if let Ok(Response::HighestOffset(highest_offset)) =
                            partition.serve(PartitionRequest::HighestOffset).await
                        {
                            assert!(highest_offset > 0);
                        } else {
                            assert!(false, "Wrong response type!");
                        }

                        if let Ok(Response::Read {
                            record,
                            next_offset: _,
                        }) = partition
                            .serve_idempotent(PartitionRequest::Read { offset: 0 })
                            .await
                        {
                            assert_eq!(record.value, RECORD_BYTES);
                        } else {
                            assert!(false, "Wrong response type!");
                        }

                        partition.close().await.unwrap();

                        let mut partition = partition_creator
                            .new_partition(&partition_id)
                            .await
                            .unwrap();

                        let mut offset = 0;
                        for record_value in [RECORD_BYTES, SENTINEL] {
                            if let Ok(Response::Read {
                                record,
                                next_offset,
                            }) = partition
                                .serve_idempotent(PartitionRequest::Read { offset })
                                .await
                            {
                                assert_eq!(record.value, record_value);
                                offset = next_offset;
                            } else {
                                assert!(false, "Wrong response type!");
                            }
                        }

                        partition
                            .serve(PartitionRequest::RemoveExpired {
                                expiry_duration: Duration::from_secs(0),
                            })
                            .await
                            .unwrap();

                        let (mut lowest_offset_value, mut highest_offset_value) = (0, 0);

                        if let Ok(Response::LowestOffset(lowest_offset)) = partition
                            .serve_idempotent(PartitionRequest::LowestOffset)
                            .await
                        {
                            lowest_offset_value = lowest_offset;
                        } else {
                            assert!(false, "Wrong response type!");
                        }

                        if let Ok(Response::HighestOffset(highest_offset)) = partition
                            .serve_idempotent(PartitionRequest::HighestOffset)
                            .await
                        {
                            highest_offset_value = highest_offset;
                        } else {
                            assert!(false, "Wrong response type!");
                        }

                        assert_eq!(lowest_offset_value, highest_offset_value);

                        partition.remove().await.unwrap();
                    })
                    .unwrap()
                    .join()
                    .unwrap();

                if Path::new(PARTITION_BASE_DIRECTORY).exists() {
                    fs::remove_dir_all(PARTITION_BASE_DIRECTORY).unwrap();
                }
            }
        }
    }
}
