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
    }
}
