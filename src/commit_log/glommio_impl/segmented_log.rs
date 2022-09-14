use super::store::Store;
use crate::commit_log::{
    segment::{config::SegmentConfig, Segment, SegmentError},
    segmented_log::{SegmentedLog, SegmentedLogError},
};
use glommio::io::ReadResult;
use std::path::Path;

pub struct SegmentCreator;

#[async_trait::async_trait(?Send)]
impl crate::commit_log::segmented_log::SegmentCreator<ReadResult, Store> for SegmentCreator {
    async fn new_segment_with_store_file_path_offset_and_config<P: AsRef<Path>>(
        &self,
        store_file_path: P,
        base_offset: u64,
        config: SegmentConfig,
    ) -> Result<Segment<ReadResult, Store>, SegmentError<ReadResult, Store>> {
        Ok(Segment::with_config_base_offset_and_store(
            config,
            base_offset,
            Store::new(store_file_path)
                .await
                .map_err(SegmentError::StoreError)?,
        ))
    }
}

pub type GlommioLog = SegmentedLog<ReadResult, Store, SegmentCreator>;

pub type GlommioLogError = SegmentedLogError<ReadResult, Store>;

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use glommio::{LocalExecutorBuilder, Placement};

    use super::GlommioLog as Log;
    use super::GlommioLogError as LogError;
    use super::SegmentCreator;
    use crate::commit_log::{
        segment::config::SegmentConfig,
        segmented_log::{common::store_file_path, config::SegmentedLogConfig as LogConfig},
        CommitLog, LogScanner, Record, Scanner,
    };

    #[test]
    fn test_log_new_close_and_remove() {
        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spawn(move || async move {
                const LOG_CONFIG: LogConfig = LogConfig {
                    initial_offset: 0,
                    segment_config: SegmentConfig {
                        store_buffer_size: 512,
                        max_store_bytes: 512,
                    },
                };

                let storage_dir_path =
                    "/tmp/laminarmq_log_test_log_new_close_and_remove".to_string();

                let log = Log::new(storage_dir_path.clone(), LOG_CONFIG, SegmentCreator)
                    .await
                    .unwrap();

                log.close().await.unwrap();

                assert!(PathBuf::from(store_file_path(
                    &storage_dir_path,
                    LOG_CONFIG.initial_offset
                ))
                .exists());

                let log = Log::new(storage_dir_path.clone(), LOG_CONFIG, SegmentCreator)
                    .await
                    .unwrap();

                log.remove().await.unwrap();
                assert!(!PathBuf::from(&storage_dir_path,).exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }

    #[test]
    fn test_log_reads_reflect_writes() {
        const STORAGE_DIR_PATH: &str = "/tmp/laminarmq_log_test_log_reads_reflect_writes";
        if Path::new(STORAGE_DIR_PATH).exists() {
            fs::remove_dir_all(STORAGE_DIR_PATH).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spawn(move || async move {
                const RECORD_VALUE: &[u8] = b"Hello world!";
                let mut record = Record {
                    value: RECORD_VALUE.to_vec(),
                    offset: 0,
                };
                let record_size = record.bincoded_repr_size().unwrap();

                let log_config = LogConfig {
                    initial_offset: 0,
                    segment_config: SegmentConfig {
                        store_buffer_size: 512,
                        max_store_bytes: record_size,
                    },
                };

                let storage_dir_path = STORAGE_DIR_PATH.to_string();

                let mut log = Log::new(storage_dir_path.clone(), log_config, SegmentCreator)
                    .await
                    .unwrap();

                let offset_0 = log.append(&mut record).await.unwrap();
                assert_eq!(offset_0, log_config.initial_offset);
                // not enough bytes written to trigger sync
                matches!(log.read(offset_0).await, Err(LogError::SegmentError(_)));

                const NUM_RECORDS: u32 = 17;
                let mut prev_offset = offset_0;

                for _ in 1..NUM_RECORDS {
                    assert!(log.is_write_segment_maxed().unwrap());
                    // this write will trigger log rotation
                    let curr_offset = log.append(&mut record).await.unwrap();

                    let record = log.read(prev_offset).await.unwrap();
                    assert_eq!(
                        record,
                        Record {
                            value: RECORD_VALUE.to_vec(),
                            offset: prev_offset
                        }
                    );
                    prev_offset = curr_offset;
                }

                log.close().await.unwrap();

                let log = Log::new(storage_dir_path.clone(), log_config, SegmentCreator)
                    .await
                    .unwrap();

                let mut log_scanner = LogScanner::new(&log).unwrap();
                let mut i = 0;
                while let Some(record) = log_scanner.next().await {
                    assert_eq!(record.value, RECORD_VALUE);
                    i += 1;
                }
                assert_eq!(i, NUM_RECORDS);

                log.remove().await.unwrap();
                assert!(!PathBuf::from(&storage_dir_path,).exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }
}
