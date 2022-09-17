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
    use super::{GlommioLog as Log, GlommioLogError as LogError, SegmentCreator};
    use crate::commit_log::{
        segment::config::SegmentConfig,
        segmented_log::{common::store_file_path, config::SegmentedLogConfig as LogConfig},
        store::common::bincoded_serialized_record_size,
        CommitLog, LogScanner, Record, Scanner,
    };
    use glommio::{LocalExecutorBuilder, Placement};
    use std::{
        fs,
        path::{Path, PathBuf},
        time::Duration,
    };

    #[test]
    fn test_log_new_close_and_remove() {
        let local_ex = LocalExecutorBuilder::new(Placement::Unbound)
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

        let local_ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                const RECORD_VALUE: &[u8] = b"Hello world!";
                let mut record = Record {
                    value: RECORD_VALUE.to_vec(),
                    offset: 0,
                };
                let record_size = bincoded_serialized_record_size(&record).unwrap();

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

                assert!(matches!(
                    log.read(log.highest_offset()).await,
                    Err(LogError::OffsetOutOfBounds)
                ));

                log.remove().await.unwrap();
                assert!(!PathBuf::from(&storage_dir_path,).exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }

    #[test]
    fn test_log_remove_expired_segments() {
        const STORAGE_DIR_PATH: &str = "/tmp/laminarmq_log_test_log_remove_expired_segments";
        if Path::new(STORAGE_DIR_PATH).exists() {
            fs::remove_dir_all(STORAGE_DIR_PATH).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                const RECORD_VALUE: &[u8] = b"Hello world!";
                let mut record = Record {
                    value: RECORD_VALUE.to_vec(),
                    offset: 0,
                };
                let record_size = bincoded_serialized_record_size(&record).unwrap();

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

                const NUM_RECORDS: u32 = 10;

                let mut base_offset_of_first_non_expired_segment = 0;

                for _ in 0..NUM_RECORDS / 2 {
                    // this write will trigger log rotation
                    base_offset_of_first_non_expired_segment =
                        log.append(&mut record).await.unwrap();
                }

                let expiry_duration = Duration::from_millis(100);

                glommio::timer::sleep(expiry_duration).await;

                for _ in NUM_RECORDS / 2..NUM_RECORDS {
                    log.append(&mut record).await.unwrap();
                }

                log.remove_expired_segments(expiry_duration).await.unwrap();

                assert_eq!(
                    log.lowest_offset(),
                    base_offset_of_first_non_expired_segment
                );

                log.read(log.lowest_offset()).await.unwrap();

                log.remove().await.unwrap();
                assert!(!PathBuf::from(&storage_dir_path).exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }

    #[test]
    fn test_log_advance_offset() {
        const STORAGE_DIR_PATH: &str = "/tmp/laminarmq_log_test_log_advance_offset";
        if Path::new(STORAGE_DIR_PATH).exists() {
            fs::remove_dir_all(STORAGE_DIR_PATH).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                const RECORD_VALUE: &[u8] = b"Hello world!";
                let mut record = Record {
                    value: RECORD_VALUE.to_vec(),
                    offset: 0,
                };
                let record_size = bincoded_serialized_record_size(&record).unwrap();

                let log_config = LogConfig {
                    initial_offset: 0,
                    segment_config: SegmentConfig {
                        store_buffer_size: 512,
                        max_store_bytes: record_size,
                    },
                };

                let storage_dir_path = STORAGE_DIR_PATH.to_string();

                let mut log_0 = Log::new(storage_dir_path.clone(), log_config, SegmentCreator)
                    .await
                    .unwrap();

                let mut log_1 = Log::new(storage_dir_path.clone(), log_config, SegmentCreator)
                    .await
                    .unwrap();

                log_0.append(&mut record).await.unwrap(); // record written but not guranteed to be
                                                          // synced

                assert!(matches!(
                    log_1.advance_to_offset(log_0.highest_offset()).await,
                    Err(LogError::OffsetNotValidToAdvanceTo)
                ));

                log_0.append(&mut record).await.unwrap(); // first segment rotation
                let highest_offset_2 = log_0.highest_offset();

                assert!(matches!(
                    log_1.advance_to_offset(highest_offset_2).await,
                    Err(LogError::OffsetNotValidToAdvanceTo)
                ));

                log_0.append(&mut record).await.unwrap(); // second log rotation; 2nd segment
                                                          // synced

                log_1.advance_to_offset(highest_offset_2).await.unwrap();

                let final_highest_offset = log_0.highest_offset();

                log_0.close().await.unwrap(); // all segments guranteed to be synced

                log_1.advance_to_offset(final_highest_offset).await.unwrap();

                log_1.close().await.unwrap();

                let log = Log::new(storage_dir_path.clone(), log_config, SegmentCreator)
                    .await
                    .unwrap();
                log.remove().await.unwrap();
                assert!(!PathBuf::from(&storage_dir_path,).exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }
}
