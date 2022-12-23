//! Module providing the specialization for
//! [`SegmentedLog`](crate::commit_log::segmented_log::SegmentedLog) for the [`glommio`] runtime.

pub mod segment;
pub mod store;

use crate::commit_log::segmented_log::{
    config::SegmentedLogConfig,
    segment::{config::SegmentConfig, Segment, SegmentError},
    SegmentCreator as BaseSegmentCreator, SegmentedLog, SegmentedLogError,
};

use async_trait::async_trait;
use glommio::io::ReadResult;
use std::path::Path;
use store::Store;

/// [`crate::commit_log::segmented_log::SegmentCreator`] implementation for the [`glommio`]
/// runtime using [`Store`] and [`Segment`] for [`glommio`].
pub struct SegmentCreator;

#[async_trait(?Send)]
impl<M> BaseSegmentCreator<ReadResult, M, Store> for SegmentCreator
where
    M: Default + serde::Serialize + serde::de::DeserializeOwned,
{
    async fn new_segment_with_store_file_path_offset_and_config<P: AsRef<Path>>(
        &self,
        store_file_path: P,
        base_offset: u64,
        config: SegmentConfig,
    ) -> Result<Segment<ReadResult, M, Store>, SegmentError<ReadResult, Store>> {
        segment::glommio_segment(store_file_path, base_offset, config).await
    }
}

/// Creates a new [`SegmentedLog`] instance specialized for the [`glommio`] runtime.
pub async fn glommio_segmented_log<P, M>(
    path: P,
    config: SegmentedLogConfig,
) -> Result<SegmentedLog<ReadResult, M, Store, SegmentCreator>, SegmentedLogError<ReadResult, Store>>
where
    P: AsRef<Path>,
    M: Default + serde::Serialize + serde::de::DeserializeOwned,
{
    SegmentedLog::new(path, config, SegmentCreator).await
}

#[cfg(test)]
mod tests {
    use super::{glommio_segmented_log, SegmentCreator, SegmentedLog, SegmentedLogError, Store};
    use crate::commit_log::{
        commit_log_record_stream,
        segmented_log::{
            common::store_file_path, config::SegmentedLogConfig as LogConfig,
            segment::config::SegmentConfig, store::common::RECORD_HEADER_LENGTH, RecordMetadata,
            SegmentedLogError as LogError,
        },
        CommitLog,
    };
    use futures_lite::StreamExt;
    use futures_util::pin_mut;
    use glommio::{io::ReadResult, LocalExecutorBuilder, Placement};
    use std::ops::Deref;
    use std::{
        fs,
        path::{Path, PathBuf},
        time::Duration,
    };

    #[test]
    fn test_log_new_close_and_remove() {
        const STORAGE_DIR_PATH: &str = "/tmp/laminarmq_log_test_log_new_close_and_remove";
        if Path::new(STORAGE_DIR_PATH).exists() {
            fs::remove_dir_all(STORAGE_DIR_PATH).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                const LOG_CONFIG: LogConfig = LogConfig {
                    initial_offset: 0,
                    segment_config: SegmentConfig {
                        store_buffer_size: None,
                        max_store_bytes: 512,
                    },
                    truncate_on_append: false,
                };

                let log: SegmentedLog<ReadResult, (), Store, SegmentCreator> =
                    glommio_segmented_log(STORAGE_DIR_PATH, LOG_CONFIG)
                        .await
                        .unwrap();

                log.close().await.unwrap();

                assert!(PathBuf::from(store_file_path(
                    STORAGE_DIR_PATH,
                    LOG_CONFIG.initial_offset
                ))
                .exists());

                let log: SegmentedLog<ReadResult, (), Store, SegmentCreator> =
                    glommio_segmented_log(STORAGE_DIR_PATH, LOG_CONFIG)
                        .await
                        .unwrap();

                log.remove().await.unwrap();
                assert!(!PathBuf::from(STORAGE_DIR_PATH).exists());
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
                let record_size = bincode::serialized_size(&RecordMetadata::<()>::default())
                    .unwrap()
                    + (RECORD_VALUE.len() + RECORD_HEADER_LENGTH) as u64;

                let log_config = LogConfig {
                    initial_offset: 0,
                    segment_config: SegmentConfig {
                        store_buffer_size: None,
                        max_store_bytes: record_size,
                    },
                    ..Default::default()
                };

                let mut log = glommio_segmented_log(STORAGE_DIR_PATH, log_config)
                    .await
                    .unwrap();

                assert!(matches!(
                    log.append(
                        RECORD_VALUE,
                        RecordMetadata {
                            offset: log.highest_offset() + 1,
                            additional_metadata: ()
                        }
                    )
                    .await,
                    Err(SegmentedLogError::OffsetOutOfBounds)
                ));

                let (offset_0, record_0_size) = log
                    .append(
                        RECORD_VALUE,
                        RecordMetadata {
                            offset: log.highest_offset(),
                            additional_metadata: (),
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(offset_0, log_config.initial_offset);
                assert_eq!(record_0_size as u64, record_size);
                // not enough bytes written to trigger sync
                matches!(log.read(offset_0).await, Err(LogError::SegmentError(_)));

                const NUM_RECORDS: u32 = 17;
                let mut prev_offset = offset_0;

                for _ in 1..NUM_RECORDS {
                    assert!(log.is_write_segment_maxed().unwrap());
                    // this write will trigger log rotation
                    let (curr_offset, _) = log
                        .append(
                            RECORD_VALUE,
                            RecordMetadata {
                                offset: log.highest_offset(),
                                additional_metadata: (),
                            },
                        )
                        .await
                        .unwrap();

                    let (record, next_record_offset) = log.read(prev_offset).await.unwrap();
                    assert_eq!(record.value.deref(), RECORD_VALUE);
                    assert_eq!(record.metadata.offset, prev_offset);
                    assert_eq!(next_record_offset, curr_offset);
                    prev_offset = curr_offset;
                }

                log.close().await.unwrap();

                let log: SegmentedLog<ReadResult, (), Store, SegmentCreator> =
                    glommio_segmented_log(STORAGE_DIR_PATH, log_config)
                        .await
                        .unwrap();

                {
                    let record_stream = commit_log_record_stream(&log, log.lowest_offset(), 0);
                    pin_mut!(record_stream);
                    let mut i = 0;
                    while let Some(record) = record_stream.next().await {
                        assert_eq!(record.value.deref(), RECORD_VALUE);
                        i += 1;
                    }
                    assert_eq!(i, NUM_RECORDS);
                }

                assert!(matches!(
                    log.read(log.highest_offset()).await,
                    Err(LogError::OffsetOutOfBounds)
                ));

                log.remove().await.unwrap();
                assert!(!PathBuf::from(STORAGE_DIR_PATH).exists());
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
                let record_size = bincode::serialized_size(&RecordMetadata::<()>::default())
                    .unwrap()
                    + (RECORD_VALUE.len() + RECORD_HEADER_LENGTH) as u64;

                let log_config = LogConfig {
                    initial_offset: 0,
                    segment_config: SegmentConfig {
                        store_buffer_size: None,
                        max_store_bytes: record_size,
                    },
                    ..Default::default()
                };

                let mut log = glommio_segmented_log(STORAGE_DIR_PATH, log_config)
                    .await
                    .unwrap();

                const NUM_RECORDS: u32 = 10;

                let mut base_offset_of_first_non_expired_segment = 0;

                for _ in 0..NUM_RECORDS / 2 {
                    // this write will trigger log rotation
                    (base_offset_of_first_non_expired_segment, _) = log
                        .append(
                            RECORD_VALUE,
                            RecordMetadata {
                                offset: log.highest_offset(),
                                additional_metadata: (),
                            },
                        )
                        .await
                        .unwrap();
                }

                let expiry_duration = Duration::from_millis(200);

                glommio::timer::sleep(expiry_duration).await;

                for _ in NUM_RECORDS / 2..NUM_RECORDS {
                    log.append(
                        RECORD_VALUE,
                        RecordMetadata {
                            offset: log.highest_offset(),
                            additional_metadata: (),
                        },
                    )
                    .await
                    .unwrap();
                }

                log.remove_expired_segments(expiry_duration).await.unwrap();

                assert_eq!(
                    log.lowest_offset(),
                    base_offset_of_first_non_expired_segment
                );

                log.read(log.lowest_offset()).await.unwrap();

                let log_highest_offset = log.highest_offset();

                // remove all segments
                log.remove_expired_segments(Duration::from_millis(0))
                    .await
                    .unwrap();

                assert_eq!(log.lowest_offset(), log_highest_offset);
                assert_eq!(log.lowest_offset(), log.highest_offset());

                log.remove().await.unwrap();
                assert!(!PathBuf::from(STORAGE_DIR_PATH).exists());
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
                let record_size = bincode::serialized_size(&RecordMetadata::<()>::default())
                    .unwrap()
                    + (RECORD_VALUE.len() + RECORD_HEADER_LENGTH) as u64;

                let log_config = LogConfig {
                    initial_offset: 0,
                    segment_config: SegmentConfig {
                        store_buffer_size: None,
                        max_store_bytes: record_size,
                    },
                    ..Default::default()
                };

                let mut log_0 = glommio_segmented_log(STORAGE_DIR_PATH, log_config)
                    .await
                    .unwrap();

                let mut log_1: SegmentedLog<ReadResult, (), Store, SegmentCreator> =
                    glommio_segmented_log(STORAGE_DIR_PATH, log_config)
                        .await
                        .unwrap();

                log_0
                    .append(
                        RECORD_VALUE,
                        RecordMetadata {
                            offset: log_0.highest_offset(),
                            additional_metadata: (),
                        },
                    )
                    .await
                    .unwrap(); // record written but not guranteed to be synced

                assert!(matches!(
                    log_1._advance_to_offset(log_0.highest_offset()).await,
                    Err(LogError::OffsetNotValidToAdvanceTo)
                ));

                log_0
                    .append(
                        RECORD_VALUE,
                        RecordMetadata {
                            offset: log_0.highest_offset(),
                            additional_metadata: (),
                        },
                    )
                    .await
                    .unwrap(); // first segment rotation
                let highest_offset_2 = log_0.highest_offset();

                assert!(matches!(
                    log_1._advance_to_offset(highest_offset_2).await,
                    Err(LogError::OffsetNotValidToAdvanceTo)
                ));

                log_0
                    .append(
                        RECORD_VALUE,
                        RecordMetadata {
                            offset: log_0.highest_offset(),
                            additional_metadata: (),
                        },
                    )
                    .await
                    .unwrap(); // second log rotation; 2nd segment synced

                log_1._advance_to_offset(highest_offset_2).await.unwrap();

                let final_highest_offset = log_0.highest_offset();

                log_0.close().await.unwrap(); // all segments guranteed to be synced

                log_1
                    ._advance_to_offset(final_highest_offset)
                    .await
                    .unwrap();

                log_1.close().await.unwrap();

                let log: SegmentedLog<ReadResult, (), Store, SegmentCreator> =
                    glommio_segmented_log(STORAGE_DIR_PATH, log_config)
                        .await
                        .unwrap();
                log.remove().await.unwrap();
                assert!(!PathBuf::from(STORAGE_DIR_PATH).exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }

    #[test]
    fn test_log_truncate_and_truncate_on_append() {
        const STORAGE_DIR_PATH: &str =
            "/tmp/laminarmq_log_test_log_truncate_and_truncate_on_append";
        if Path::new(STORAGE_DIR_PATH).exists() {
            fs::remove_dir_all(STORAGE_DIR_PATH).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                const RECORD_VALUE: &[u8] = b"Hello world!";
                let record_size = bincode::serialized_size(&RecordMetadata::<()>::default())
                    .unwrap()
                    + (RECORD_VALUE.len() + RECORD_HEADER_LENGTH) as u64;

                const INITIAL_OFFSET: u64 = 457;

                let log_config = LogConfig {
                    initial_offset: INITIAL_OFFSET,
                    segment_config: SegmentConfig {
                        store_buffer_size: None,
                        max_store_bytes: record_size,
                    },
                    truncate_on_append: true,
                };

                let mut log = glommio_segmented_log::<&str, ()>(STORAGE_DIR_PATH, log_config)
                    .await
                    .unwrap();

                const NUM_RECORDS: usize = 11;
                let mut record_offsets: [u64; NUM_RECORDS] = [0; NUM_RECORDS];

                let mut i = 0;
                while i < NUM_RECORDS {
                    let (offset, _) = log
                        .append(
                            RECORD_VALUE,
                            RecordMetadata {
                                offset: log.highest_offset(),
                                additional_metadata: (),
                            },
                        )
                        .await
                        .unwrap();
                    record_offsets[i] = offset;
                    i += 1;
                }

                assert!(record_offsets[NUM_RECORDS - 1] > 0);

                const RECORD_INDEX_TO_REAPPEND: usize = NUM_RECORDS * 2 / 3;

                let (new_record_offset, new_highest_offset) = (
                    record_offsets[RECORD_INDEX_TO_REAPPEND],
                    record_offsets[RECORD_INDEX_TO_REAPPEND + 1],
                );

                log.append(
                    RECORD_VALUE,
                    RecordMetadata {
                        offset: new_record_offset,
                        additional_metadata: (),
                    },
                )
                .await
                .unwrap();

                assert_eq!(log.highest_offset(), new_highest_offset);

                log.close().await.unwrap();

                let mut log = glommio_segmented_log::<&str, ()>(STORAGE_DIR_PATH, log_config)
                    .await
                    .unwrap();

                assert_eq!(log.highest_offset(), new_highest_offset);

                log.truncate(log.lowest_offset()).await.unwrap();

                assert_eq!(log.lowest_offset(), INITIAL_OFFSET);
                assert_eq!(log.highest_offset(), INITIAL_OFFSET);

                log.append(
                    RECORD_VALUE,
                    RecordMetadata {
                        offset: log.highest_offset(),
                        additional_metadata: (),
                    },
                )
                .await
                .unwrap();

                log.remove().await.unwrap();
            })
            .unwrap();

        local_ex.join().unwrap();
    }
}
