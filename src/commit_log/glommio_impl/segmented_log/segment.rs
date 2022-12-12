//! Module providing specialization for [`Segment`](`crate::commit_log::segmented_log::segment::Segment`)
//! for the [`glommio`] runtime.

use std::path::Path;

use glommio::io::ReadResult;

use crate::commit_log::segmented_log::segment::{config::SegmentConfig, Segment, SegmentError};

use super::store::{Store, DEFAULT_STORE_WRITER_BUFFER_SIZE};

pub async fn glommio_segment<M: Default + serde::Serialize + serde::de::DeserializeOwned>(
    path: impl AsRef<Path>,
    base_offset: u64,
    config: SegmentConfig,
) -> Result<Segment<ReadResult, M, Store>, SegmentError<ReadResult, Store>> {
    Ok(Segment::with_config_base_offset_and_store(
        config,
        base_offset,
        Store::with_path_and_buffer_size(
            path,
            config
                .store_buffer_size
                .unwrap_or(DEFAULT_STORE_WRITER_BUFFER_SIZE),
        )
        .await
        .map_err(SegmentError::StoreError)?,
    ))
}

#[cfg(test)]
mod tests {
    use std::{fs, ops::Deref, path::PathBuf};

    use futures_lite::StreamExt;
    use futures_util::pin_mut;
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::commit_log::{
        segmented_log::{
            segment::{config::SegmentConfig, segment_record_stream, SegmentError},
            store::common::RECORD_HEADER_LENGTH,
        },
        segmented_log::{store::common::STORE_FILE_EXTENSION, RecordMetadata},
    };

    use super::glommio_segment;

    #[inline]
    fn test_file_path_string(test_name: &str) -> String {
        format!(
            "/tmp/laminarmq_test_log_segment_{}.{}",
            test_name, STORE_FILE_EXTENSION
        )
    }

    #[test]
    fn test_segment_new_and_remove() {
        let test_file_path = PathBuf::from(test_file_path_string("test_segment_new_and_remove"));

        if test_file_path.exists() {
            fs::remove_file(&test_file_path).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spawn(move || async move {
                let segment = glommio_segment::<()>(
                    &test_file_path,
                    0,
                    SegmentConfig {
                        store_buffer_size: None,
                        max_store_bytes: 512,
                    },
                )
                .await
                .unwrap();

                segment.remove().await.unwrap();

                assert!(!test_file_path.exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }

    #[test]
    fn test_segment_reads_reflect_appends() {
        let test_file_path =
            PathBuf::from(test_file_path_string("test_segment_reads_reflect_appends"));

        if test_file_path.exists() {
            fs::remove_file(&test_file_path).unwrap();
        }

        let local_ex = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spawn(move || async move {
                const RECORD_VALUE: &[u8] = b"Hello World!";
                let record_representation_size =
                    bincode::serialized_size(&RecordMetadata::<()>::default()).unwrap()
                        + (RECORD_VALUE.len() + RECORD_HEADER_LENGTH) as u64;
                let expected_segment_size: u64 = 2 * record_representation_size;

                let mut segment = glommio_segment(
                    &test_file_path,
                    0,
                    SegmentConfig {
                        store_buffer_size: None,
                        max_store_bytes: expected_segment_size,
                    },
                )
                .await
                .unwrap();

                assert!(matches!(
                    segment.read(segment.next_offset()).await,
                    Err(SegmentError::OffsetOutOfBounds)
                ));

                let (offset_1, record_1_size) = segment
                    .append(
                        RECORD_VALUE,
                        RecordMetadata {
                            offset: segment.next_offset(),
                            additional_metadata: (),
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(offset_1, 0);
                assert_eq!(record_1_size as u64, record_representation_size);
                assert_eq!(segment.next_offset(), record_representation_size);

                assert!(matches!(
                    segment.read(segment.next_offset()).await,
                    Err(SegmentError::OffsetOutOfBounds)
                ));

                assert!(matches!(
                    segment._advance_to_offset(segment.next_offset()),
                    Ok(_)
                ));

                let (offset_2, _) = segment
                    .append(
                        RECORD_VALUE,
                        RecordMetadata {
                            offset: segment.next_offset(),
                            additional_metadata: (),
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(offset_2, record_representation_size);

                assert_eq!(segment.size(), expected_segment_size);
                assert!(segment.is_maxed());

                // close segment to ensure that the records are presisted
                segment.close().await.unwrap();

                let mut segment = glommio_segment(
                    &test_file_path,
                    0,
                    SegmentConfig {
                        store_buffer_size: None,
                        max_store_bytes: expected_segment_size,
                    },
                )
                .await
                .unwrap();

                assert_eq!(segment.size(), expected_segment_size);
                assert!(segment.is_maxed());

                assert!(matches!(
                    segment
                        .append(
                            RECORD_VALUE,
                            RecordMetadata {
                                offset: segment.next_offset(),
                                additional_metadata: ()
                            }
                        )
                        .await,
                    Err(SegmentError::SegmentMaxed)
                ));

                let (record_1, record_1_next_record_offset) = segment.read(offset_1).await.unwrap();
                assert_eq!(record_1.metadata.offset, offset_1);
                assert_eq!(record_1.value.deref(), RECORD_VALUE);
                assert_eq!(record_1_next_record_offset, offset_2);

                let (record_2, record_2_next_record_offset) = segment.read(offset_2).await.unwrap();
                assert_eq!(record_2.metadata.offset, offset_2);
                assert_eq!(record_2.value.deref(), RECORD_VALUE);
                assert_eq!(record_2_next_record_offset, segment.next_offset());

                // read at invalid loacation
                assert!(matches!(
                    segment.read(offset_2 + 1).await,
                    Err(SegmentError::StoreError(_))
                ));

                assert!(matches!(
                    segment.read(segment.next_offset()).await,
                    Err(SegmentError::OffsetOutOfBounds)
                ));

                let records = vec![record_1, record_2];

                {
                    let record_stream = segment_record_stream(&segment, segment.base_offset());
                    pin_mut!(record_stream);
                    let mut i = 0;
                    while let Some(record) = record_stream.next().await {
                        assert_eq!(record.metadata.offset, records[i].metadata.offset);
                        assert_eq!(record.value.deref(), records[i].value.deref());

                        i += 1;
                    }
                    assert_eq!(i, records.len());
                }

                assert!(matches!(
                    segment._advance_to_offset(segment.next_offset() + 1),
                    Err(SegmentError::OffsetBeyondCapacity)
                ));

                assert!(matches!(
                    segment.truncate(segment.next_offset()).await,
                    Err(SegmentError::OffsetOutOfBounds)
                ));

                assert!(matches!(
                    segment.truncate(records[0].metadata.offset + 1).await,
                    Err(SegmentError::StoreError(_))
                ));

                segment
                    .truncate(records.last().unwrap().metadata.offset)
                    .await
                    .unwrap();

                {
                    let record_stream = segment_record_stream(&segment, segment.base_offset());
                    pin_mut!(record_stream);
                    let mut i = 0;
                    while let Some(record) = record_stream.next().await {
                        assert_eq!(record.metadata.offset, records[i].metadata.offset);
                        assert_eq!(record.value.deref(), records[i].value.deref());

                        i += 1;
                    }
                    assert_eq!(i, records.len() - 1);
                }

                segment.truncate(segment.base_offset()).await.unwrap();
                assert_eq!(segment.size(), 0);

                segment.remove().await.unwrap();

                assert!(!test_file_path.exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }
}
