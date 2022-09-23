//! Module providing specialization for [`Segment`](`crate::commit_log::segment::Segment`) for the
//! [`glommio`] runtime.

use std::path::Path;

use glommio::io::ReadResult;

use crate::commit_log::segment::{config::SegmentConfig, Segment, SegmentError};

use super::store::Store;

type Error = SegmentError<ReadResult, Store>;

impl Segment<ReadResult, Store> {
    /// Creates a new [`Segment`] specialized for the [`glommio`] runtime with a [`Store`] at the
    /// given path with the given base offset and config.
    ///
    /// ## Errors
    /// - [`SegmentError::StoreError`]: if there is an error in creating the store.
    pub async fn new<P: AsRef<Path>>(
        path: P,
        base_offset: u64,
        config: SegmentConfig,
    ) -> Result<Self, Error> {
        Ok(Self::with_config_base_offset_and_store(
            config,
            base_offset,
            Store::with_path_and_buffer_size(path, config.store_buffer_size)
                .await
                .map_err(SegmentError::StoreError)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use glommio::{LocalExecutorBuilder, Placement};

    use crate::commit_log::{
        segment::SegmentScanner,
        store::common::{bincoded_serialized_record_size, STORE_FILE_EXTENSION},
        Record, Scanner,
    };

    use super::{Segment, SegmentConfig, SegmentError};

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
                let segment = Segment::new(
                    test_file_path.clone(),
                    0,
                    SegmentConfig {
                        store_buffer_size: 512,
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
                let mut record = Record {
                    value: Vec::from(RECORD_VALUE),
                    offset: 0,
                };
                let record_representation_size = bincoded_serialized_record_size(&record).unwrap();
                let expected_segment_size: u64 = 2 * record_representation_size;

                let mut segment = Segment::new(
                    test_file_path.clone(),
                    0,
                    SegmentConfig {
                        store_buffer_size: 512,
                        max_store_bytes: expected_segment_size,
                    },
                )
                .await
                .unwrap();

                assert!(matches!(
                    segment.read(segment.next_offset()).await,
                    Err(SegmentError::OffsetOutOfBounds)
                ));

                let offset_1 = segment.append(&mut record).await.unwrap();
                assert_eq!(offset_1, 0);
                assert_eq!(segment.next_offset(), record_representation_size);

                assert!(matches!(
                    segment.read(segment.next_offset()).await,
                    Err(SegmentError::OffsetOutOfBounds)
                ));

                assert!(matches!(
                    segment.advance_to_offset(segment.next_offset()),
                    Ok(_)
                ));

                let offset_2 = segment.append(&mut record).await.unwrap();
                assert_eq!(offset_2, record_representation_size);

                assert_eq!(segment.size(), expected_segment_size);
                assert!(segment.is_maxed());

                // close segment to ensure that the records are presisted
                segment.close().await.unwrap();

                let mut segment = Segment::new(
                    test_file_path.clone(),
                    0,
                    SegmentConfig {
                        store_buffer_size: 512,
                        max_store_bytes: expected_segment_size,
                    },
                )
                .await
                .unwrap();

                assert_eq!(segment.size(), expected_segment_size);
                assert!(segment.is_maxed());

                assert!(matches!(
                    segment.append(&mut record).await,
                    Err(SegmentError::SegmentMaxed)
                ));

                let (record_1, record_1_next_record_offset) = segment.read(offset_1).await.unwrap();
                assert_eq!(record_1.offset, offset_1);
                assert_eq!(record_1.value, RECORD_VALUE);
                assert_eq!(record_1_next_record_offset, offset_2);

                let (record_2, record_2_next_record_offset) = segment.read(offset_2).await.unwrap();
                assert_eq!(record_2.offset, offset_2);
                assert_eq!(record_2.value, RECORD_VALUE);
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

                assert!(matches!(
                    segment.advance_to_offset(segment.next_offset() + 1),
                    Err(SegmentError::OffsetBeyondCapacity)
                ));

                let records = vec![record_1, record_2];

                let mut segment_scanner = SegmentScanner::new(&segment).unwrap();
                let mut i = 0;
                while let Some(record) = segment_scanner.next().await {
                    assert_eq!(record, records[i]);

                    i += 1;
                }
                assert_eq!(i, records.len());

                segment.remove().await.unwrap();

                assert!(!test_file_path.exists());
            })
            .unwrap();
        local_ex.join().unwrap();
    }
}
