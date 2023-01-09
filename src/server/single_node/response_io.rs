//! Module providing utilities for I/O operations with [`Response`](super::Response) instances.

use super::{RequestKind, Response};
use crate::{
    commit_log::{segmented_log::RecordMetadata, Record},
    server::tokio_compat::common::read_exact,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    io::{self, ErrorKind::Other},
    ops::Deref,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

impl<T: Deref<Target = [u8]>> Response<T> {
    /// Writes this [`Response`] instance to the given [`AsyncWrite`] instance.
    pub async fn write<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Self::PartitionHierachy(hierarchy) => {
                writer
                    .write_u8(RequestKind::PartitionHierachy as u8)
                    .await?;
                writer
                    .write_u64(
                        bincode::serialized_size(&hierarchy)
                            .map_err(|_| io::Error::new(Other, "serialized size not obtained"))?,
                    )
                    .await?;
                writer
                    .write_all(
                        &bincode::serialize(&hierarchy)
                            .map_err(|_| io::Error::new(Other, "bincode serialization failed"))?,
                    )
                    .await?;
            }

            Self::Read {
                record,
                next_offset,
            } => {
                writer.write_u8(RequestKind::Read as u8).await?;
                writer.write_u64(record.metadata.offset).await?;
                writer.write_u64(*next_offset).await?;
                writer.write_u64(record.value.len() as u64).await?;
                writer.write_all(record.value.deref()).await?;
            }

            Self::LowestOffset(lowest_offset) => {
                writer.write_u8(RequestKind::LowestOffset as u8).await?;
                writer.write_u64(*lowest_offset).await?;
            }

            Self::HighestOffset(highest_offset) => {
                writer.write_u8(RequestKind::HighestOffset as u8).await?;
                writer.write_u64(*highest_offset).await?;
            }

            Self::Append {
                write_offset,
                bytes_written,
            } => {
                writer.write_u8(RequestKind::Append as u8).await?;
                writer.write_u64(*write_offset).await?;
                writer.write_u64(*bytes_written as u64).await?;
            }

            Self::ExpiredRemoved => writer.write_u8(RequestKind::RemoveExpired as u8).await?,
            Self::PartitionCreated => writer.write_u8(RequestKind::CreatePartition as u8).await?,
            Self::PartitionRemoved => writer.write_u8(RequestKind::RemovePartition as u8).await?,
        }

        Ok(())
    }
}

impl Response<Vec<u8>> {
    /// Reads a [`Response<Vec<u8>>`] from the given [`AsyncRead`] instance.
    pub async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let request_kind: RequestKind = reader
            .read_u8()
            .await?
            .try_into()
            .map_err(|_| io::Error::new(Other, "invalid request kind"))?;

        match request_kind {
            RequestKind::PartitionHierachy => {
                let content_len = reader.read_u64().await?;
                let content_bytes = read_exact(reader, content_len).await?;

                let hierarchy: HashMap<Cow<'static, str>, Vec<u64>> =
                    bincode::deserialize(&content_bytes)
                        .map_err(|_| io::Error::new(Other, "hashmap deserialization failed"))?;

                Ok(Self::PartitionHierachy(hierarchy))
            }
            RequestKind::Read => {
                let record_offset = reader.read_u64().await?;
                let next_offset = reader.read_u64().await?;

                let record_len = reader.read_u64().await?;
                let record_bytes = read_exact(reader, record_len).await?;

                Ok(Self::Read {
                    record: Record {
                        metadata: RecordMetadata {
                            offset: record_offset,
                            additional_metadata: (),
                        },
                        value: record_bytes,
                    },
                    next_offset,
                })
            }
            RequestKind::LowestOffset => Ok(Self::LowestOffset(reader.read_u64().await?)),
            RequestKind::HighestOffset => Ok(Self::HighestOffset(reader.read_u64().await?)),
            RequestKind::Append => {
                let write_offset = reader.read_u64().await?;
                let bytes_written = reader.read_u64().await? as usize;

                Ok(Self::Append {
                    write_offset,
                    bytes_written,
                })
            }
            RequestKind::RemoveExpired => Ok(Self::ExpiredRemoved),
            RequestKind::CreatePartition => Ok(Self::PartitionCreated),
            RequestKind::RemovePartition => Ok(Self::PartitionRemoved),
        }
    }
}

#[cfg(test)]
mod tests {
    use byteorder::WriteBytesExt;

    use crate::{
        commit_log::segmented_log::RecordMetadata, prelude::Record, server::single_node::Response,
    };
    use std::{borrow::Cow, collections::HashMap, io::Cursor};

    #[test]
    fn test_response_io() {
        futures_lite::future::block_on(async {
            let mut backing_storage = Cursor::new(Vec::<u8>::new());

            let old_position = backing_storage.position();

            const MESSAGE: &[u8] = b"Hello World!";
            const OFFSET: u64 = 107;
            const NEXT_OFFSET: u64 = 68419;

            let response = Response::Read {
                record: Record {
                    metadata: RecordMetadata {
                        offset: OFFSET,
                        additional_metadata: (),
                    },
                    value: MESSAGE,
                },
                next_offset: NEXT_OFFSET,
            };

            response.write(&mut backing_storage).await.unwrap();

            let position = backing_storage.position();
            backing_storage.set_position(old_position);

            if let Response::Read {
                record,
                next_offset,
            } = Response::read(&mut backing_storage).await.unwrap()
            {
                assert_eq!(record.metadata.offset, OFFSET);
                assert_eq!(record.value, MESSAGE);
                assert_eq!(next_offset, NEXT_OFFSET);
            } else {
                assert!(false, "Wrong response type read.");
            }

            backing_storage.set_position(position);

            let old_position = backing_storage.position();

            let baseline_partition_hierarchy: HashMap<Cow<'static, str>, Vec<u64>> =
                HashMap::from([("topic_1".into(), vec![1, 2]), ("topic_2".into(), vec![3])]);

            let response = Response::<&[u8]>::PartitionHierachy(baseline_partition_hierarchy);
            response.write(&mut backing_storage).await.unwrap();

            let position = backing_storage.position();
            backing_storage.set_position(old_position);

            if let Response::PartitionHierachy(hierarchy) =
                Response::read(&mut backing_storage).await.unwrap()
            {
                assert_eq!(hierarchy.get("topic_1").unwrap(), &[1, 2]);
                assert_eq!(hierarchy.get("topic_2").unwrap(), &[3]);
            } else {
                assert!(false, "Wrong response type read.");
            }

            backing_storage.set_position(position);

            let old_position = backing_storage.position();

            const LOWEST_OFFSET: u64 = 107;
            let response = Response::<&[u8]>::LowestOffset(LOWEST_OFFSET);
            response.write(&mut backing_storage).await.unwrap();

            let position = backing_storage.position();
            backing_storage.set_position(old_position);

            if let Response::LowestOffset(lowest_offset) =
                Response::read(&mut backing_storage).await.unwrap()
            {
                assert_eq!(lowest_offset, LOWEST_OFFSET);
            } else {
                assert!(false, "Wrong response type received");
            }

            backing_storage.set_position(position);

            let old_position = backing_storage.position();

            const HIGHEST_OFFSET: u64 = 68419;
            let response = Response::<&[u8]>::HighestOffset(HIGHEST_OFFSET);
            response.write(&mut backing_storage).await.unwrap();

            let position = backing_storage.position();
            backing_storage.set_position(old_position);

            if let Response::HighestOffset(highest_offset) =
                Response::read(&mut backing_storage).await.unwrap()
            {
                assert_eq!(highest_offset, HIGHEST_OFFSET);
            } else {
                assert!(false, "Wrong response type received");
            }

            backing_storage.set_position(position);

            let old_position = backing_storage.position();

            const WRITE_OFFSET: u64 = 68419;
            const BYTES_WRITTEN: usize = 107;
            let response = Response::<&[u8]>::Append {
                write_offset: WRITE_OFFSET,
                bytes_written: BYTES_WRITTEN,
            };
            response.write(&mut backing_storage).await.unwrap();

            let position = backing_storage.position();
            backing_storage.set_position(old_position);

            if let Response::Append {
                write_offset,
                bytes_written,
            } = Response::read(&mut backing_storage).await.unwrap()
            {
                assert_eq!(write_offset, WRITE_OFFSET);
                assert_eq!(bytes_written, BYTES_WRITTEN);
            } else {
                assert!(false, "Wrong response type received");
            }

            backing_storage.set_position(position);

            let old_position = backing_storage.position();

            const RESPONSES: [Response<&[u8]>; 3] = [
                Response::<&[u8]>::ExpiredRemoved,
                Response::<&[u8]>::PartitionCreated,
                Response::<&[u8]>::PartitionRemoved,
            ];

            for response in &RESPONSES {
                response.write(&mut backing_storage).await.unwrap();
            }

            let position = backing_storage.position();
            backing_storage.set_position(old_position);

            for i in 0..RESPONSES.len() {
                let response = Response::read(&mut backing_storage).await.unwrap();
                match (response, &RESPONSES[i]) {
                    (Response::ExpiredRemoved, &Response::ExpiredRemoved) => {}
                    (Response::PartitionCreated, &Response::PartitionCreated) => {}
                    (Response::PartitionRemoved, &Response::PartitionRemoved) => {}
                    _ => assert!(false, "Wrong response type received"),
                }
            }

            backing_storage.set_position(position);

            let old_position = backing_storage.position();
            backing_storage.write_u8(70).unwrap();

            backing_storage.set_position(old_position);

            assert!(Response::read(&mut backing_storage).await.is_err());
        });
    }
}
