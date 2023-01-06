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
                    .write(&bincode::serialize(&hierarchy).unwrap_or(vec![]))
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
                writer.write(record.value.deref()).await?;
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
