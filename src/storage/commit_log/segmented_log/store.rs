use crate::common::split::SplitAt;

use super::super::super::{AsyncConsume, AsyncTruncate, Sizable};
use bytes::Buf;
use common::RecordHeader;
use futures_core::Stream;
use num::Unsigned;
use std::{hash::Hasher, ops::Deref};

#[async_trait::async_trait(?Send)]
pub trait Store:
    AsyncTruncate<Mark = Self::Position, TruncError = Self::Error>
    + AsyncConsume<ConsumeError = Self::Error>
    + Sizable
{
    /// Content bytes to be read from this store.
    type Content: Deref<Target = [u8]> + SplitAt<u8>;

    type ChesksumHasher: Hasher + Default;

    /// Represents a position in the underlying storage.
    type Position: Unsigned;

    /// The error type used by the methods of this trait.
    type Error: std::error::Error;

    /// Consumes the provided [`Stream`] by writing it to this [`Store`].
    /// This method computes the [`RecordHeader`] from the stream
    /// while writing it to this [`Store`].
    ///
    /// Returns the position at which the bytes were written along with
    /// the corresponding [`RecordHeader`] for the bytes.
    async fn append<B, S>(
        &mut self,
        stream: &mut S,
    ) -> Result<(Self::Position, RecordHeader), Self::Error>
    where
        B: Buf,
        S: Stream<Item = B> + Unpin;

    /// Reads the record bytes content for the given [`RecordHeader`].
    /// This method validates the read content with the given
    /// [`RecordHeader`] before returning it.
    async fn read(
        &self,
        position: &Self::Position,
        record_header: &RecordHeader,
    ) -> Result<Self::Content, Self::Error>;
}

pub mod common {
    use std::{
        hash::Hasher,
        io::{ErrorKind::UnexpectedEof, Read, Write},
    };

    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use bytes::Buf;

    use futures_core::Stream;
    use futures_lite::{AsyncWrite, AsyncWriteExt, StreamExt};

    /// Extension used by backing files for [`Store`](super::Store)s.
    pub const STORE_FILE_EXTENSION: &str = "store";

    /// Number of bytes required for storing the record header.
    pub const RECORD_HEADER_LENGTH: usize = 16;

    #[derive(Debug, Default, PartialEq, Eq)]
    pub struct RecordHeader {
        pub checksum: u64,
        pub length: u64,
    }

    impl RecordHeader {
        pub fn read<R: Read>(source: &mut R) -> std::io::Result<RecordHeader> {
            let checksum = source.read_u64::<LittleEndian>()?;
            let length = source.read_u64::<LittleEndian>()?;

            if checksum == 0 && length == 0 {
                Err(std::io::Error::from(UnexpectedEof))
            } else {
                Ok(Self { checksum, length })
            }
        }

        pub fn write<W: Write>(&self, dest: &mut W) -> std::io::Result<()> {
            dest.write_u64::<LittleEndian>(self.checksum)?;
            dest.write_u64::<LittleEndian>(self.length as u64)?;

            Ok(())
        }

        pub fn compute<H>(record_bytes: &[u8]) -> Self
        where
            H: Hasher + Default,
        {
            let mut hasher = H::default();
            hasher.write(record_bytes);
            let checksum = hasher.finish();

            RecordHeader {
                checksum,
                length: record_bytes.len() as u64,
            }
        }
    }

    pub async fn write_record_bytes<H, B, S, W>(
        buf_stream: &mut S,
        writer: &mut W,
    ) -> std::io::Result<RecordHeader>
    where
        B: Buf,
        S: Stream<Item = B> + Unpin,
        W: AsyncWrite + Unpin,
        H: Hasher + Default,
    {
        let (mut hasher, mut length) = (H::default(), 0 as usize);
        while let Some(mut buf) = buf_stream.next().await {
            while buf.has_remaining() {
                let chunk = buf.chunk();

                writer.write_all(chunk).await?;
                hasher.write(chunk);

                let chunk_len = chunk.len();
                buf.advance(chunk_len);
                length += chunk_len;
            }
        }

        Ok(RecordHeader {
            checksum: hasher.finish(),
            length: length as u64,
        })
    }
}
