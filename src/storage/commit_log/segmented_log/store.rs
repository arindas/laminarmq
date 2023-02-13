use bytes::Buf;
use futures_core::Stream;
use num::FromPrimitive;

use self::common::{write_record_bytes, RecordHeader};

use super::super::super::Storage;
use std::{hash::Hasher, marker::PhantomData};

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

pub struct Store<S, H> {
    _storage: S,

    _phantom_data: PhantomData<H>,
}

impl<S, H> Store<S, H>
where
    S: Storage,
    H: Hasher + Default,
{
    pub async fn read(
        &mut self,
        position: &S::Position,
        record_header: &RecordHeader,
    ) -> Result<S::Content, ()> {
        let record_size = S::Size::from_u64(record_header.length).ok_or(())?;

        let record_bytes = self
            ._storage
            .read(position, &record_size)
            .await
            .map_err(|_| ())?;

        if &RecordHeader::compute::<H>(&record_bytes) != record_header {
            return Err(());
        }

        Ok(record_bytes)
    }

    pub async fn append<ReqBuf, ReqBody>(
        &mut self,
        stream: &mut ReqBody,
    ) -> Result<(S::Position, RecordHeader), ()>
    where
        ReqBuf: Buf,
        ReqBody: Stream<Item = ReqBuf> + Unpin,
    {
        self._storage
            .append(
                stream,
                &mut write_record_bytes::<H, ReqBuf, ReqBody, S::Write>,
            )
            .await
            .map_err(|_| ())
    }
}
