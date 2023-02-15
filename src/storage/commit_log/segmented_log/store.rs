use self::common::{write_record_bytes, RecordHeader};
use super::super::super::{AsyncConsume, AsyncTruncate, Sizable, Storage};
use async_trait::async_trait;
use bytes::Buf;
use futures_core::Stream;
use num::FromPrimitive;
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

    /// Extension used by backing files for [`Store`](super::Store) instances.
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
            dest.write_u64::<LittleEndian>(self.length)?;

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
    storage: S,

    _phantom_data: PhantomData<H>,
}

impl<S: Default, H> Default for Store<S, H> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S, H> Store<S, H> {
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            _phantom_data: PhantomData,
        }
    }
}

#[derive(Debug)]
pub enum StoreError<StorageError> {
    StorageError(StorageError),
    IncompatibleSizeType,
    RecordHeaderMismatch,
}

impl<StorageError> std::fmt::Display for StoreError<StorageError>
where
    StorageError: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<StorageError> std::error::Error for StoreError<StorageError> where
    StorageError: std::error::Error
{
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
    ) -> Result<S::Content, StoreError<S::Error>> {
        let record_size =
            S::Size::from_u64(record_header.length).ok_or(StoreError::IncompatibleSizeType)?;

        let record_bytes = self
            .storage
            .read(position, &record_size)
            .await
            .map_err(StoreError::StorageError)?;

        if &RecordHeader::compute::<H>(&record_bytes) != record_header {
            return Err(StoreError::RecordHeaderMismatch);
        }

        Ok(record_bytes)
    }

    pub async fn append<ReqBuf, ReqBody>(
        &mut self,
        stream: &mut ReqBody,
    ) -> Result<(S::Position, RecordHeader), StoreError<S::Error>>
    where
        ReqBuf: Buf,
        ReqBody: Stream<Item = ReqBuf> + Unpin,
    {
        self.storage
            .append(
                stream,
                &mut write_record_bytes::<H, ReqBuf, ReqBody, S::Write>,
            )
            .await
            .map_err(StoreError::StorageError)
    }
}

#[async_trait(?Send)]
impl<S: Storage, H> AsyncTruncate for Store<S, H> {
    type Mark = S::Mark;

    type TruncError = StoreError<S::Error>;

    async fn truncate(&mut self, pos: &Self::Mark) -> Result<(), Self::TruncError> {
        self.storage
            .truncate(pos)
            .await
            .map_err(StoreError::StorageError)
    }
}

#[async_trait(?Send)]
impl<S: Storage, H> AsyncConsume for Store<S, H> {
    type ConsumeError = StoreError<S::Error>;

    async fn remove(self) -> Result<(), Self::ConsumeError> {
        self.storage
            .remove()
            .await
            .map_err(StoreError::StorageError)
    }

    async fn close(self) -> Result<(), Self::ConsumeError> {
        self.storage.close().await.map_err(StoreError::StorageError)
    }
}

impl<S: Storage, H> Sizable for Store<S, H> {
    type Size = S::Size;

    fn size(&self) -> Self::Size {
        self.storage.size()
    }
}
