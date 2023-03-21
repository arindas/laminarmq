use self::common::RecordHeader;
use super::super::super::{AsyncConsume, AsyncTruncate, Sizable, Storage};
use async_trait::async_trait;
use futures_core::Stream;
use futures_lite::StreamExt;
use std::{error::Error as StdError, hash::Hasher, marker::PhantomData, ops::Deref};

pub mod common {
    use std::{
        hash::Hasher,
        io::{ErrorKind::UnexpectedEof, Read, Write},
    };

    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

    /// Extension used by backing files for [`Store`](super::Store) instances.
    pub const STORE_FILE_EXTENSION: &str = "store";

    /// Number of bytes required for storing the record header.
    pub const RECORD_HEADER_LENGTH: usize = 16;

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
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
pub enum StoreError<SE> {
    StorageError(SE),
    IncompatibleSizeType,
    RecordHeaderMismatch,
    ReadOnEmptyStore,
}

impl<SE> std::fmt::Display for StoreError<SE>
where
    SE: StdError,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<SE> StdError for StoreError<SE> where SE: StdError {}

macro_rules! u64_as_size {
    ($size:ident, $Size:ty) => {
        <$Size as num::FromPrimitive>::from_u64($size).ok_or(StoreError::IncompatibleSizeType)
    };

    ($size:literal, $Size:ty) => {
        <$Size as num::FromPrimitive>::from_u64($size).ok_or(StoreError::IncompatibleSizeType)
    };
}

macro_rules! size_as_u64 {
    ($size:ident, $Size:ty) => {
        <$Size as num::ToPrimitive>::to_u64(&$size).ok_or(StoreError::IncompatibleSizeType)
    };
}

impl<S, H> Store<S, H>
where
    S: Storage,
    H: Hasher + Default,
{
    pub async fn read(
        &self,
        position: &S::Position,
        record_header: &RecordHeader,
    ) -> Result<S::Content, StoreError<S::Error>> {
        if self.size() == u64_as_size!(0_u64, S::Size)? {
            return Err(StoreError::ReadOnEmptyStore);
        }

        let record_length = record_header.length;
        let record_size = u64_as_size!(record_length, S::Size)?;

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

    pub async fn append<XBuf, X, XE>(
        &mut self,
        stream: X,
        append_threshold: Option<S::Size>,
    ) -> Result<(S::Position, RecordHeader), StoreError<S::Error>>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let mut hasher = H::default();

        let mut stream = stream.map(|x| match x {
            Ok(x) => {
                hasher.write(&x);
                Ok(x)
            }
            Err(e) => Err(e),
        });

        let (position, bytes_written) = self
            .storage
            .append(&mut stream, append_threshold)
            .await
            .map_err(StoreError::StorageError)?;

        let record_header = RecordHeader {
            checksum: hasher.finish(),
            length: size_as_u64!(bytes_written, S::Size)?,
        };

        Ok((position, record_header))
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

pub(crate) mod test {
    use super::{
        super::super::super::{AsyncConsume, AsyncTruncate},
        RecordHeader, Storage, Store, StoreError,
    };
    use std::{convert::Infallible, future::Future, hash::Hasher, marker::PhantomData, ops::Deref};

    pub(crate) const _RECORDS: [&[u8; 129]; 20] = [
                    b"T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77T0fesa77t",
                    b"9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9Yxuipjd9YxuipjdD",
                    b"zjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMszjxEHzMsW",
                    b"9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqc9cOGqwqcw",
                    b"ZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7XcZXI6B7Xco",
                    b"9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nO9sjES6nOi",
                    b"KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3KZq1Egx3A",
                    b"cJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykcJQv6uykL",
                    b"6BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL76BKwSxL7O",
                    b"h5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKh5FxA3eKe",
                    b"DNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0NpcDNNs0Npc8",
                    b"6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOh6lHRDBOhu",
                    b"0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO0emonuBO6",
                    b"BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1BXn8YHM1V",
                    b"VWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcVWa0VnRcX",
                    b"RaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVRaiNfDSVc",
                    b"ujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7Pujz06A7PE",
                    b"6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs6q4fzIbs9",
                    b"28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcv28qu1qcvW",
                    b"j9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSj9AeJZGSD",
                ];

    pub(crate) async fn _test_store_read_append_truncate_consistency<SP, F, S, H>(
        storage_provider: SP,
    ) where
        F: Future<Output = (S, PhantomData<H>)>,
        SP: Fn() -> F,
        S: Storage,
        S::Position: num::Zero,
        H: Hasher + Default,
    {
        let mut store = Store::<S, H>::new(storage_provider().await.0);

        match store.read(&num::zero(), &RecordHeader::default()).await {
            Err(StoreError::ReadOnEmptyStore) => {}
            _ => unreachable!("Wrong result returned for read on empty store"),
        }

        let mut record_append_info_vec =
            Vec::<(S::Position, RecordHeader)>::with_capacity(_RECORDS.len());

        for record in _RECORDS {
            let record: &[u8] = record;

            let record_append_info = store
                .append(
                    futures_lite::stream::once(Ok::<&[u8], Infallible>(record)),
                    None,
                )
                .await
                .unwrap();

            record_append_info_vec.push(record_append_info);
        }

        let store = if S::is_persistent() {
            store.close().await.unwrap();
            Store::<S, H>::new(storage_provider().await.0)
        } else {
            store
        };

        for i in 0..record_append_info_vec.len() {
            let record_info = &record_append_info_vec[i];
            assert_eq!(
                store
                    .read(&record_info.0, &record_info.1)
                    .await
                    .unwrap()
                    .deref(),
                _RECORDS[i]
            );
        }

        let truncate_index = record_append_info_vec.len() / 2;
        let mut store = store;

        store
            .truncate(&record_append_info_vec[truncate_index].0)
            .await
            .unwrap();

        let mut i = 0;

        loop {
            let record_info = &record_append_info_vec[i];
            match store.read(&record_info.0, &record_info.1).await {
                Ok(record_content) => {
                    assert_eq!(record_content.deref(), _RECORDS[i]);
                }
                Err(_) => break,
            }

            i += 1;
        }

        assert_eq!(i, truncate_index);

        store.remove().await.unwrap();
    }
}
