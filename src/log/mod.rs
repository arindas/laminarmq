use async_trait::async_trait;

#[async_trait(?Send)]
pub trait Scanner {
    type Item;

    async fn next(&mut self) -> Option<Self::Item>;
}

pub mod store {
    use async_trait::async_trait;
    use std::{cell::Ref, marker::PhantomData, ops::Deref, path::Path, result::Result};

    use super::Scanner;

    #[async_trait(?Send)]
    pub trait Store<Record>
    where
        Record: Deref<Target = [u8]>,
    {
        type Error;
        type CloseResult;

        async fn append(&mut self, record_bytes: &[u8]) -> Result<(u64, usize), Self::Error>;

        async fn read(&self, position: u64) -> Result<Record, Self::Error>;

        async fn close(self) -> Result<Self::CloseResult, Self::Error>;

        fn size(&self) -> u64;

        fn path(&self) -> Result<Ref<Path>, Self::Error>;
    }

    pub struct StoreScanner<'a, Record, S>
    where
        Record: Deref<Target = [u8]>,
        S: Store<Record>,
    {
        store: &'a S,
        position: u64,

        _phantom_data: PhantomData<Record>,
    }

    impl<'a, Record, S> StoreScanner<'a, Record, S>
    where
        Record: Deref<Target = [u8]>,
        S: Store<Record>,
    {
        pub fn with_offset(store: &'a S, position: u64) -> Self {
            Self {
                store,
                position,
                _phantom_data: PhantomData,
            }
        }

        pub fn new(store: &'a S) -> Self {
            Self::with_offset(store, 0)
        }
    }

    #[async_trait(?Send)]
    impl<'a, Record, S> Scanner for StoreScanner<'a, Record, S>
    where
        Record: Deref<Target = [u8]> + Unpin,
        S: Store<Record>,
    {
        type Item = Record;

        async fn next(&mut self) -> Option<Self::Item> {
            if let Ok(record) = self.store.read(self.position).await {
                self.position += (record.len() + common::RECORD_HEADER_LENGTH) as u64;
                return Some(record);
            }

            None
        }
    }

    pub mod common {
        use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

        pub const STORE_FILE_EXTENSION: &str = "store";
        pub const RECORD_HEADER_LENGTH: usize = 8;

        pub struct RecordHeader {
            pub checksum: u32,
            pub length: u32,
        }

        impl RecordHeader {
            pub fn from_record_bytes(record_bytes: &[u8]) -> Self {
                Self {
                    checksum: crc32fast::hash(record_bytes),
                    length: record_bytes.len() as u32,
                }
            }

            pub fn from_bytes(record_header_bytes: &[u8]) -> std::io::Result<Self> {
                let mut cursor = std::io::Cursor::new(record_header_bytes);

                let checksum = cursor.read_u32::<LittleEndian>()?;
                let length = cursor.read_u32::<LittleEndian>()?;

                Ok(Self { checksum, length })
            }

            pub fn as_bytes(self) -> std::io::Result<[u8; RECORD_HEADER_LENGTH]> {
                let mut bytes = [0; RECORD_HEADER_LENGTH];

                let buffer: &mut [u8] = &mut bytes;
                let mut cursor = std::io::Cursor::new(buffer);

                cursor.write_u32::<LittleEndian>(self.checksum)?;
                cursor.write_u32::<LittleEndian>(self.length)?;

                Ok(bytes)
            }
        }

        #[inline]
        pub fn is_checksum_valid(bytes: &[u8], checksum: u32) -> bool {
            checksum == crc32fast::hash(bytes)
        }
    }
}

#[cfg(target_os = "linux")]
pub mod glommio_impl;
