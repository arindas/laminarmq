pub mod common {
    use std::io::ErrorKind::UnexpectedEof;

    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use bytes::Buf;

    use futures_core::Stream;
    use futures_lite::{AsyncWrite, AsyncWriteExt, StreamExt};

    /// Extension used by backing files for [`Store`](super::Store)s.
    pub const STORE_FILE_EXTENSION: &str = "store";

    /// Number of bytes required for storing the record header.
    pub const RECORD_HEADER_LENGTH: usize = 8;

    pub struct RecordHeader {
        pub checksum: u32,
        pub length: u32,
    }

    impl RecordHeader {
        /// Creates a [`RecordHeader`] instance from serialized record header bytes.
        /// This method internally users a [`std::io::Cursor`] to read the checksum and
        /// length as integers from the given bytes with little endian encoding.
        pub fn from_bytes_le(record_header_bytes: &[u8]) -> std::io::Result<RecordHeader> {
            let mut cursor = std::io::Cursor::new(record_header_bytes);

            let checksum = cursor.read_u32::<LittleEndian>()?;
            let length = cursor.read_u32::<LittleEndian>()?;

            if checksum == 0 && length == 0 {
                Err(std::io::Error::from(UnexpectedEof))
            } else {
                Ok(Self { checksum, length })
            }
        }

        /// Serializes this given record header to an owned byte array.
        /// This method internally use a [`std::io::Cursor`] to write the checksum and length
        /// fields as little endian integers into the byte array.
        pub fn as_bytes(self) -> std::io::Result<[u8; RECORD_HEADER_LENGTH]> {
            let mut bytes = [0; RECORD_HEADER_LENGTH];

            let buffer: &mut [u8] = &mut bytes;
            let mut cursor = std::io::Cursor::new(buffer);

            cursor.write_u32::<LittleEndian>(self.checksum)?;
            cursor.write_u32::<LittleEndian>(self.length)?;

            Ok(bytes)
        }

        /// States whether this [`RecordHeader`] is valid for the given record bytes. This
        /// method internally checks if the checksum and length is valid for the given slice.
        #[inline]
        pub fn valid_for_record_bytes(&self, record_bytes: &[u8]) -> bool {
            self.length as usize == record_bytes.len()
                && self.checksum == crc32fast::hash(record_bytes)
        }
    }

    impl TryFrom<RecordHeader> for [u8; RECORD_HEADER_LENGTH] {
        type Error = std::io::Error;

        fn try_from(value: RecordHeader) -> Result<Self, Self::Error> {
            value.as_bytes()
        }
    }

    impl TryFrom<&[u8]> for RecordHeader {
        type Error = std::io::Error;

        fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
            RecordHeader::from_bytes_le(value)
        }
    }

    pub async fn write_record_bytes<B, S, W>(
        buf_stream: &mut S,
        writer: &mut W,
    ) -> std::io::Result<RecordHeader>
    where
        B: Buf,
        S: Stream<Item = B> + Unpin,
        W: AsyncWrite + Unpin,
    {
        let (mut hasher, mut length) = (crc32fast::Hasher::new(), 0 as usize);
        while let Some(mut buf) = buf_stream.next().await {
            while buf.has_remaining() {
                let chunk = buf.chunk();

                writer.write_all(chunk).await?;
                hasher.update(chunk);

                let chunk_len = chunk.len();
                buf.advance(chunk_len);
                length += chunk_len;
            }
        }

        Ok(RecordHeader {
            checksum: hasher.finalize(),
            length: length as u32,
        })
    }
}
