pub mod common {
    use bytes::Buf;
    use futures_core::Stream;
    use futures_lite::{AsyncWrite, AsyncWriteExt, StreamExt};

    pub struct RecordHeader {
        pub checksum: u32,
        pub length: u32,
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
