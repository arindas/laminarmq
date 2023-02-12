use std::io::{Read, Seek, SeekFrom, Write};

use bytes::Buf;
use futures_core::{Future, Stream};

pub struct Storage<RWS> {
    _storage: RWS,
    _size: u64,
}

impl<RWS> Storage<RWS>
where
    RWS: Read + Write + Seek,
{
    fn _read_at(&mut self, position: u64, size: usize) -> Vec<u8> {
        let storage = &mut self._storage;
        storage.seek(SeekFrom::Start(position)).unwrap();
        let mut buf = Vec::with_capacity(size);
        storage.take(size as u64).read_to_end(&mut buf).unwrap();
        buf
    }

    async fn _append<B, S, W, F, T>(
        &mut self,
        byte_stream: &mut S,
        write_fn: &mut W,
    ) -> Result<(u64, T), ()>
    where
        B: Buf,
        S: Stream<Item = B> + Unpin,
        W: FnMut(&mut S, &mut RWS) -> F,
        F: Future<Output = std::io::Result<T>>,
    {
        let storage = &mut self._storage;

        storage.seek(SeekFrom::End(0)).unwrap();

        let t = write_fn(byte_stream, storage).await.unwrap();

        storage.seek(SeekFrom::End(0)).unwrap();

        Ok((storage.stream_position().unwrap(), t))
    }
}
