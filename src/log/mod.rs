pub mod base {
    use async_trait::async_trait;
    use futures_lite::{FutureExt, Stream};
    use std::{
        cell::Ref, marker::PhantomData, ops::Deref, path::Path, pin::Pin, result::Result,
        task::Poll,
    };

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

    pub struct StoreStreamer<'a, Record, S>
    where
        Record: Deref<Target = [u8]>,
        S: Store<Record>,
    {
        store: &'a S,
        position: u64,

        _phantom_data: PhantomData<Record>,
    }

    impl<'a, Record, S> StoreStreamer<'a, Record, S>
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

    impl<'a, Record, S> Stream for StoreStreamer<'a, Record, S>
    where
        Record: Deref<Target = [u8]> + Unpin,
        S: Store<Record>,
    {
        type Item = Record;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            let self_inner = Pin::into_inner(self);

            match self_inner.store.read(self_inner.position).poll(cx) {
                Poll::Ready(Ok(record)) => {
                    self_inner.position += record.len() as u64;
                    Poll::Ready(Some(record))
                }
                Poll::Ready(Err(_)) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

pub use self::base::*;
