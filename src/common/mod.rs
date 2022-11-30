pub mod cache;

pub mod borrow {
    use bytes::Bytes;
    use std::{borrow::Cow, ops::Deref};

    #[derive(Debug)]
    pub enum BytesCow<'borrowed> {
        Owned(Bytes),
        Borrowed(Cow<'borrowed, [u8]>),
    }

    impl<'borrowed> From<Cow<'borrowed, [u8]>> for BytesCow<'borrowed> {
        fn from(cow: Cow<'borrowed, [u8]>) -> Self {
            match cow {
                Cow::Borrowed(_) => BytesCow::Borrowed(cow),
                Cow::Owned(owned_bytes) => BytesCow::Owned(Bytes::from(owned_bytes)),
            }
        }
    }

    impl<'borrowed> Into<Cow<'borrowed, [u8]>> for &BytesCow<'borrowed> {
        fn into(self) -> Cow<'borrowed, [u8]> {
            match self {
                BytesCow::Owned(x) => Into::<Vec<u8>>::into(x.clone()).into(),
                BytesCow::Borrowed(cow) => cow.clone(),
            }
        }
    }

    impl<'borrowed> Deref for BytesCow<'borrowed> {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            match self {
                BytesCow::Owned(x) => x.deref(),
                BytesCow::Borrowed(x) => x.deref(),
            }
        }
    }
}
