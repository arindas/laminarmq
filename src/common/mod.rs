pub mod cache;

pub mod borrow {
    use bytes::Bytes;
    use std::{borrow::Cow, ops::Deref};

    #[derive(Debug, Clone)]
    pub enum BytesCow<'a> {
        Owned(Bytes),
        Borrowed(Cow<'a, [u8]>),
    }

    impl<'a> From<Cow<'a, [u8]>> for BytesCow<'a> {
        fn from(cow: Cow<'a, [u8]>) -> Self {
            match cow {
                Cow::Borrowed(_) => BytesCow::Borrowed(cow),
                Cow::Owned(owned_bytes) => BytesCow::Owned(Bytes::from(owned_bytes)),
            }
        }
    }

    impl<'a> Into<Cow<'a, [u8]>> for BytesCow<'a> {
        fn into(self) -> Cow<'a, [u8]> {
            match self {
                BytesCow::Owned(x) => Into::<Vec<u8>>::into(x).into(),
                BytesCow::Borrowed(cow) => cow,
            }
        }
    }

    impl<'a> Deref for BytesCow<'a> {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            match self {
                BytesCow::Owned(x) => x.deref(),
                BytesCow::Borrowed(x) => x.deref(),
            }
        }
    }
}
