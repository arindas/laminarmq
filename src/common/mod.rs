pub mod cache;

pub mod borrow {
    use bytes::Bytes;
    use std::{borrow::Cow, ops::Deref};

    #[derive(Debug)]
    pub enum BytesCow {
        Owned(Bytes),
        Borrowed(Cow<'static, [u8]>),
    }

    impl From<Cow<'static, [u8]>> for BytesCow {
        fn from(cow: Cow<'static, [u8]>) -> Self {
            match cow {
                Cow::Borrowed(_) => BytesCow::Borrowed(cow),
                Cow::Owned(owned_bytes) => BytesCow::Owned(Bytes::from(owned_bytes)),
            }
        }
    }

    impl Into<Cow<'static, [u8]>> for &BytesCow {
        fn into(self) -> Cow<'static, [u8]> {
            match self {
                BytesCow::Owned(x) => Into::<Vec<u8>>::into(x.clone()).into(),
                BytesCow::Borrowed(cow) => cow.clone(),
            }
        }
    }

    impl Deref for BytesCow {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            match self {
                BytesCow::Owned(x) => x.deref(),
                BytesCow::Borrowed(x) => x.deref(),
            }
        }
    }
}
