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

    impl<'a> From<BytesCow<'a>> for Cow<'a, [u8]> {
        fn from(bytes_cow: BytesCow<'a>) -> Self {
            match bytes_cow {
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

pub mod binalt {
    /// Binary Alternative.
    ///
    /// This is useful in reducing code duplication in matches with a lot of fallback cases.
    /// Consider the following example:
    /// ```
    /// use laminarmq::common::binalt::BinAlt;
    ///
    /// struct Value { x: i32 }
    ///
    /// fn some_func(x: &i32) -> Result<i32, ()> {
    ///     if x > &10 {
    ///         Ok(100)
    ///     } else {
    ///         Err(())
    ///     }
    /// }
    ///
    /// async fn fallback(x: Option<i32>) -> Value {
    ///     Value { x: -x.unwrap_or(57) }
    /// }
    ///
    /// async fn without_binalt(x_option: Option<i32>) -> Value {
    ///    match x_option {
    ///     Some(x) => match some_func(&x) {
    ///         Ok(x) => Value{x},
    ///         Err(()) => fallback(x_option).await,
    ///     }
    ///     None => fallback(x_option).await,
    ///    }
    /// }
    ///
    /// // Reduces number of await points to one.
    /// async fn with_binalt(x_option: Option<i32>) -> Value {
    ///     let x_binalt = match x_option {
    ///         Some(x) => match some_func(&x) {
    ///             Ok(x) => BinAlt::A(x),
    ///             Err(()) => BinAlt::B(x_option),
    ///         }
    ///         None => BinAlt::B(x_option),
    ///     };
    ///
    ///     match x_binalt {
    ///         BinAlt::A(x) => Value{x},
    ///         BinAlt::B(x_option) => fallback(x_option).await
    ///     }
    /// }
    ///
    /// ```
    ///
    /// Notice how in the second example we have only a single await point.
    /// Could this be done using [std::result::Result]?
    /// Yes. However, Result should be reserved for error handling contexts.
    pub enum BinAlt<A, B> {
        /// Alternative 'A'
        A(A),
        /// Alternative 'B'
        B(B),
    }
}
