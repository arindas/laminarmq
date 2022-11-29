pub trait Cache<Key, Value> {
    type Error;

    fn get<'value>(&self, key: &Key) -> Result<&'value Value, Self::Error>;

    fn query<'value>(&mut self, key: &Key) -> Result<&'value Value, Self::Error>;

    fn insert(&mut self, key: Key, value: Value) -> Result<(), Self::Error>;

    fn remove(&mut self, key: &Key) -> Result<Value, Self::Error>;
}
