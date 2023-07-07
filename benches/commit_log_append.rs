use std::convert::Infallible;

use criterion::{criterion_group, criterion_main, Criterion};
use futures_lite::stream;
use laminarmq::storage::commit_log::{segmented_log::MetaWithIdx, CommitLog, Record};

#[allow(unused)]
async fn commit_log_append<M, X, T, C>(commit_log: &mut C, record: Record<M, X>)
where
    C: CommitLog<M, X, T>,
{
    commit_log.append(record).await.unwrap();
}

fn infallible<T>(t: T) -> Result<T, Infallible> {
    Ok(t)
}

fn record<X, Idx>(stream: X) -> Record<MetaWithIdx<(), Idx>, X> {
    Record {
        metadata: MetaWithIdx {
            metadata: (),
            index: None,
        },
        value: stream,
    }
}

fn criterion_benchmark(_c: &mut Criterion) {
    let _tiny_message = stream::once(infallible(b"Hello World!"));

    let _lorem_140 = [
        b"Donec neque velit, pulvinar in sed.",
        b"Pellentesque sodales, felis sit et.",
        b"Sed lobortis magna sem, eu laoreet.",
        b"Praesent quis varius diam. Nunc at.",
    ];

    // 140 bytes: pre-2017 Twitter tweet limit
    let _tweet = stream::iter(_lorem_140.iter().map(infallible));

    // 2940 bytes: within 3000 character limit on LinkedIn posts
    let _linked_in_post = stream::iter(_lorem_140.iter().cycle().take(21).map(infallible));

    let _record = record::<_, u32>(_tweet);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
