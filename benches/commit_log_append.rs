use criterion::{
    async_executor::FuturesExecutor, criterion_group, criterion_main, BenchmarkId, Criterion,
    Throughput,
};
use futures_lite::stream;
use laminarmq::{
    common::serde_compat::bincode,
    storage::{
        commit_log::{
            segmented_log::{segment::Config as SegmentConfig, Config, MetaWithIdx, SegmentedLog},
            CommitLog, Record,
        },
        impls::in_mem::{segment::InMemSegmentStorageProvider, storage::InMemStorage},
    },
};
use std::{convert::Infallible, ops::Deref};

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

const LOREM_140: [&[u8]; 4] = [
    b"Donec neque velit, pulvinar in sed.",
    b"Pellentesque sodales, felis sit et.",
    b"Sed lobortis magna sem, eu laoreet.",
    b"Praesent quis varius diam. Nunc at.",
];

fn criterion_benchmark_with_record_content<X, XBuf, XE>(
    c: &mut Criterion,
    record_content: X,
    record_content_size: u64,
    record_size_group_name: &str,
) where
    X: stream::Stream<Item = Result<XBuf, XE>> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    let mut group = c.benchmark_group(record_size_group_name);

    for num_appends in (0..10000).step_by(1000) {
        group
            .throughput(Throughput::Bytes(record_content_size * num_appends as u64))
            .sample_size(10)
            .bench_with_input(
                BenchmarkId::new("in_memory_segmented_log", num_appends),
                &num_appends,
                |b, &num_appends| {
                    b.to_async(FuturesExecutor).iter_with_large_drop(|| async {
                        let config = Config {
                            segment_config: SegmentConfig {
                                max_store_size: 1048576,
                                max_store_overflow: 524288,
                                max_index_size: 1048576,
                            },
                            initial_index: 0,
                        };

                        let mut segmented_log = SegmentedLog::<
                            InMemStorage,
                            (),
                            crc32fast::Hasher,
                            u32,
                            usize,
                            bincode::BinCode,
                            _,
                        >::new(
                            config,
                            InMemSegmentStorageProvider::<u32>::default(),
                        )
                        .await
                        .unwrap();

                        for _ in 0..num_appends {
                            segmented_log
                                .append(record(record_content.clone()))
                                .await
                                .unwrap();
                        }
                    });
                },
            );
    }
}

fn benchmark_tiny_message_append(c: &mut Criterion) {
    // 12 bytes
    let tiny_message = stream::once(infallible(b"Hello World!" as &[u8]));

    criterion_benchmark_with_record_content(
        c,
        tiny_message,
        12,
        "commit_log_append_with_tiny_message",
    );
}

fn benchmark_tweet_append(c: &mut Criterion) {
    // 140 bytes: pre-2017 Twitter tweet limit
    let tweet = stream::iter(LOREM_140.iter().cloned().map(infallible));

    criterion_benchmark_with_record_content(c, tweet, 140, "commit_log_append_with_tweet");
}

fn benchmark_half_k_message_append(c: &mut Criterion) {
    // 2940 bytes: within 3000 character limit on LinkedIn posts
    let k_message = stream::iter(LOREM_140.iter().cloned().cycle().take(4).map(infallible));

    criterion_benchmark_with_record_content(
        c,
        k_message,
        2940,
        "commit_log_append_with_half_k_message",
    );
}

fn benchmark_k_message_append(c: &mut Criterion) {
    // 2940 bytes: within 3000 character limit on LinkedIn posts
    let k_message = stream::iter(LOREM_140.iter().cloned().cycle().take(8).map(infallible));

    criterion_benchmark_with_record_content(c, k_message, 2940, "commit_log_append_with_k_message");
}

fn benchmark_linked_in_post_append(c: &mut Criterion) {
    // 2940 bytes: within 3000 character limit on LinkedIn posts
    let linked_in_post = stream::iter(LOREM_140.iter().cloned().cycle().take(21).map(infallible));

    criterion_benchmark_with_record_content(
        c,
        linked_in_post,
        2940,
        "commit_log_append_with_linked_in_post",
    );
}

fn benchmark_blog_post_append(c: &mut Criterion) {
    // 2940 * 4 bytes
    let linked_in_post = stream::iter(LOREM_140.iter().cloned().cycle().take(84).map(infallible));

    criterion_benchmark_with_record_content(
        c,
        linked_in_post,
        2940,
        "commit_log_append_with_blog_post",
    );
}

criterion_group!(
    benches,
    benchmark_tiny_message_append,
    benchmark_tweet_append,
    benchmark_half_k_message_append,
    benchmark_k_message_append,
    benchmark_linked_in_post_append,
    benchmark_blog_post_append
);
criterion_main!(benches);
