use criterion::{
    async_executor::FuturesExecutor, black_box, criterion_group, criterion_main, BenchmarkId,
    Criterion, Throughput,
};
use futures_lite::{stream, StreamExt};
use laminarmq::{
    common::serde_compat::bincode,
    storage::{
        commit_log::{
            segmented_log::{segment::Config as SegmentConfig, Config, MetaWithIdx, SegmentedLog},
            CommitLog, Record,
        },
        impls::{
            common::DiskBackedSegmentStorageProvider,
            glommio::storage::{
                buffered::{BufferedStorage, BufferedStorageProvider},
                dma::{DmaStorage, DmaStorageProvider},
            },
            in_mem::{segment::InMemSegmentStorageProvider, storage::InMemStorage},
            tokio::storage::{StdFileStorage, StdFileStorageProvider},
        },
        AsyncConsume,
    },
};
use pprof::criterion::{Output::Flamegraph, PProfProfiler};
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

const LOREM_140: [[u8; 35]; 4] = [
    *b"Donec neque velit, pulvinar in sed.",
    *b"Pellentesque sodales, felis sit et.",
    *b"Sed lobortis magna sem, eu laoreet.",
    *b"Praesent quis varius diam. Nunc at.",
];

struct GlommioAsyncExecutor(glommio::LocalExecutor);

impl criterion::async_executor::AsyncExecutor for GlommioAsyncExecutor {
    fn block_on<T>(&self, future: impl futures_core::Future<Output = T>) -> T {
        self.0.run(future)
    }
}

const IN_MEMORY_SEGMENTED_LOG_CONFIG: Config<u32, usize> = Config {
    segment_config: SegmentConfig {
        max_store_size: 1048576, // = 1MiB
        max_store_overflow: 524288,
        max_index_size: 1048576,
    },
    initial_index: 0,
};

const PERSISTENT_SEGMENTED_LOG_CONFIG: Config<u32, u64> = Config {
    segment_config: SegmentConfig {
        max_store_size: 10000000, // ~ 10MB
        max_store_overflow: 10000000 / 2,
        max_index_size: 10000000,
    },
    initial_index: 0,
};

fn increase_rlimit_nofile_soft_limit_to_hard_limit() -> std::io::Result<()> {
    use rlimit::{getrlimit, setrlimit, Resource};

    getrlimit(Resource::NOFILE).and_then(|(_, hard)| setrlimit(Resource::NOFILE, hard, hard))
}

fn criterion_benchmark_with_record_content<X, XBuf, XE>(
    c: &mut Criterion,
    record_content: X,
    record_content_size: u64,
    record_size_group_name: &str,
) where
    X: stream::Stream<Item = Result<XBuf, XE>> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    increase_rlimit_nofile_soft_limit_to_hard_limit().unwrap();

    let mut group = c.benchmark_group(record_size_group_name);

    for num_appends in (1000..=10000).step_by(1000) {
        group
            .throughput(Throughput::Bytes(record_content_size * num_appends as u64))
            .sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("in_memory_segmented_log", num_appends),
            &num_appends,
            |b, &num_appends| {
                b.to_async(FuturesExecutor).iter_custom(|_| async {
                    let mut segmented_log = SegmentedLog::<
                        InMemStorage,
                        (),
                        crc32fast::Hasher,
                        u32,
                        usize,
                        bincode::BinCode,
                        _,
                    >::new(
                        IN_MEMORY_SEGMENTED_LOG_CONFIG,
                        InMemSegmentStorageProvider::<u32>::default(),
                    )
                    .await
                    .unwrap();

                    let start = std::time::Instant::now();

                    for _ in 0..num_appends {
                        black_box(
                            segmented_log
                                .append(record(record_content.clone()))
                                .await
                                .unwrap(),
                        );
                    }

                    let time_taken = start.elapsed();

                    segmented_log.remove().await.unwrap();

                    time_taken
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("glommio_dma_file_segmented_log", num_appends),
            &num_appends,
            |b, &num_appends| {
                b.to_async(GlommioAsyncExecutor(glommio::LocalExecutor::default()))
                    .iter_custom(|_| async {
                        const BENCH_GLOMMIO_DMA_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY: &str =
                            "/tmp/laminarmq_bench_glommio_dma_file_segmented_log_append";

                        let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                            _,
                            _,
                            u32,
                        >::with_storage_directory_path_and_provider(
                            BENCH_GLOMMIO_DMA_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY,
                            DmaStorageProvider,
                        )
                        .unwrap();

                        let mut segmented_log = SegmentedLog::<
                            DmaStorage,
                            (),
                            crc32fast::Hasher,
                            u32,
                            u64,
                            bincode::BinCode,
                            _,
                        >::new(
                            PERSISTENT_SEGMENTED_LOG_CONFIG,
                            disk_backed_storage_provider,
                        )
                        .await
                        .unwrap();

                        let start = std::time::Instant::now();

                        for _ in 0..num_appends {
                            black_box(
                                segmented_log
                                    .append(record(record_content.clone()))
                                    .await
                                    .unwrap(),
                            );
                        }

                        let time_taken = start.elapsed();

                        segmented_log.close().await.unwrap();

                        glommio::executor()
                            .spawn_blocking(|| {
                                std::fs::remove_dir_all(
                                    BENCH_GLOMMIO_DMA_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY,
                                )
                                .unwrap();
                            })
                            .await;

                        time_taken
                    });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("glommio_buffered_file_segmented_log", num_appends),
            &num_appends,
            |b, &num_appends| {
                b.to_async(GlommioAsyncExecutor(glommio::LocalExecutor::default()))
                    .iter_custom(|_| async {
                        const BENCH_GLOMMIO_BUFFERED_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY: &str =
                            "/tmp/laminarmq_bench_glommio_buffered_file_segmented_log_append";

                        let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                            _,
                            _,
                            u32,
                        >::with_storage_directory_path_and_provider(
                            BENCH_GLOMMIO_BUFFERED_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY,
                            BufferedStorageProvider,
                        )
                        .unwrap();

                        let mut segmented_log = SegmentedLog::<
                            BufferedStorage,
                            (),
                            crc32fast::Hasher,
                            u32,
                            u64,
                            bincode::BinCode,
                            _,
                        >::new(
                            PERSISTENT_SEGMENTED_LOG_CONFIG,
                            disk_backed_storage_provider,
                        )
                        .await
                        .unwrap();

                        let start = std::time::Instant::now();

                        for _ in 0..num_appends {
                            black_box(
                                segmented_log
                                    .append(record(record_content.clone()))
                                    .await
                                    .unwrap(),
                            );
                        }

                        let time_taken = start.elapsed();

                        segmented_log.close().await.unwrap();

                        glommio::executor()
                            .spawn_blocking(|| {
                                std::fs::remove_dir_all(
                                    BENCH_GLOMMIO_BUFFERED_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY,
                                )
                                .unwrap();
                            })
                            .await;

                        time_taken
                    });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tokio_segmented_log", num_appends),
            &num_appends,
            |b, &num_appends| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter_custom(|_| async {
                        const BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY: &str =
                            "/tmp/laminarmq_bench_tokio_std_file_segmented_log_append";

                        let disk_backed_storage_provider = DiskBackedSegmentStorageProvider::<
                            _,
                            _,
                            u32,
                        >::with_storage_directory_path_and_provider(
                            BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY,
                            StdFileStorageProvider,
                        )
                        .unwrap();

                        let mut segmented_log = SegmentedLog::<
                            StdFileStorage,
                            (),
                            crc32fast::Hasher,
                            u32,
                            u64,
                            bincode::BinCode,
                            _,
                        >::new(
                            PERSISTENT_SEGMENTED_LOG_CONFIG,
                            disk_backed_storage_provider,
                        )
                        .await
                        .unwrap();

                        let start = tokio::time::Instant::now();

                        for _ in 0..num_appends {
                            black_box(
                                segmented_log
                                    .append(record(record_content.clone()))
                                    .await
                                    .unwrap(),
                            );
                        }

                        let time_taken = start.elapsed();

                        segmented_log.close().await.unwrap();

                        tokio::fs::remove_dir_all(BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY)
                            .await
                            .unwrap();

                        time_taken
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

fn benchmark_lorem_140_repeated(
    c: &mut Criterion,
    repetitions: usize,
    record_size_group_name: &str,
) {
    let source_packets = LOREM_140.iter().map(|x| x as &[u8]);
    const SOURCE_PACKETS_LEN: usize = LOREM_140.len();
    const AVG_PACKET_LEN: usize = LOREM_140[0].len();

    let bench_source_packets_len = SOURCE_PACKETS_LEN * repetitions;
    let bench_source_packets = source_packets.cycle().take(bench_source_packets_len);

    let record_content_size: u64 = (bench_source_packets_len * AVG_PACKET_LEN) as u64;
    let message = stream::iter(bench_source_packets).map(infallible);

    criterion_benchmark_with_record_content(
        c,
        message,
        record_content_size,
        record_size_group_name,
    );
}

fn benchmark_tweet_append(c: &mut Criterion) {
    // 140 * 1 = 140 bytes = pre 2017 twitter limit
    benchmark_lorem_140_repeated(c, 1, "commit_log_append_with_tweet");
}

fn benchmark_half_k_message_append(c: &mut Criterion) {
    // 140 * 4 = 560 ≈ 512 bytes
    benchmark_lorem_140_repeated(c, 4, "commit_log_append_with_half_k_message");
}

fn benchmark_k_message_append(c: &mut Criterion) {
    // 140 * 8 = 1120 ≈ 1024 bytes
    benchmark_lorem_140_repeated(c, 8, "commit_log_append_with_k_message");
}

fn benchmark_linked_in_post_append(c: &mut Criterion) {
    // 140 * 21 = 2940 <= within 3000 bytes LinkedIn post limit
    benchmark_lorem_140_repeated(c, 21, "commit_log_append_with_linked_in_post");
}

fn benchmark_blog_post_append(c: &mut Criterion) {
    // (140 * 21) * 4 = 11760; avg. blog size = 4x LinkedIn post
    benchmark_lorem_140_repeated(c, 21 * 4, "commit_log_append_with_blog_post");
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Flamegraph(None)));
    targets =
    benchmark_tiny_message_append,
    benchmark_tweet_append,
    benchmark_half_k_message_append,
    benchmark_k_message_append,
    benchmark_linked_in_post_append,
    benchmark_blog_post_append
);
criterion_main!(benches);
