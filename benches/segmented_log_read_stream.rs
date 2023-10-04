use criterion::{
    async_executor::{AsyncExecutor, FuturesExecutor},
    black_box, criterion_group, criterion_main,
    measurement::{Measurement, WallTime},
    BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use futures_lite::{stream, StreamExt};
use laminarmq::{
    common::{cache::NoOpCache, serde_compat::bincode},
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
            tokio::storage::{
                std_random_read::{StdRandomReadFileStorage, StdRandomReadFileStorageProvider},
                std_seek_read::{StdSeekReadFileStorage, StdSeekReadFileStorageProvider},
            },
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
    num_index_cached_read_segments: None,
};

const PERSISTENT_SEGMENTED_LOG_CONFIG: Config<u32, u64> = Config {
    segment_config: SegmentConfig {
        max_store_size: 10000000, // ~ 10MB
        max_store_overflow: 10000000 / 2,
        max_index_size: 10000000,
    },
    initial_index: 0,
    num_index_cached_read_segments: None,
};

fn increase_rlimit_nofile_soft_limit_to_hard_limit() -> std::io::Result<()> {
    use rlimit::{getrlimit, setrlimit, Resource};

    getrlimit(Resource::NOFILE).and_then(|(_, hard)| setrlimit(Resource::NOFILE, hard, hard))
}

async fn time_in_mem_seg_log<X, XBuf, XE>(
    record_content: X,
    num_appends: usize,
) -> std::time::Duration
where
    X: stream::Stream<Item = Result<XBuf, XE>> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    let mut segmented_log = SegmentedLog::<
        InMemStorage,
        (),
        crc32fast::Hasher,
        u32,
        usize,
        bincode::BinCode,
        _,
        NoOpCache<usize, ()>,
    >::new(
        IN_MEMORY_SEGMENTED_LOG_CONFIG,
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

    let start = std::time::Instant::now();

    black_box(segmented_log.stream_unbounded().count().await);

    let time_taken = start.elapsed();

    segmented_log.remove().await.unwrap();

    time_taken
}

async fn time_glommio_dma_file_segmented_log<X, XBuf, XE>(
    record_content: X,
    num_appends: usize,
) -> std::time::Duration
where
    X: stream::Stream<Item = Result<XBuf, XE>> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    const BENCH_GLOMMIO_DMA_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY: &str =
        "/tmp/laminarmq_bench_glommio_dma_file_segmented_log_read_stream";

    let disk_backed_storage_provider =
        DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
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
        NoOpCache<usize, ()>,
    >::new(
        PERSISTENT_SEGMENTED_LOG_CONFIG,
        disk_backed_storage_provider,
    )
    .await
    .unwrap();

    for _ in 0..num_appends {
        segmented_log
            .append(record(record_content.clone()))
            .await
            .unwrap();
    }

    let start = std::time::Instant::now();

    black_box(segmented_log.stream_unbounded().count().await);

    let time_taken = start.elapsed();

    segmented_log.close().await.unwrap();

    glommio::executor()
        .spawn_blocking(|| {
            std::fs::remove_dir_all(BENCH_GLOMMIO_DMA_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY)
                .unwrap();
        })
        .await;

    time_taken
}

async fn time_glommio_buffered_file_segmented_log<X, XBuf, XE>(
    record_content: X,
    num_appends: usize,
) -> std::time::Duration
where
    X: stream::Stream<Item = Result<XBuf, XE>> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    const BENCH_GLOMMIO_BUFFERED_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY: &str =
        "/tmp/laminarmq_bench_glommio_buffered_file_segmented_log_read_stream";

    let disk_backed_storage_provider =
        DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
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
        NoOpCache<usize, ()>,
    >::new(
        PERSISTENT_SEGMENTED_LOG_CONFIG,
        disk_backed_storage_provider,
    )
    .await
    .unwrap();

    for _ in 0..num_appends {
        segmented_log
            .append(record(record_content.clone()))
            .await
            .unwrap();
    }

    let start = std::time::Instant::now();

    black_box(segmented_log.stream_unbounded().count().await);

    let time_taken = start.elapsed();

    segmented_log.close().await.unwrap();

    glommio::executor()
        .spawn_blocking(|| {
            std::fs::remove_dir_all(BENCH_GLOMMIO_BUFFERED_FILE_SEGMENTED_LOG_STORAGE_DIRECTORY)
                .unwrap();
        })
        .await;

    time_taken
}

async fn time_tokio_std_random_read_segmented_log<X, XBuf, XE>(
    record_content: X,
    num_appends: usize,
) -> tokio::time::Duration
where
    X: stream::Stream<Item = Result<XBuf, XE>> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    const BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY: &str =
        "/tmp/laminarmq_bench_tokio_std_random_read_segmented_log_read_stream";

    let disk_backed_storage_provider =
        DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
            BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY,
            StdRandomReadFileStorageProvider,
        )
        .unwrap();

    let mut segmented_log = SegmentedLog::<
        StdRandomReadFileStorage,
        (),
        crc32fast::Hasher,
        u32,
        u64,
        bincode::BinCode,
        _,
        NoOpCache<usize, ()>,
    >::new(
        PERSISTENT_SEGMENTED_LOG_CONFIG,
        disk_backed_storage_provider,
    )
    .await
    .unwrap();

    for _ in 0..num_appends {
        segmented_log
            .append(record(record_content.clone()))
            .await
            .unwrap();
    }

    let start = tokio::time::Instant::now();

    black_box(segmented_log.stream_unbounded().count().await);

    let time_taken = start.elapsed();

    segmented_log.close().await.unwrap();

    tokio::fs::remove_dir_all(BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY)
        .await
        .unwrap();

    time_taken
}

async fn time_tokio_std_seek_read_segmented_log<X, XBuf, XE>(
    record_content: X,
    num_appends: usize,
) -> tokio::time::Duration
where
    X: stream::Stream<Item = Result<XBuf, XE>> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    const BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY: &str =
        "/tmp/laminarmq_bench_tokio_std_seek_read_segmented_log_read_stream";

    let disk_backed_storage_provider =
        DiskBackedSegmentStorageProvider::<_, _, u32>::with_storage_directory_path_and_provider(
            BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY,
            StdSeekReadFileStorageProvider,
        )
        .unwrap();

    let mut segmented_log = SegmentedLog::<
        StdSeekReadFileStorage,
        (),
        crc32fast::Hasher,
        u32,
        u64,
        bincode::BinCode,
        _,
        NoOpCache<usize, ()>,
    >::new(
        PERSISTENT_SEGMENTED_LOG_CONFIG,
        disk_backed_storage_provider,
    )
    .await
    .unwrap();

    for _ in 0..num_appends {
        segmented_log
            .append(record(record_content.clone()))
            .await
            .unwrap();
    }

    let start = tokio::time::Instant::now();

    black_box(segmented_log.stream_unbounded().count().await);

    let time_taken = start.elapsed();

    segmented_log.close().await.unwrap();

    tokio::fs::remove_dir_all(BENCH_TOKIO_SEGMENTED_LOG_STORAGE_DIRECTORY)
        .await
        .unwrap();

    time_taken
}

#[allow(unused)]
struct BenchGroup<'a, 'b, M: Measurement> {
    group: &'b mut BenchmarkGroup<'a, M>,
}
#[allow(unused)]
impl<'a, 'b, M> BenchGroup<'a, 'b, M>
where
    M: Measurement,
{
    fn bench<MA, A, MR, R, F, I>(
        &'b mut self,
        input: I,
        bench_name: &str,
        make_async_executor_fn: MA,
        make_routine_fn: MR,
    ) where
        MA: Fn() -> A,
        A: AsyncExecutor,
        MR: Fn() -> R,
        R: FnMut(u64) -> F,
        F: futures_util::Future<Output = M::Value>,
        I: std::fmt::Display + Copy,
    {
        self.group
            .bench_with_input(BenchmarkId::new(bench_name, input), &input, |b, _| {
                b.to_async(make_async_executor_fn())
                    .iter_custom(make_routine_fn());
            });
    }
}

fn lorem_140_repeated_message(
    repetitions: usize,
) -> (
    impl stream::Stream<Item = impl Deref<Target = [u8]>> + Clone + Unpin,
    u64,
) {
    let source_packets = LOREM_140.iter().map(|x| x as &[u8]);
    const SOURCE_PACKETS_LEN: usize = LOREM_140.len();
    const AVG_PACKET_LEN: usize = LOREM_140[0].len();

    let bench_source_packets_len = SOURCE_PACKETS_LEN * repetitions;
    let bench_source_packets = source_packets.cycle().take(bench_source_packets_len);

    let record_content_size: u64 = (bench_source_packets_len * AVG_PACKET_LEN) as u64;
    let message = stream::iter(bench_source_packets);

    (message, record_content_size)
}

fn input_message_lorem_ipsum_repetitions_with_names() -> impl Iterator<Item = (&'static str, usize)>
{
    std::iter::empty()
        .chain(std::iter::once(("tweet", 1)))
        .chain(std::iter::once(("half_k_message", 4)))
        .chain(std::iter::once(("k_message", 8)))
        .chain(std::iter::once(("linked_in_post", 21)))
        .chain(std::iter::once(("blog_post", 21 * 4)))
}

fn bench_helper<X, XBuf>(group: &mut BenchmarkGroup<WallTime>, message: X, num_appends: usize)
where
    X: stream::Stream<Item = XBuf> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    let content = message.map(infallible);

    let mut bench_group = BenchGroup { group };

    bench_group.bench(
        &num_appends,
        "in_memory_segmented_log",
        || FuturesExecutor,
        || |_| async { time_in_mem_seg_log(content.clone(), num_appends).await },
    );

    let mut bench_group = BenchGroup { group };

    bench_group.bench(
        &num_appends,
        "glommio_dma_file_segmented_log",
        || GlommioAsyncExecutor(glommio::LocalExecutor::default()),
        || |_| async { time_glommio_dma_file_segmented_log(content.clone(), num_appends).await },
    );

    let mut bench_group = BenchGroup { group };

    bench_group.bench(
        &num_appends,
        "glommio_buffered_file_segmented_log",
        || GlommioAsyncExecutor(glommio::LocalExecutor::default()),
        || {
            |_| async {
                time_glommio_buffered_file_segmented_log(content.clone(), num_appends).await
            }
        },
    );

    let mut bench_group = BenchGroup { group };

    bench_group.bench(
        &num_appends,
        "tokio_std_random_read_segmented_log",
        || tokio::runtime::Runtime::new().unwrap(),
        || {
            |_| async {
                time_tokio_std_random_read_segmented_log(content.clone(), num_appends).await
            }
        },
    );

    let mut bench_group = BenchGroup { group };

    bench_group.bench(
        &num_appends,
        "tokio_std_seek_read_segmented_log",
        || tokio::runtime::Runtime::new().unwrap(),
        || |_| async { time_tokio_std_seek_read_segmented_log(content.clone(), num_appends).await },
    );
}

fn bench_message_kind<I, X, XBuf>(
    criterion: &mut Criterion,
    num_appends_iter: I,
    message: X,
    message_size: u64,
    group_name: &str,
) where
    I: Iterator<Item = usize>,
    X: stream::Stream<Item = XBuf> + Clone + Unpin,
    XBuf: Deref<Target = [u8]>,
{
    let mut group = criterion.benchmark_group(group_name);
    for num_appends in num_appends_iter {
        group
            .throughput(Throughput::Bytes(message_size * num_appends as u64))
            .sample_size(10);

        bench_helper(&mut group, message.clone(), num_appends);
    }
}

fn bench_segmented_log_read_stream(criterion: &mut Criterion) {
    increase_rlimit_nofile_soft_limit_to_hard_limit().unwrap();

    let input_message_kinds =
        input_message_lorem_ipsum_repetitions_with_names().map(|(kind_name, reps)| {
            let (message, record_content_size) = lorem_140_repeated_message(reps);

            (kind_name, message, record_content_size)
        });

    let input_num_appends_range = (1000..=10000).step_by(1000);

    {
        let tiny_message = stream::once(b"Hello World!" as &[u8]);
        let tiny_message_content_size = 12;

        bench_message_kind(
            criterion,
            input_num_appends_range.clone(),
            tiny_message,
            tiny_message_content_size,
            "segmented_log_read_stream_with_tiny_message",
        );
    }

    for message_kind in input_message_kinds {
        let (kind_name, message, record_content_size) = message_kind;

        bench_message_kind(
            criterion,
            input_num_appends_range.clone(),
            message,
            record_content_size,
            &format!("segmented_log_read_stream_with_{}", kind_name),
        );
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Flamegraph(None)));
    targets = bench_segmented_log_read_stream
);
criterion_main!(benches);
