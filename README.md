# `laminarmq` assorted notes
Brain-dump on different aspects of `laminarmq` as they come to my head.

## Execution model
Each partition in a node is uniquely mapped to a task. This task manages all operations associated with the task including:
- read and append operations
- raft consensus leader elections and FSM application.

There are dedicated tasks for service discovery on each node. This is used both for client side load balancing and
^_raft leader follower discovery_ (maybe i mean something else).

Next, we have the problem of routing requests to tasks. We have the following approaches:
- The requests are evenly distributed across all the tasks
  - Each task has a pair of SPSC channels for every other task.
  - If it receives a request not belonging to it's partition
    - if sends the request to the designated task and waits for the response using the pair of SPSC channels
    - it forwards the response to the client
  - if the request belongs to it, it locally processes the request and sends the response to the client
- The requests are uniquely routed to tasks responsible for managing the request
  - Each task listens on a public node
    - The service discovery layer forwards information regarding which node contains what partitions, and which ports
     those partitions are listening on.
  - Each task listens on a local port behind a packet routing solution.
    - eBPF?
    - A dedicated reverse proxy of a sort

Now how these tasks are scheduled depend on the number of processors we have. We could run all tasks on a single thread
or each task on it's own thread or a compromise between the two (static cpu shares on shared threads). However, this
execution model design should still remain correct.

## `laminarmq` server application architecture

__Note__: This model assumes that the requests are routed to proper threads based on partition.

![architecture-diagram](./assets/laminarmq-server-architecture.png)

Web server is broken down into shards. Requests are routed to shards with eBPF as described above.

There are three levels of execution:
- The http handler layer running with hyper. It has handler methods for different REST API endpoints.
It creates a PartitionTask containing the topic, partition id, operation details and response send end. It
keeps the recv end with itself. It then sends this task over the channel and keeps waiting on the recv end.
Note that this handler future is detached in the background.
- The shard manager receives partition tasks. It has a map which contains mapping from partition uid to
`Rc<RWLock<Partition<_>>>` instances. It looks up the partition for the given partition task, creates
a new future with a cloned Rc of the partition instance which invokes .read() or .write() as necessary
depending on whether the operation causes mutation or not. It then invokes the provided partition
implementation instances process() method on the partition task. The process() method is expected to
write the response to the channel. Once this future closure is created, it is detached in the background.
The shard manager continues to recv more partition tasks.

When the future detached by hyper receives the result in the recv end, it responds with result and exits.

Each shard runs an identical copy of the program described above.

## Misc. notes

- "Remove expired" could be a normal request type, which partitions may or may not serve.

## A bit about service discovery, replication and consensus
Service discovery happens at the server process level. When a new node is added to the cluster, we mark it as a peer.
Then for every partition to be replicated, we add the new nodes as a follower to the partition raft cluster.

Half baked inspirations:
- `Partition -> ReplicatedPartition<ConsensusAlgorithm>`
- `AddReplica(Peer)

## In-Efficiencies spotted
Right now we use a simple binary encoding scheme for encoding our data. There we wastefully allocate memory
by re-encoding the record value bytes with binary encoding.
In the future our record type should look like this:

```rust
struct Record<'a, M> {
  metadata: M
  value: Cow<'a, [u8]>
}
```

Notice how this doesn't contain any offset field. `Append()` will not do any further validation of the
metadata and simply write to disk. User must perform validation on their end before appending.

## Missing Features
We need a `Truncate()` operation for truncating a `CommitLog` up to a particular highest offset.

Partition implementations then can check for the offset specified in the request (if req.offset < highest offset) and depending on the
requirements truncate away all records that appear after it, or return an error.

~~(Index becomes necessary here. How do you scan backwards for records in a continuous file?)~~

## ~~Index: handle corner case for advance_to_offset() in segment~~

<del>
<p>
Our current segment implementation is thread-safe. In order to preserve thread-safety, we have to incorporate
Index into our segments in a fault tolerant manner. Specifically, there should be no holes in our index.
</p>

<p>
I propose the following:
- When creating the index from a file, store the positions read and file write seek position.
- Resume from the last write position and continue reading until you find the
</p>
</del>

~~Remove `advance_to_offset()` family of operations.~~

Provide Truncate() at the `Store` level. Store implementations can check whether a position is valid or not by
traversing over the record headers and seeking through the file. However, this will need the store backing files
to be synced properly. (`reload_write_segment()` :3)
We can also find the previous valid position in the same store file and truncate after that if necessary.

Still store can keep a set of positions to check whether there is indeed a record at that position.

## Strategy for v0.1.0
Move forward with the current commit log implementation, finish the server and make a basic release.
There-after adapt these requirements in the `CommitLog` after we have a server ready.

Or, you know, you have no obligations to anyone. Maybe clean this up first.

## Progress on `feature/truncate` and `enhancement/lesser-copies`

## `truncate()`
There's a viable implementation of `truncate()` for segment and store in `feature/truncate`. It is possible to
implement `truncate()` for segmented_log based solely on the contents of that branch.  The current truncate
implementation uses a simple vector for keeping valid record positions in the store. Since we keep the positions
in sorted order (append order), we are able to leverage binary search to check whether a position is valid or not.

Segment simply invokes the underlying stores truncate() implementation after translating the offsets to positions.

Now the advance_to_offset() family of operations do not cause any problems with truncate since it used to extend
beyond the current size/limit, while truncate is used to limit the data-storage to a shorter size/limit. Also
these operations reload the files from disk to ensure that they read the latest values.

## minimizing copies [done]
The Cow might not be necessary at all. Any record ultimately contains a type that implements `Deref<Target = [u8]>`
and a serialize-able metadata field. When considering this way, the data model becomes simpler.

In order to tackle this, we have enhanced our store with a multipart record append operation. This allows to only
serialize the metadata, keeping the data contents untouched. While reading back from storage, we also accept
a metadata size field to split the record bytes into metadata and record byte segments. It is up-to the
implementation to explore zero copy data split operations here.

Next order of business:
- implement truncate() on CommitLog
- create a specific generic metadata type for SegmentedLog which contains a u64 offset field and a generic part.
The API using it has to be refactored accordingly.

The tasks are truly independent, if they are written keeping certain assumptions in mind. However, there is a
possibility of merge conflicts if we work on these two branches simultaneously. TBH we can deal with merge
conflicts. Time is of essence.

Then there is the larger task of refactoring the server/ modules with the new Record API. It's great that we have
tests.

Once we are done with the above two sub tasks, we can merge back into -> cached-partition.

## enhancement/lesser-copies is done!
`Record<'static>` is no more.

__Bonus__: Record metadata has been handled properly. Request and Response types are now generic over content
carrying type.

Next order of business -> test commit-log truncate properly.

## commit-log truncate has been sufficiently tested.

## 11 Feb, 2023 [done]

We have chosen to reimplement commit_log to make it more genric so as to allow streams to be written.

Before we implement Store and Index for glommio, we have to create a Storage trait

which implements asyncindexedread, and a generic append which accepts a stream and a writer fn that
writes a stream and returns something

async fn append<S, F, W, T>(&mut self, bytes: &S, writer: F) where S: Stream<...>, W: FnMut(S) -> F, F: Future<T>

now on top of this storage implement store and index

Once this is done, store and index could be concrete, implemented solely with storage

## 14 Mar, 2023 [done]

Implement Buf on ReadResult

Streaming response wont be possible at the moment since we would have to implement a custom Bytes with custom vtable, not possible right now

We need an HttpBody implementation for ReadResult which in turn will require Buf implementation

This will need a wrapper struct ofc

hyper serve requires body to qualify bounds HttpBody + 'static

## 22 Mar, 2023

Tasks accomplished:
- Indexed SegmentedLog read_append_truncate tests using new API and InMemStorageProvider
- Deref to HttpBody conversion

Strategy going forward:
- [x] Add a max store overflow config. `is_maxed` will remain unchanged.
  `append_threshold = store_remaining + max_overflow`
- [x] Add remove_expired tests
- [x] Add Storage implementations for BufferedFile and DmaFile
- [x] Create a diagram depicting the control and data flow in the segmented log data structure
- [ ] Add documentation for the segmented log API.
- [ ] Add benchmarks and profiling with flamegraphs.
- [ ] Once there is sufficient documentation and benchmarks, merge to develop and merge develop to main.
- [ ] Create a pre-release with sufficient release notes regarding the new data
structure. Make ripples with design differences and breaking changes involved
Highlight capabilities. Also clarify the intent of moving to this impl and deprecating
the old implementation.
- [ ] Write a blog post about the new segmented-log design and post it on r/rust. We have
refactored 3-4 times already. Let's face the world now. The only thing we aren't
confident about at this point is async_trait usage. Well very soon we will be able to
have stack-allocated async traits so no point in moving.

## 22 Mar, 2023, 18:17
In the long term, we should decide how the server abstractions should be organized.

The behaviour of partition is actually very well defined.
- Persistence using CommitLog
- Discovery with gossip, possibly an event source
- Replication to peer partitions in replication group
- Forwarding requests to designated partitions

This orchestration is concrete. Instead of making this orchestration abstract, we must
abstract the sub systems instead.

The task scheduling system and application sharding is not affected by cluster size.
It should be accordingly generic.

Instead of trying to make a small design of only the single-node case, we mus strive to
develop the general design, of which the single-node case will be a specialization.
Similar to how we have done for the segmented log.

## Preparation for next release
- [ ] Single thread, single node message queue REST API server
- [ ] Client CLI app for easily interacting with `laminarmq`

(eBPF based routing will be taken up in the next release.)

## 8th May, 2023, 13:07

We can use rendezvous hashing to assign partition to nodes or even, individual threads.
"CityHash" can be a useful hashing function in this regard.

Different network topology details can be part of the identifier we use for service
discovery using gossip protocols.

## 19th May 2023, 11:42

Added diagrams for:
- Cluster hierarchy
- Service discovery and partition distribution to nodes
- Request routing in nodes
- Partition control flow and replication

Here's all the diagrams in an order that makes sense:

## Design

### Cluster Hierarchy

![cluster-hierarchy](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-cluster-hierarchy.svg)

### Service Discovery and partition distribution to nodes

![service-discovery-and-partition-distribution-to-nodes](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-service-discovery-and-partition-distribution.svg)

### Request routing in nodes

#### General design

![request-rouing-general](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-node-request-routing-general.svg)

#### Thread per core architecture

![request-routing-thread-per-core](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-node-request-routing-thread-per-core.svg)

### Partition control flow and replication

![partition-control-flow-replication](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-partition-control-flow-and-replication.svg)

## Execution Model
  
### General Work stealing async runtime (e.g. `tokio`)

![async-execution-model-general](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-async-execution-model-general.svg)
  
### Thread per core async runtime (e.g. `glommio`)
  
![async-execution-model-thread-per-core](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-async-execution-model-thread-per-core.svg)
  
### Storage data-structures

#### `segmented_log`

![segmented-log](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-segmented-log.svg)

#### `laminarmq` specific enhancements to the `segmented_log` data-structure

![indexed-segmented-log](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-indexed-segmented-log-landscape.svg)

```
//! Index and position invariants across segmented_log

// segmented_log index invariants
segmented_log.lowest_index  = segmented_log.read_segments[0].lowest_index
segmented_log.highest_index = segmented_log.write_segment.highest_index

// record position invariants in store
records[i+1].position = records[i].position + records[i].record_header.length

// segment index invariants in segmented_log
segments[i+1].base_index = segments[i].highest_index = segments[i].index[index.len - 1].index + 1
```
## 16th August, 2023

Currently, we required 16MB to cache index records for 1GB of storage with
records of size 1 KB.

Target: <= 160MB for cache for 1 TB of storage.

Clearly, we can't reduce the storage footprint in further (already halved it
from 32 bytes to 16 bytes per index record). The way forward is to not store
index records for every segment in memory.

We need a form of LRU Cache of segments which will dictate which segments have
cached index records and which don't. Whenever a segment is read from we put into LRU cache.

(Write segment always has index records cached)

Whenever a segment enters a LRU cache, it loads index records in memory.
Whenever it leaves, it drops the cached index records vector.

With a fixed size LRU cache we can put an upper limit on the memory being used
to cache index records for segments.

Since, it's not necessary that every segment is being read from all the time.
This will lead to better utilization.

On the practical side of things, whenever a segment first enters the LRU cache
with the first read, all the index records are loaded so the first read will
have higher latency. Considering the amount of memory savings, this trade-off
doesn't sound too bad.

Finally, this inherently requires some form of mutation on reads. The current
APIs have been designed to guarantee idempotence on reads.

Here's a sketch for the API design:

Index will have an Option<Vec<IndexRecord>>. If it has Some(vec) it reads from
vec, otherwise it delegates reads to underlying storage.

Index have a cache_records(), drop_cache()

Segment has cache_index_records(), drop_index_cache()

We provide a new trait AsyncIndexedReadMut: AsyncIndexedRead with a ::read_mut(&mut self, …)
function.

Only SegmentedLog implements this trait with the LRU cache mechanism.
