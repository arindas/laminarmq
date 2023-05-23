<p align="center">
  <img src="https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/logo.png" alt="laminarmq">
</p>

<p align="center">
  <a href="https://github.com/arindas/laminarmq/actions/workflows/rust-ci.yml">
  <img src="https://github.com/arindas/laminarmq/actions/workflows/rust-ci.yml/badge.svg" />
  </a>
  <a href="https://codecov.io/gh/arindas/laminarmq" >
  <img src="https://codecov.io/gh/arindas/laminarmq/branch/main/graph/badge.svg?token=6VLETF5REC"/>
  </a>
  <a href="https://crates.io/crates/laminarmq">
  <img src="https://img.shields.io/crates/v/laminarmq" />
  </a>
  <a href="https://github.com/arindas/laminarmq/actions/workflows/rustdoc.yml">
  <img src="https://github.com/arindas/laminarmq/actions/workflows/rustdoc.yml/badge.svg" />
  </a>
</p>

<p align="center">
A scalable, distributed message queue powered by a segmented, partitioned, replicated and immutable
log.<br><i>This is currently a work in progress.</i>
</p>

## Usage

`laminarmq` provides a library crate and two binaries for managing `laminarmq` deployments. In order
to use `laminarmq` as a library, add the following to your `Cargo.toml`:

```toml
[dependencies]
laminarmq = "0.0.5-rc1"
```

Refer to latest git [API Documentation](https://arindas.github.io/laminarmq/docs/laminarmq/) or
[Crate Documentation](https://docs.rs/laminarmq) for more details. There's also a
[book](https://arindas.github.io/laminarmq/book) being written to further describe design decisions,
implementation details and recipes.

`laminarmq` presents an elementary commit-log abstraction (a series of records ordered by indices),
on top of which several message queue semantics such as publish subscribe or even full blown
protocols like MQTT could be implemented. Users are free to read the messages with offsets in any
order they need.

## Major milestones for `laminarmq`

- [x] Locally persistent queue of records
- [ ] Single node, multi threaded, eBPF based request to thread routed message queue
- [ ] Service discovery with
  [SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf).
- [ ] Replication and consensus of replicated records with [Raft](https://raft.github.io/raft.pdf).

## Design

This section describes the internal design of `laminarmq`.

### Cluster Hierarchy

![cluster-hierarchy](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-cluster-hierarchy.svg)

```
partition_id_x is of the form (topic_id, partition_idx)

In this example, consider:

partition_id_0 = (topic_id_0, partition_idx_0)
partition_id_1 = (topic_id_0, partition_idx_1)

partition_id_2 = (topic_id_1, partition_idx_0)

```
>The exact numerical ids don't have any pattern with respect to partition_id and topic_id; there can
>be multiple topics, each of which can have multiple partitions (identified by partition_idx).

… alternatively:

```text
[cluster]
├── node#001
│   ├── (topic#001, partition#001) [L]
│   │   └── segmented_log{[segment#001, segment#002, ...]}
│   ├── (topic#001, partition#002) [L]
│   │   └── segmented_log{[segment#001, segment#002, ...]}
│   └── (topic#002, partition#001) [F]
│       └── segmented_log{[segment#001, segment#002, ...]}
├── node#002
│   ├── (topic#001, partition#002) [F]
│   │   └── segmented_log{[segment#001, segment#002, ...]}
│   └── (topic#002, partition#001) [L]
│       └── segmented_log{[segment#001, segment#002, ...]}
└── ...other nodes

```
```
[L] := leader; [F] := follower
```

<p align="center">
<b>Fig:</b> <code>laminarmq</code> cluster hierarchy depicting partitioning and replication.
</p>

A "topic" is a collection of records. A topic is divided into multiple "partition"(s). Each
"partition" is then further replicated across multiple "node"(s). A "node" may contain some or all
"partition"(s) of a "topic". In this way a topic is both partitioned and replicated across the
nodes in the cluster.

There is no ordering of messages at the "topic" level. However, a "partition" is an ordered
collection of records, ordered by record indices.

Although we conceptually maintain a hierarchy of partitions and topics, at the cluster level, we
have chosen to maintain a flat representation of topic partitions. We present an elementary
commit-log API at the partition level.

Users may hence do the following:
- Directly interact with our message queue at the partition level
- Use client side load balancing between topic partitions

This alleviates the burden of load balancing messages among partitions and message stream ownership
record keeping from the cluster. Higher level constructs can be built on top of the partition
commit-log based API as necessary.

Each partition replica group has a leader where writes go, and a set of followers which follow the
leader and may be read from. Users may again use client side load balancing to balance reads across
the leader and all the followers.

Each partition replica is backed by a segmented log for storage.

### Service discovery and partition distribution to nodes

![service-discovery-and-partition-distribution-to-nodes](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-service-discovery-and-partition-distribution.svg)

<p align="center">
<b>Fig:</b> Rendezvous hashing based partition distribution and gossip style service discovery
mechanism used by <code>laminarmq</code>
</p>

In our cluster, nodes discover each other using gossip style peer to peer mechanisms. One such
mechanism is [SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) (Scalable
Weakly Consistent Infection-style Process Group Memberhsip).

In this mechanism, each member node notifies other members in the group whether a node is joining or
leaving the cluster by using a gossip style information dissemination mechanism (A node gossips to
neighbouring nodes, they in-turn gossip to their neighbours and so on).

In order to see whether a node has failed, the nodes randomly probes individual nodes in the
cluster. For instance, node A probes node B directly. If node B responds, it has not failed. If node
B does not respond, A attempts to probe node B indirectly through other nodes in the cluster, e.g.
node A might ask node C to probe node B. Node A continues to indirectly probe node B with all the
other nodes in the cluster. If node B responds to any of the indirect probes, it is still considered
to not have failed. It is otherwise declared failed and removed from the cluster.

There are mechanisms in place to reduce false failures caused due to temporary hiccups. The paper
goes into detail about those mechanisms.

This is also the core technology used in [Hashicorp Serf](https://www.serf.io/), where there are
further enhancements to improve failure detection and convergence speed.

Using this mechanism we can obtain a list of all the members in our cluster, along with their unique
ids and capacity weights. We then use their ids and weights to determine where to place a partition
using Rendezvous hashing.

From the Wikipedia [article](https://en.wikipedia.org/wiki/Rendezvous_hashing):
>Rendezvous or highest random weight (HRW) hashing is an algorithm that allows clients to achieve
>distributed agreement on a set of _k_ options out of a possible set of _n_ options. A typical
>application is when clients need to agree on which sites (or proxies) objects are assigned to.

In our case, we use rendezvous hashing to determine the subset of nodes to use for placing the
replicas of a partition.

For some hashing function `H`, some weight function `f(w, hash)` and partition id `P_x`, we proceed
as follows:
- For every node `N_i` in the cluster with a weight `w_i`, we compute `R_i = f(w_i, H(concat(P_x,
N_i)))`
- We rank all nodes `N_i` belonging to the set of nodes `N` with respect to their rank value `R_i`.
- For some replication factor `k`, we select the top `k` nodes to place the `k` replicas of the
partition with id `P_x`

(We assume `k <= |N|`; where `|N|` is the number of nodes and `k` is the number of replicas)

With this mechanism, anyone with the ids and weights of all the members in the cluster can compute
the destination nodes for the replicas of a partition. This knowledge can also be used to route
partition request to the appropriate nodes at both the client side and the server side.

In our case, we use client side load balancing to load balance all idempotent partition requests
across all the possible nodes where a replica of the request's partition can be present. For
non-idempotent request, if we send it to any one of the candidate nodes, they redirect it to the
current leader of the replica set.

## Supported execution models

`laminarmq` supports two execution models:
- General async execution model used by various async runtimes in the Rust ecosystem (e.g `tokio`)
- Thread per core execution model

In the thread-per-core execution model individual processor cores are limited to single threads.
This model encourages design that minimizes inter-thread contention and locks, thereby improving
tail latencies in software services. Read: [The Impact of Thread per Core Architecture on
Application Tail Latency.](
https://helda.helsinki.fi//bitstream/handle/10138/313642/tpc_ancs19.pdf?sequence=1)

In the thread per core execution model, we have to leverage application level partitioning such that
each individual thread is responsible for a subset of requests and/or responsibilities. We also have
to complement this model with proper routing of requests to the threads to ensure locality of
requests. In our case this translates to having each thread be responsible for only a subset of the
partition replicas in a node. Requests pertaining to a partition replica are always routed to the
same thread. The following sections will go into more detail as to how this is achieved.

We realize that although the thread per core model has some inherent advantages, being compatible
with the existing Rust ecosystem will significantly increase adoption. Therefore, we have designed
our system with reusable components which can be organized to suit both execution models.

## Testing

You may run tests with `cargo` as you would for any other crate. However, since `laminarmq` is
poised to support multiple runtimes, some of them might require some additional setup before running
the steps.

For instance, the `glommio` async runtime which requires an updated linux kernel (at least 5.8) with
`io_uring` support. `glommio` also requires at least 512 KiB of locked memory for `io_uring` to
work. (Note: 512 KiB is the minimum needed to spawn a single executor. Spawning multiple executors
may require you to raise the limit accordingly. I recommend 8192 KiB on a 8 GiB RAM machine.)

First, check the current `memlock` limit:

```sh
ulimit -l

# 512 ## sample output
```

If the `memlock` resource limit (rlimit) is lesser than 512 KiB, you can increase it as follows:
```sh
sudo vi /etc/security/limits.conf
*    hard    memlock        512
*    soft    memlock        512
```

To make the new limits effective, you need to log in to the machine again. Verify whether the limits
have been reflected with `ulimit` as described above.

>(On old WSL versions, you might need to spawn a login shell every time for the limits to be
>reflected:
>```sh
>su ${USER} -l
>```
>The limits persist once inside the login shell. This is not necessary on the latest WSL2 version as
>of 22.12.2022)

Finally, clone the repository and run the tests:
```sh
git clone https://github.com/arindas/laminarmq.git
cd laminarmq/
cargo test
```

## License

`laminarmq` is licensed under the MIT License. See [License](./LICENSE) for more details.
