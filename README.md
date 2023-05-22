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

[L] := leader
[F] := follower

The exact numerical ids don't have any pattern; there can be multiple topics, each of which can have
multiple partitions.
```

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

[L] := leader
[F] := follower
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
