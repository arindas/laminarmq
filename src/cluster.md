# Cluster Hierarchy

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
