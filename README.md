<p align="center">
  <img src="./assets/logo.png" alt="laminarmq">
</p>

<p align="center">
  <a href="https://github.com/arindas/generational-lru/actions/workflows/ci.yml">
  <img src="https://github.com/arindas/generational-lru/actions/workflows/ci.yml/badge.svg" />
  </a>
  <a href="https://codecov.io/gh/arindas/laminarmq" > 
  <img src="https://codecov.io/gh/arindas/laminarmq/branch/main/graph/badge.svg?token=6VLETF5REC"/> 
  </a>
  <a href="https://github.com/arindas/generational-lru/actions/workflows/rustdoc.yml">
  <img src="https://github.com/arindas/generational-lru/actions/workflows/rustdoc.yml/badge.svg" /> 
  </a>
</p>

<p align="center">
A scalable, distributed message queue powered by a segmented, partitioned, replicated and immutable log.
<br><i>This is currently a work in progress.</i>
</p>

## Planned Architecture
This section presents a brief overview on the different aspects of our message queue. This is only an outline of
the architecture that we have planned for `laminarmq` and it is subject to change as this project evolves.

### Storage Hierarchy
Data is persisted in `laminarmq` with the following hierarchy:

```text
[cluster]
├── node#001
│   ├── (topic#001 -> partition#001) [L]
│   │   └── log{[segment#001, segment#002, ...]}
│   ├── (topic#001 -> partition#002) [L]
│   │   └── log{[segment#001, segment#002, ...]}
│   └── (topic#002 -> partition#001) [F]
│       └── log{[segment#001, segment#002, ...]}
├── node#002
│   ├── (topic#001 -> partition#002) [F]
│   │   └── log{[segment#001, segment#002, ...]}
│   └── (topic#002 -> partition#001) [L]
│       └── log{[segment#001, segment#002, ...]}
└── ...other nodes
```

Every "partition" is backed by a persistent, segmented log. A log is an append only collection of "message"(s).
Messages in a "partition" are accessed using their "offset" i.e. location of the "message"'s bytes in the log.

### Replication and Partitioning (or redundancy and horizontal scaling)
A particular "node" contains some or all "partition"(s) of a "topic". Hence a "topic" is both partitioned and 
replicated within the nodes. The data is partitioned with the dividing of the data among the "partition"(s),
and replicated by replicating these "partition"(s) among the other "node"(s).

Each partition is part of a Raft cluster; e.g each replica of `(topic#001 -> partition#001)` is part of a Raft
cluster, while each replica of `(topic#002 -> partition#002)` are part of a different Raft cluster. A particular
"node" might host some Raft leader "partition"(s) as well as Raft follower "partition"(s). For instance in the
above example data persistence hierarchy, the `[L]` denote leaders, and the `[F]` denote followers.

If a node goes down:
- Any data not already replicated on other nodes from the partitions stored on it becomes unavailable.
- If the only partition leader goes down, the partition goes down. If there are other leader and followers still 
alive for that partition, there are leader elections among the partition replicas and they continue to service
reads and writes. When a new node joins they are again rebalanced and replicated as necessary.

### Service Discovery
Now we maintain a "member-list" abstraction of all "node"(s) which states which nodes are online in real time.
This "member-list" abstraction is able to respond to events such as a new node joining or leaving the cluster.
(It internally uses a gossip protocol for membership information dissemination.) This abstraction can also
handle queries like the different "partition"(s) hosted by a particular node. Using this we do the following:
- Create new replicas of partitions or rebalance partitions when a node joins or leaves the cluster.
- Identify which node holds which partition in real time. This information can be used for client side load
balancing when reading or writing to a particular partition.

### Data Retention SLA
A "segment_age" configuration is made available to configure the maximum allowed age of segments. Since all 
partitions maintain consistency using Raft consensus, they have completely identical message-segment distribution.
At regular intervals, segments with age greater than the specified "segment_age" are removed and the messages
stored in these segments are lost. A good value for "segment_age" could be `7 days`.

## Major milestones for `laminarmq`
- [ ] Single node end-to-end message queue functionality. This includes a RPC like web API for interacting with `laminarmq`.
- [ ] Service discovery with [SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf).
- [ ] Replication and consensus of replicated records with [Raft](https://raft.github.io/raft.pdf).

## Testing
After cloning the repository, simply run `cargo test` at the repository root.

## License
`laminarmq` is licensed under the MIT License. See [License](./LICENSE) for more details.
