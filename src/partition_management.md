# Partition control flow and replication

![partition-control-flow-replication](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-partition-control-flow-and-replication.svg)
<p align="center">
<b>Fig:</b> Partition serving future control flow and partition replication mechanism in
<code>laminarmq</code>
</p>

The partition controller future receives membership event requests `membership::{join, leave,
update_weight}` or `(paritition::*, tx)` requests from the request router future.

The partition request handler handles the different requests as follows:

- Idempotent `partition::*_request`: performs the necessary idempotent operation on the underlying
  segmented log and responds with the result on the response channel.

- Non-idempotent `partition::*_request`: Leader and follower replicas handle non-idempotent replicas
  differently:

  - Leader replicas: Replicates non-idempotent operations on all follower partitions in the Raft
    group if this partition is a leader, and then applies the operation locally. This might involve
    first sending an Raft append request, locally writing once majority of replicas respond back,
    then commit-ing it locally and finally relay the commit to all other replicas. Leader replicas
    respond to requests only from clients. Non-idempotent requests from follower replicas are
    ignored.
  - Follower replicas: Follower replicas respond to non-idempotent requests only from leaders.
    Non-idempotent from clients are redirected to the leader. A follower replica handles
    non-idempotent requests by applying the changes locally in accordance with Raft.

  Once the replicas are done handling the request, they send back an appropriate response to the
  response channel, if present. (A redirect response is also encoded properly and sent back to the
  response channel).

- `membership::join(i)`: add {node #i} to local priority queue. If the required number of replicas
  is more than the current number, pop() one member from the priority queue and add it to the Raft
  group (making it an eligible candidate in the Raft leader election process). If the current
  replica is a leader, we send a `partition::create(...)` request. If there is no leader among the
  replicas, we initial the leadership election process with each replica as a candidate.

- `membership::leave(j)`: remove {node #j} from priority queue and Raft group if present. If `{node
  #j}` was not present in the Raft group no further action is necessary. If it was present in the
  Raft group, `pop()` another member from the priority queue, add it to the Raft group and proceed
  similarly as in the case of `membership::join(j)`

- `membership::update_weight(k)`: updates priority for `{node #k}` by recomputing rendezvous_hash
  for {node #k} with this partition replicas partition_id. Next, if any node in the priority queue
  has a higher priority than any of the nodes in the Raft group, the node with the least priority
  is replaced by the highest priority element from the queue. We send a
  `partition::remove(partition_id, ...)` request to `{node #k}`. Afterwards we proceed similarly
  to `membership::{leave, join}` requests.

When a node goes down the appropriate `membership::leave(i)` message (where `i` is the node that
went down) is sent to all the nodes in the cluster. The partition replica controllers in each node
handle the membership request accordingly. In effect:
- For every leader partition in that node:
  - if there are no other follower replicas in other nodes in it's Raft group, that partition goes
    down.
  - if there are other follower replicas in other nodes, there are leader elections among them and
    after a leader is elected, reads and writes for that partition proceed normally
- For every follower partition in that node:
  - the remaining replicas in the same raft group continue to function in accordance with Raft's
    mechanisms.

For each of the partition replicas on the node that went down, new host nodes are selected using
rendezvous hash priority.

In our system, we use different Raft groups for different data buckets (replica groups).
[CockroachDB](https://www.cockroachlabs.com/) and [Tikv](https://tikv.org) call this manner of using
different Raft groups for different data buckets on the same node as MultiRaft.

Read more here:
- <https://tikv.org/deep-dive/scalability/multi-raft/>
- <https://www.cockroachlabs.com/blog/scaling-raft/>

Every partition controller is backed by a `segmented_log` for persisting records.

