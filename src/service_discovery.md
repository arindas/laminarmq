# Service discovery and partition distribution

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
