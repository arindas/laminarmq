# General design

![request-rouing-general](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-node-request-routing-general.svg)
<p align="center">
<b>Fig:</b> Request routing mechanism in <code>laminarmq</code> nodes using the general execution
model.
</p>

In our cluster, we have two kinds of requests:
- __membership requests__: used by the gossip style service discovery system for maintaining cluster
membership.
- __partition requests__: used to interact with `laminarmq` topic partitions.

We use an [eBPF](https://ebpf.io/what-is-ebpf/) XDP filter to classify request packets at the socket
layer into membership request packets and partition request packets. Next we use eBPF to route
membership packets to a different socket which is exclusively used by the membership management
subsystem in that node. The partition request packets are left to flow as is.

Next we have an "HTTP server", which parses the incoming partition request packets from the original
socket into valid `partition::*` requests. For every `partition::*` request, the HTTP server spawns
a future to handle it. This request handler future does the following:
- Create a new channel `(tx, rx)` for the request.
- Send the parsed partition request along with send end of the channel `(partition::*, tx)` to the
"Request Router" over the request router's receiving channel.
- Await on the recv. end of the channel created by this future for the response. `res = rx.await`
- When we receive the response from this future's channel, we serialize it and respond back to the
socket we had received the packets from.

Next we have a "Request Router / Partition manager" responsible for routing various requests to the
partition serving futures. The request router unit receives both `membership::*` requests from the
membership subsystem and `partition::*` requests received from the "HTTP server" request handler
futures (also called request poller futures from here on since they poll for the response from the
channel recv. `rx` end). The request router unit routes requests as follows:
- `membership::*` requests are broadcast to all the partition serving futures
- `(partition::*_request(partition_id_x, …), tx)` tuples are routed to their destination partitions
using the `partition_id`.
- `(partition::create(partition_id_x, …), tx)` tuples are handled by the request router/ partition
manager itself. For this, the request router / partition manager creates a new partition serving
future, allocates the required storage units or it and sends and appropriate response on `tx`.

Finally, the individual partition server futures receive both `membership::*` and `(partition::*,
tx)` requests as they come to our node and routed. They handle the requests as necessary and send a
response back to `tx` where applicable.
