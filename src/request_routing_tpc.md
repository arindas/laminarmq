# TPC execution model compatible design

![request-routing-thread-per-core](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-node-request-routing-thread-per-core.svg)
<p align="center">
<b>Fig:</b> Request routing mechanism in <code>laminarmq</code> nodes using the thred per core
execution model.
</p>

In the thread per core execution model each thread is responsible for a subset of the partitions.
Hence each thread has it's own "Request Router / Partition Manager", "HTTP Server" and a set of
partition serving futures. We run multiple such threads on different processor cores.

Now, as discussed before we need to route individual requests to the correct destination thread to
ensure request locality. We use a dedicated "Thread Router" eBPF XDP filter to route partition
request packets to their destination threads.

The "Thread Router" eBPF XDP filter keeps a eBPF `sockmap` which contains the sockets where each of
the threads listen to for requests. For every incoming request, we route it to its destination
thread using this `sockmap`. Now we can again leverage rendezvous hashing here to determine the
thread to be used for a request. We use the `partition_id` and `thread_id` for rendezvous hashing.
Since all the threads run on different processor cores, they will have similar request handling
capacity and hence will have equal weights. Using this, requests belonging to a particular partition
will always be routed to the same thread on a particular node. This ensures a high level of request
locality.

The remaining components behave as discussed above. Notice how we are able to reuse the same
components in a drastically different execution model, as promised before.
