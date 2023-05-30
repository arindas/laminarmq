# Supported execution models

`laminarmq` supports two execution models:
- General async execution model used by various async runtimes in the Rust ecosystem (e.g. `tokio`,
  `async-std` etc.)
- Thread per core execution model (hereafter referred to as TPC sometimes)

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

We realize that although the thread per core execution model has some inherent advantages, being
compatible with the existing Rust ecosystem will significantly increase adoption. Therefore, we have
designed our system with reusable components which can be organized to suit both execution models.
