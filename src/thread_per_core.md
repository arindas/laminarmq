# Thread Per Core

![async-execution-model-thread-per-core](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-async-execution-model-thread-per-core.svg)
<p align="center">
<b>Fig:</b> Thread per core async runtime based execution model for <code>laminarmq</code>
</p>

In the thread per core model since each processor core is limited to a single thread, tasks in a
thread need to be scheduled efficiently. Hence each worker thread runs their own task scheduler.

We currently use [`glommio`](https://docs.rs/glommio) as our thread-per-core runtime.

Here, tasks can be scheduled on different task queues and different task queues can be provisioned
with specific fractions of CPU time shares. Generally tasks with similar latency profiles are
executed on the same task queue. For instance web server tasks will be executed on a different queue
than the one that runs tasks for persisting data to the disk.

We re-use the same constructs that we use in the general async runtime execution model. The only
difference being, we explicitly care about in which task queue a class of future's tasks are
executed. In our case, we have the following 4 task queues:
- Request router task queue
- HTTP server request parser task queue
- Partition replica controller task queue
- Response poller task queue

Each of these task queue can be assigned specific fractions of CPU time shares. `glommio` also
provides utilities for automatically deducing these CPU time shares based on their runtime latency
profiles.

Apart from this `glommio` leverages the new linux 5.x [`io_uring`](https://kernel.dk/io_uring.pdf)
API which facilitates true asynchronous IO for both networking and disk interfaces. (Other `async`
runtimes such as [`tokio`](https://docs.rs/tokio) make blocking system calls for disk IO operations
in a thread-pool.)

`io_uring` also has the advantage of being able to queue together multiple system calls together and
then asynchronously wait for their completion by making a maximum of one context switch. It is also
possible to avoid context switches altogether. This is achieved with a pair of ring buffers called
the submission-queue and the completion-queue. Once the queues are set up, user can queue multiple
system calls on the submission queue. The linux kernel processes the system calls and places the
results in the completion queue. The user can then freely read the results from the
completion-queue. This entire process after setting up the queues doesn't require any additional
context switch.

Read more: <https://man.archlinux.org/man/io_uring.7.en>

`glommio` presents additional abstractions on top of `io_uring` in the form of an async runtime,
with support for networking, disk IO, channels, single threaded locks and more.

Read more: <https://www.datadoghq.com/blog/engineering/introducing-glommio/>
