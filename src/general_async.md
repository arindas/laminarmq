# General async runtime

![async-execution-model-general](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-async-execution-model-general.svg)
<p align="center">
<b>Fig:</b> General async runtime based execution model for <code>laminarmq</code>
</p>

This execution model is based on the executor, reactor, waker abstractions used by all rust async
runtimes. We don't have to specifically care about how and where a particular future is executed.

The data flow in this execution model is as follows:
- A HTTP server future parses HTTP requests from the request socket
- For every HTTP request it creates a new future to handle it
- The HTTP handler future sends the request and a response channel tx to the request router via a channel.
It also awaits on the response rx end.
- The request router future maintains a map of partition_id to designated request channel tx for each
partition controller future.
- For every partition request received it forwards the request on the appropriate partition request
channel tx. If a `partition::create(...)` request is received it creates a new partition controller
future.
- The partition controller future send back the response to the provided response channel tx.
- The response poller future received it and responds back with a serialized response to the socket.

All futures are spawned using the async runtime's designated `{…}::spawn(…)` method. We don't have
to specify any details as to how and where the future's corresponding task will be executed.
