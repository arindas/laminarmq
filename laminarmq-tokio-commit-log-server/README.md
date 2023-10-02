# laminarmq-tokio-commit-log-server

A simple in memory commit log server using the tokio runtime.

## Endpoints

This server exposes the following endpoints:

```rust
.route("/index_bounds", get(index_bounds))  // obtain the index bounds
.route("/records/:index", get(read))        // obtain the record at given index
.route("/records", post(append))            // append a new record at the end of the commit log

.route("/rpc/truncate", post(truncate))     // truncate the commit log 
                                            // expects JSON: { truncate_index: <idx> }
```

## Usage

Run the server as follows:

```sh
cd laminarmq-tokio-commit-log-server/
cargo run
```
