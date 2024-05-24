# laminarmq-tokio-commit-log-server

A simple persistent commit log server using the tokio runtime.

## Endpoints

This server exposes the following HTTP endpoints:

```rust
.route("/index_bounds", get(index_bounds))  // obtain the index bounds
.route("/records/:index", get(read))        // obtain the record at given index
.route("/records", post(append))            // append a new record at the end of the commit log

.route("/rpc/truncate", post(truncate))     // truncate the commit log
                                            // expects JSON: { "truncate_index": <idx: number> }
                                            // records starting from truncate_index are removed
```

## Usage

Run the server as follows:

```sh
cargo run --example laminarmq-tokio-commit-log-server --release
```

The server optionally expects an environment variable: `STORAGE_DIRECTORY`.

The default value is:

```rust
const DEFAULT_STORAGE_DIRECTORY: &str = "./.storage/laminarmq_tokio_commit_log_server/commit_log";
```

You may specify it as follows:

```sh
STORAGE_DIRECTORY="<storage directory>" cargo run --release
```

Once the server is running you may make requests as follows:

```sh
curl -w "\n" "http://127.0.0.1:3000/index_bounds"

curl -w "\n" --request POST --data "Hello World" "http://127.0.0.1:3000/records"
curl -w "\n" --request POST --data "Moshi moshi" "http://127.0.0.1:3000/records"
curl -w "\n" --request POST --data "Bonjour  <3" "http://127.0.0.1:3000/records"

curl -w "\n" "http://127.0.0.1:3000/index_bounds"

curl -w "\n" "http://127.0.0.1:3000/records/1"

curl -w "\n" --header "Content-Type: application/json" --request POST \
    --data "{\"truncate_index\": 1}" \
    "http://127.0.0.1:3000/rpc/truncate"

curl -w "\n" "http://127.0.0.1:3000/index_bounds"
```

Here's what's happening above:

- First request find the index_bounds, (highest_index) is exclusive
- We append three records with the given data
- We lookup the current index_bounds after appending to the commit_log
- We read the record at index 1
- We truncate the commit_log at index 1. All records starting from index 1 are
  removed. After this operation the bounds are [0, 1)
- We lookup the current index_bounds after truncating the commit_log

> Note: The `-w "\n"` flag is for appending a "\n" to the output of curl. This way
> the output is more readable.
