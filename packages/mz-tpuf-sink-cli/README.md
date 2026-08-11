# mz-tpuf-sink-cli

Command-line runner for [`mz-tpuf-sink`](../mz-tpuf-sink), which atomically
sinks a [Materialize](https://materialize.com) Kafka topic into
[turbopuffer](https://turbopuffer.com).

This package is a thin wrapper: it loads configuration from the environment,
sets up logging, installs `SIGINT`/`SIGTERM` handlers, and calls
`run_sink()`. Everything else lives in the library.

## Usage

```sh
uv tool install mz-tpuf-sink-cli
mz-tpuf-sink
```

```
Options:
  --log-level TEXT  [default: INFO]
  --version         Show the version and exit.
  --help            Show this message and exit.
```

Configuration comes from environment variables, or a `.env` file in the
working directory:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `MZ_TPUF_KAFKA_BOOTSTRAP_SERVERS` | yes | | Kafka bootstrap servers |
| `MZ_TPUF_KAFKA_TOPIC` | yes | | Topic produced by the Materialize sink |
| `MZ_TPUF_KAFKA_GROUP_ID` | | `mz-tpuf-sink` | Consumer group ID |
| `MZ_TPUF_SCHEMA_REGISTRY_URL` | yes | | Confluent Schema Registry URL |
| `MZ_TPUF_SCHEMA_REGISTRY_AUTH` | | | `user:password` for the registry |
| `MZ_TPUF_MATERIALIZE_DSN` | yes | | e.g. `postgres://materialize@localhost:6875/materialize` |
| `MZ_TPUF_MATERIALIZE_SINK` | yes | | Sink, fully qualified as `database.schema.sink` |
| `MZ_TPUF_TURBOPUFFER_API_KEY` | yes | | turbopuffer API key |
| `MZ_TPUF_TURBOPUFFER_REGION` | | | turbopuffer region (e.g. `aws-us-east-1`) |
| `MZ_TPUF_TURBOPUFFER_BASE_URL` | | | Overrides region; useful for testing |
| `MZ_TPUF_NAMESPACE` | yes | | Target turbopuffer namespace |
| `MZ_TPUF_MAX_ROWS_PER_REQUEST` | | `10000` | Chunking limit (rows) |
| `MZ_TPUF_MAX_BYTES_PER_REQUEST` | | `200 MiB` | Chunking limit (bytes) |
| `MZ_TPUF_BUFFER_WARN_BYTES` | | `1 GiB` | Warn when buffered transactions exceed this |
| `MZ_TPUF_POLL_TIMEOUT` | | `1.0` | Kafka poll timeout (seconds) |
| `MZ_TPUF_FRONTIER_READY_TIMEOUT` | | `15.0` | Fail startup if no frontier row arrives (wrong sink name) |

Run **one process per topic**, one topic per namespace.

See the [repository README](https://github.com/MaterializeInc/mz-turbopuffer-sink)
for the design and delivery semantics.
