# mz-tpuf-sink

Atomically sink a [Materialize](https://materialize.com) Kafka sink topic into
[turbopuffer](https://turbopuffer.com).

Materialize stamps every message it emits with a `materialize-timestamp`
header; all messages sharing a timestamp were committed atomically in one
Materialize transaction. This service reconstructs those transactions from the
topic and applies each one as **a single atomic turbopuffer write request**:

- **insert** (`before: null`) → full-row upsert
- **update** (`before` + `after`) → **column-level patch** containing only the
  columns that actually changed
- **delete** (`after: null`) → delete by ID

It is generic over the schema: the topic must be `FORMAT AVRO` with
`ENVELOPE DEBEZIUM` (and a `KEY`), registered in a Confluent-compatible Schema
Registry. Every Avro field maps to a turbopuffer attribute.

## Requirements

- Python ≥ 3.12, [uv](https://docs.astral.sh/uv/)
- A Materialize sink like:

  ```sql
  CREATE SINK events_sink
    FROM my_view
    INTO KAFKA CONNECTION kafka_conn (TOPIC 'events')
    KEY (id)
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    ENVELOPE DEBEZIUM;
  ```

- A SQL connection to the same Materialize instance (used only to `SUBSCRIBE`
  to the sink's write frontier).

## Running

```sh
uv sync
uv run mz-tpuf-sink
```

Configuration is environment variables (or a `.env` file):

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `MZ_TPUF_KAFKA_BOOTSTRAP_SERVERS` | yes | | Kafka bootstrap servers |
| `MZ_TPUF_KAFKA_TOPIC` | yes | | Topic produced by the Materialize sink |
| `MZ_TPUF_KAFKA_GROUP_ID` | | `mz-tpuf-sink` | Consumer group ID |
| `MZ_TPUF_SCHEMA_REGISTRY_URL` | yes | | Confluent Schema Registry URL |
| `MZ_TPUF_SCHEMA_REGISTRY_AUTH` | | | `user:password` for the registry |
| `MZ_TPUF_MATERIALIZE_DSN` | yes | | e.g. `postgres://materialize@localhost:6875/materialize` |
| `MZ_TPUF_MATERIALIZE_SINK` | yes | | Sink name as it appears in `mz_sinks.name` |
| `MZ_TPUF_TURBOPUFFER_API_KEY` | yes | | turbopuffer API key |
| `MZ_TPUF_TURBOPUFFER_REGION` | | | turbopuffer region (e.g. `gcp-us-central1`) |
| `MZ_TPUF_TURBOPUFFER_BASE_URL` | | | Overrides region; useful for testing |
| `MZ_TPUF_NAMESPACE` | yes | | Target turbopuffer namespace |
| `MZ_TPUF_MAX_ROWS_PER_REQUEST` | | `10000` | Chunking limit (rows) |
| `MZ_TPUF_MAX_BYTES_PER_REQUEST` | | `200 MiB` | Chunking limit (bytes) |
| `MZ_TPUF_BUFFER_WARN_BYTES` | | `1 GiB` | Warn when buffered transactions exceed this |
| `MZ_TPUF_POLL_TIMEOUT` | | `1.0` | Kafka poll timeout (seconds) |
| `MZ_TPUF_FRONTIER_READY_TIMEOUT` | | `15.0` | Fail startup if no frontier row arrives (wrong sink name) |

Run **one process per topic**, one topic per namespace.

## How it works

```
Kafka consumer ──▶ Avro/Debezium decoder ──▶ TransactionBuffer (by ts)
                                                   │  flush when complete
mz SUBSCRIBE mz_frontiers ──▶ FrontierWatcher ─────┘
                                                   ▼
                                  one atomic turbopuffer write per ts
                                                   ▼
                                        commit Kafka offsets
```

**Transaction completeness.** Within a partition, Materialize emits messages
in non-decreasing timestamp order. A timestamp `T` is complete once every
assigned partition has either (a) yielded a message with `ts > T`, or (b) been
consumed to its high watermark while the Materialize sink's *write frontier*
(observed via one long-lived
`SUBSCRIBE (... mz_internal.mz_frontiers JOIN mz_sinks ...)`) has passed `T`.
Rule (b) is what makes idle partitions and quiet topics deterministic — no
flush-timeout heuristics.

**Backpressure.** A partition may read the oldest unflushed timestamp plus one
timestamp ahead; beyond that it is `pause()`d until the writer catches up, so
memory is bounded by roughly two transactions per partition.

## Delivery semantics

- **At-least-once, effectively exactly-once for state.** Offsets are committed
  only after the transaction is written. On restart, replayed messages
  re-issue the same upserts/patches/deletes, which are idempotent.
- **Atomicity.** One Materialize timestamp = one turbopuffer write request,
  which turbopuffer applies atomically. The exception is a transaction larger
  than the configured request limits (notably the initial snapshot): it is
  split into sequential requests and readers may briefly observe a partial
  state for *that timestamp only*; offsets still commit only after the last
  chunk.
- **Single writer.** Run one instance per consumer group/namespace. turbopuffer
  has no request-level compare-and-swap, so concurrent writers to the same
  namespace are not fenced.
- On rebalance, all buffered state is dropped and uncommitted messages are
  redelivered, so a stale consumer can never overwrite newer writes.

## Data mapping

| Source | turbopuffer |
| --- | --- |
| Kafka key: single `int`/`long` column | u64 ID (negative values are an error) |
| Kafka key: single `string` column | string ID (>64 bytes → deterministic UUIDv5) |
| Kafka key: single `uuid` logical type | UUID ID |
| Kafka key: composite | canonical-JSON string ID (>64 bytes → UUIDv5) |
| `timestamp`/`date`/`time` logical types | ISO-8601 string |
| `decimal` | string |
| `bytes` | base64 string |
| nested records / maps | JSON string |
| arrays of primitives | arrays |
| everything else | as-is |

A value column named `id` is not treated as an attribute; the document ID
always comes from the Kafka key.

## Development

```sh
uv run pytest
```

The suite covers the completeness rule, backpressure, per-key merging,
Debezium diffing, ID derivation, chunking, and retry behavior with fakes — no
network required.

### Manual end-to-end check

1. Start Materialize (or the emulator) + Redpanda + Schema Registry.
2. Create a table, a view, and an Avro/Debezium sink as above.
3. Export the `MZ_TPUF_*` variables (a real `MZ_TPUF_TURBOPUFFER_API_KEY` and
   namespace) and run `uv run mz-tpuf-sink --log-level DEBUG`.
4. `INSERT`/`UPDATE`/`DELETE` rows in Materialize inside and outside of
   explicit transactions; confirm documents appear/patch/disappear in
   turbopuffer and that multi-row transactions land together.
