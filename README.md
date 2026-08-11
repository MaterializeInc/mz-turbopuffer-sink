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
| `timestamp`/`date`/`time` logical types | `datetime` (sent as ISO-8601) |
| `decimal` | `float` |
| `float`/`double` | `float` |
| `int`/`long` | `int` |
| `boolean` | `bool` |
| `bytes` | base64 `string` |
| nested records / maps | JSON `string` |
| arrays of primitives | `[]string` / `[]int` / `[]float` / … |
| arrays of records | JSON `string` |

A value column named `id` is not treated as an attribute; the document ID
always comes from the Kafka key. The sink logs a warning once if a value
column named `id` is dropped because the key column has a different name.

`numeric` becomes a turbopuffer `float`, so range filters and sorting work
(`["price", "Gt", 10]`). Materialize encodes unconstrained `numeric` as an Avro
decimal with scale 39, so `9.99` arrives as
`9.990000000000000000000000000000000000000`; the float conversion drops that
padding. Values carrying more than 17 significant digits cannot round-trip
through a float, and the sink warns once when it sees one.

### Attribute schema is declared, not inferred

On startup the sink reads the topic's registered Avro value schema and derives
an explicit turbopuffer attribute schema from it (logged at INFO), which it
sends with every write.

This is not an optimization — it is required for correctness. turbopuffer
otherwise infers each attribute's type from the first value it sees, and an
integral float is indistinguishable from an integer on the wire: a first row
with `price = 5.00` pins the attribute to `int`, and the next row's `9.99` is
then rejected with `number cannot be represented as signed 64-bit integer`,
stalling the sink. Declaring the type up front also fixes columns whose first
observed value is `NULL`, which offer nothing to infer from.

## Development

```sh
uv run pytest          # unit tests only; no Docker, no network
```

The unit suite covers the completeness rule, backpressure, per-key merging,
Debezium diffing, ID derivation, schema derivation, chunking, and retry
behavior. One test drives the real turbopuffer SDK client over a mock HTTP
transport so the request shape and URL routing are checked without network
access.

### End-to-end test

`e2e/` runs the whole pipeline — Materialize → Redpanda (Kafka + Schema
Registry) → this sink → **real turbopuffer**:

```sh
echo 'MZ_TPUF_TURBOPUFFER_API_KEY=tpuf_...' > e2e/.env   # gitignored
echo 'MZ_TPUF_TURBOPUFFER_REGION=aws-us-east-1'         >> e2e/.env
uv run pytest e2e -v -s
```

It needs Docker, takes about 45 seconds, and is skipped if no API key is
present. It writes to a throwaway namespace (`mz-tpuf-e2e-<timestamp>`) and
deletes it afterwards, so it cannot touch existing data.

The topic is deliberately created with **3 partitions** while the test uses
only a few keys, so at least one partition stays empty — that is what
exercises frontier-based settlement for idle partitions. The scenario asserts:

1. A multi-row insert lands with correct type mapping.
2. An update is a genuine **column-level patch**: the test writes a `sidecar`
   attribute directly to turbopuffer that Materialize knows nothing about, then
   updates one column and asserts the sidecar survived. A full-row upsert would
   have destroyed it.
3. A delete removes exactly one document.
4. A statement touching two rows produces **one** turbopuffer request carrying
   two operations (asserted against the sink's own log).
5. An insert after an idle period still lands promptly, proving empty and idle
   partitions settle rather than stalling the sink.
6. Offsets are actually committed to Kafka, so a restart resumes instead of
   replaying the topic.
