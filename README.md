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
`ENVELOPE DEBEZIUM` (and a single-column `KEY`), registered in a
Confluent-compatible Schema Registry. Every Avro field maps to a turbopuffer
attribute.

## Packages

This repository is a uv workspace holding two distributions:

| Package | What it is |
| --- | --- |
| [`packages/mz-tpuf-sink`](packages/mz-tpuf-sink) | The library. Exposes `SinkConfig` and `run_sink(config, stop=None)` — no CLI, no environment reading. |
| [`packages/mz-tpuf-sink-cli`](packages/mz-tpuf-sink-cli) | A thin wrapper providing the `mz-tpuf-sink` command: loads `MZ_TPUF_*` settings, sets up logging and signal handlers, calls `run_sink`. |

Embedding it in your own process:

```python
from mz_tpuf_sink import SinkConfig, run_sink

run_sink(SinkConfig(
    kafka_bootstrap_servers="localhost:9092",
    kafka_topic="events",
    schema_registry_url="http://localhost:8081",
    materialize_dsn="postgres://materialize@localhost:6875/materialize",
    materialize_sink="materialize.public.events_sink",
    turbopuffer_api_key="tpuf_...",
    turbopuffer_region="aws-us-east-1",
    namespace="events",
))
```

`run_sink` blocks; pass a `threading.Event` as `stop` to shut it down from
another thread. The library deliberately does not read the environment,
configure logging, or install signal handlers — those are the caller's to own,
which is exactly what the CLI package supplies.

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

  Configure it as `MZ_TPUF_MATERIALIZE_SINK=materialize.public.events_sink`.
  The sink is looked up by database, schema, name, **and** topic, so a name
  that resolves to a sink writing somewhere else fails at startup instead of
  tracking the wrong frontier.

- A SQL connection to the same Materialize instance (used only to `SUBSCRIBE`
  to the sink's write frontier).

## Running

```sh
uv sync
uv run mz-tpuf-sink
```

Configuration comes from `MZ_TPUF_*` environment variables or a `.env` file;
the full table lives in the
[CLI package README](packages/mz-tpuf-sink-cli/README.md).

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
- **Committed records only.** Materialize writes its sink topic using Kafka
  transactions, so the consumer pins `isolation.level=read_committed` and never
  applies records from aborted transactions. (librdkafka already defaults to
  `read_committed`, unlike the Java client, but the sink sets it explicitly
  rather than depending on that.)
- On rebalance, all buffered state is dropped and uncommitted messages are
  redelivered, so a stale consumer can never overwrite newer writes.

## Data mapping

| Source | turbopuffer |
| --- | --- |
| Kafka key: `int`/`long` column | `uint` ID (negative values are an error) |
| Kafka key: `string` column | `string` ID, verbatim (max 64 bytes) |
| Kafka key: `uuid` logical type | `uuid` ID |
| `timestamp`/`timestamptz`/`date` | `datetime` (sent as ISO-8601) |
| `time` | `int` (Materialize sends an untagged microsecond count) |
| `interval` | base64 `string` (Avro `fixed`) |
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

### The sink's KEY must be one column

A turbopuffer document id is a single value, and this sink never invents an
encoding for one. The key is used verbatim, so:

- **`KEY` must name exactly one column.** A composite key is rejected at
  startup. If you need one, derive a single key column in the upstream view
  and sink that.
- **A string key over 64 bytes is an error**, not a hash. Hashing would put
  two encodings into one id space, where a short key can equal another key's
  hash and the two rows would silently share — and overwrite — one document.
  Shorten the key upstream instead; sinking a hashed key column from the view
  makes the choice explicit and visible in your schema.
- **A negative integer key is an error**, since turbopuffer's numeric ids are
  unsigned.

These are startup or per-record failures on purpose: the sink stops rather
than writing a document whose identity it cannot guarantee is unique. Note a
per-record failure is a poison message — the sink exits, and on restart it
replays the same record and exits again — so an over-long key needs an
upstream fix, not a retry.

`numeric` becomes a turbopuffer `float`, so range filters and sorting work
(`["price", "Gt", 10]`). Materialize encodes unconstrained `numeric` as an Avro
decimal with scale 39, so `9.99` arrives as
`9.990000000000000000000000000000000000000`; the float conversion drops that
padding. Values carrying more than 17 significant digits cannot round-trip
through a float, and the sink warns once when it sees one.

### Attribute schema is declared, not inferred

On startup the sink reads the topic's registered Avro value schema and derives
an explicit turbopuffer attribute schema from it (logged at INFO), which it
sends with every write. The document `id` type is declared too, so a UUID key
is stored as a UUID rather than inferred as a string.

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

Both packages' tests run from the workspace root. The library suite covers the
completeness rule, backpressure, per-key merging, Debezium diffing, ID
derivation, schema derivation, chunking, and retry behavior; one test drives
the real turbopuffer SDK client over a mock HTTP transport so the request shape
and URL routing are checked without network access. The CLI suite covers
environment loading, `.env` handling, and that a signal requests a clean
shutdown.

To work on one package alone, `uv run --package mz-tpuf-sink pytest
packages/mz-tpuf-sink/tests`.

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
