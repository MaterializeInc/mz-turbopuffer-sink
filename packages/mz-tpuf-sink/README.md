# mz-tpuf-sink

Atomically sink a [Materialize](https://materialize.com) Kafka sink topic into
[turbopuffer](https://turbopuffer.com).

Materialize stamps every message it emits with a `materialize-timestamp`
header; all messages sharing a timestamp were committed atomically in one
Materialize transaction. This library reconstructs those transactions from the
topic and applies each one as **a single atomic turbopuffer write request**:
inserts become full-row upserts, updates become **column-level patches** diffed
from the Debezium `before`/`after`, and deletes become deletes.

For a command-line runner, install [`mz-tpuf-sink-cli`](../mz-tpuf-sink-cli)
instead.

## Usage

```python
from mz_tpuf_sink import SinkConfig, run_sink

run_sink(SinkConfig(
    kafka_bootstrap_servers="localhost:9092",
    kafka_topic="events",
    schema_registry_url="http://localhost:8081",
    materialize_dsn="postgres://materialize@localhost:6875/materialize",
    materialize_sink="materialize.public.events_sink",   # database.schema.sink
    turbopuffer_api_key="tpuf_...",
    turbopuffer_region="aws-us-east-1",
    namespace="events",
))
```

`run_sink` blocks the calling thread. To shut it down, pass a
`threading.Event` and set it from another thread or a signal handler:

```python
import signal, threading

stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())
run_sink(config, stop=stop)
```

The library never reads the environment, configures logging, or installs
signal handlers — those belong to the program embedding it. Startup problems
(an unreachable broker, a sink name matching nothing, a composite key) raise
rather than retrying forever.

## Transforms: derived attributes and embeddings

A transform computes extra turbopuffer attributes from a record's columns. It
runs **after** the Debezium diff, so an update that did not touch its source
columns never recomputes it — which is the point when each computation is an
embedding API call.

```python
from mz_tpuf_sink import FunctionTransform, SinkConfig, run_sink

embedding = FunctionTransform(
    name="title_embedding",
    sources=("title", "description"),          # columns it reads
    schema={"embedding": {"type": "[1536]f32", "ann": True}},
    distance_metric="cosine_distance",
    batch_size=256,
    compute=lambda rows: [                     # ONE call per batch
        {"embedding": vector}
        for vector in my_model.embed(
            [f"{r['title']} {r['description']}" for r in rows]
        )
    ],
)

run_sink(config, transforms=[embedding])
```

Each `row` holds exactly the declared `sources` plus `"id"`, with values already
in turbopuffer form — datetimes are ISO strings, `numeric` is a float, `bytes` is
base64. Return one mapping per row, in order, containing exactly the attributes
your `schema` declares. Returning `None` for one clears it.

**When it recomputes.** An insert always computes. An update computes only when
*all* of a transform's sources are present in the change — the sink retains the
unchanged siblings of a multi-column source so `compute` always sees complete
input, and never fires a transform on a partial one.

**Vectors force a full-document rewrite.** turbopuffer cannot patch a vector, so
when a change touches a vector transform's source the record is written as a
whole-document upsert rather than a column patch. Two consequences: attributes
written to turbopuffer out of band are lost for that document, and every
transform recomputes for it (an upsert replaces the document, so it must carry
them all). Updates that miss those sources stay patches and leave the stored
vector untouched — that is what keeps unrelated updates cheap. A namespace
supports at most two vector attributes, and a vector needs both `ann: True` and
a `distance_metric`.

Validation happens at startup — unknown source columns, output names colliding
with table columns, chaining one transform's output into another's input, a
missing `distance_metric` — so a mistake is a startup error, not a crash on the
first record.

**Failures stop the sink.** If `compute` raises, the sink raises `TransformError`
naming the transform and documents, and offsets are not committed, so the
timestamp replays on restart. There is no framework-level retry: your embedding
SDK already has one. A permanently failing record is therefore a poison pill —
fix it upstream.

Things to know: adding a transform does **not** backfill existing documents, they
get the attribute the next time they change; a crash during a large snapshot
re-embeds it on restart, so cache inside `compute` if that is expensive;
parallelism belongs inside `compute`, which is why it takes a batch; and a long
`compute` can outlast Kafka's `max.poll.interval.ms` and get the consumer
evicted, so raise `kafka_max_poll_interval_ms` or lower `batch_size` (the sink
warns as it approaches half the budget).

## What it requires

The topic must be produced by a Materialize Kafka sink using `FORMAT AVRO`
with `ENVELOPE DEBEZIUM` and a single-column `KEY`, registered in a
Confluent-compatible Schema Registry:

```sql
CREATE SINK events_sink
  FROM my_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'events')
  KEY (id)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
```

It is otherwise generic over your schema: attribute types are derived from the
registered Avro schema at startup.

Run **one instance per topic**, one topic per namespace.

See the [repository README](https://github.com/MaterializeInc/mz-turbopuffer-sink)
for the completeness and backpressure design, delivery semantics, and the full
type-mapping table.
