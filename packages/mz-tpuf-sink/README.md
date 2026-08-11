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
