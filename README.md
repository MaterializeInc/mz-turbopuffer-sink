# mz-tpuf-sink

Keep a [turbopuffer](https://turbopuffer.com) namespace in sync with a
[Materialize](https://materialize.com) view.

Materialize maintains your view incrementally as the underlying data changes.
This project mirrors that view into turbopuffer, so a namespace you search
against is always a current reflection of a query you already trust.

```sh
uv add "mz-tpuf-sink @ git+https://github.com/MaterializeInc/mz-turbopuffer-sink"
```

```python
from mz_tpuf_sink import SinkConfig, run_sink

run_sink(SinkConfig(...))   # blocks until stopped
```

## What it gives you

Transactions arrive whole. Materialize commits a batch of changes at a single
timestamp, and the sink applies that timestamp to turbopuffer as one write. A
statement that changes fifty rows shows up as fifty changed documents at once,
and a row that leaves the view leaves the namespace. A search never sees half of
an update.

Embeddings are recomputed only when their text changes. The sink sees which
columns moved in each record, so it re-embeds the records whose source text
changed and leaves every other vector alone. Edit an article's `title` and it is
re-embedded; change its `view_count` a thousand times and it is not embedded
once. The bill tracks edits to the text rather than writes to the table.

## Embeddings

The reason to put a view in turbopuffer is usually vector search, and the
expensive part of vector search is embedding. A **transform** computes derived
attributes from a record's columns — and only runs when the columns it reads
actually change.

```python
from mz_tpuf_sink import FunctionTransform, SinkConfig, run_sink
from openai import OpenAI

client = OpenAI()

def embed(rows):
    """Called once per batch of records, never once per row."""
    text = [f"{row['title']}\n\n{row['description']}" for row in rows]
    response = client.embeddings.create(model="text-embedding-3-small", input=text)
    return [{"embedding": item.embedding} for item in response.data]

article_embedding = FunctionTransform(
    name="article_embedding",
    sources=("title", "description"),                       # columns it reads
    schema={"embedding": {"type": "[1536]f32", "ann": True}},
    distance_metric="cosine_distance",
    batch_size=256,
    compute=embed,
)

run_sink(
    SinkConfig(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="articles",
        schema_registry_url="http://localhost:8081",
        materialize_dsn="postgres://materialize@localhost:6875/materialize",
        materialize_sink="materialize.public.articles_sink",
        turbopuffer_api_key="tpuf_...",
        turbopuffer_region="aws-us-east-1",
        namespace="articles",
    ),
    transforms=[article_embedding],
)
```

Each row handed to `compute` holds exactly the columns named in `sources`, plus
`id`. Return one mapping per row, in the same order, containing the attributes
named in `schema`.

A transform is ordinary Python, so it can call any model, local or hosted, and
it receives records in batches so one API call covers many documents. It can
produce anything, not just vectors — a slug, a sentiment score, a translated
title.

## What you need

- **A Materialize sink** publishing the view you want mirrored:

  ```sql
  CREATE SINK articles_sink
    FROM articles_view
    INTO KAFKA CONNECTION kafka_conn (TOPIC 'articles')
    KEY (id)
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    ENVELOPE DEBEZIUM;
  ```

  `KEY` must name exactly one column, and its value becomes the turbopuffer
  document id. Integers, strings up to 64 bytes, and UUIDs all work.

- **A connection to that Materialize instance**, so the sink knows when a
  transaction is complete.

- **A turbopuffer API key**, and Python 3.12+.

Run one process per topic, writing to one namespace.

## Type mapping

Column types are read from the Avro schema the sink publishes to Schema
Registry and declared to turbopuffer, so numbers stay numbers and timestamps
stay timestamps, filterable and sortable. There is no mapping file to maintain,
and adding a column to your view needs no change here.

| Materialize | turbopuffer |
| --- | --- |
| `text` | `string` |
| `int`, `bigint` | `int` |
| `numeric`, `float`, `double` | `float` |
| `boolean` | `bool` |
| `timestamp`, `timestamptz`, `date` | `datetime` |
| `time` | `int` (microseconds since midnight) |
| `bytea`, `interval` | base64 `string` |
| lists | `[]string`, `[]int`, `[]float`, … |
| records, maps, lists of records | JSON `string` |

A namespace holds at most two vector attributes, and a vector needs both
`ann: True` and a `distance_metric`.

## Development

```sh
uv sync
uv run pytest              # unit tests: no Docker, no network
uv run pytest e2e -v -s    # full pipeline against real turbopuffer
```

The end-to-end suite needs Docker and a turbopuffer key in `e2e/.env`
(gitignored); it creates throwaway namespaces and deletes them afterwards, and
skips itself when no key is present.

CI runs both suites on every pull request. The end-to-end job reads a
`TURBOPUFFER_API_KEY` repository secret, which GitHub does not expose to pull
requests from forks — those runs skip the end-to-end tests rather than failing.
