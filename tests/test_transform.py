import pytest

from mz_tpuf_sink.ids import IdCodec
from mz_tpuf_sink.models import ChangeEvent, Delete, Patch, Upsert
from mz_tpuf_sink.transform import (
    FunctionTransform,
    TransformError,
    apply_transforms,
    retain_groups,
    validate_transforms,
    write_distance_metric,
)
from mz_tpuf_sink.translate import Translator

VECTOR2 = {"type": "[2]f32", "ann": True}


def transform(
    name="embed",
    sources=("title",),
    schema=None,
    compute=None,
    batch_size=100,
    distance_metric="cosine_distance",
):
    return FunctionTransform(
        name=name,
        sources=tuple(sources),
        schema=schema if schema is not None else {"embedding": dict(VECTOR2)},
        compute=compute or (lambda rows: [{"embedding": [1.0, 2.0]} for _ in rows]),
        batch_size=batch_size,
        distance_metric=distance_metric,
    )


def row_schema(*names):
    return {
        "type": "record",
        "name": "row",
        "fields": [{"name": n, "type": ["null", "string"]} for n in names],
    }


def table_schema(*names):
    return {n: {"type": "string"} for n in names}


# ------------------------------------------------------------------ validation


class TestValidation:
    def test_returns_schema_merged_with_produced_attributes(self):
        merged = validate_transforms(
            [transform()], table=table_schema("title", "id"), columns={"id", "title"}
        )
        assert merged["embedding"] == VECTOR2
        assert merged["title"] == {"type": "string"}

    def test_unknown_source_column_is_rejected(self):
        with pytest.raises(ValueError, match="nosuch"):
            validate_transforms(
                [transform(sources=("nosuch",))],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_unknown_source_error_lists_available_columns(self):
        with pytest.raises(ValueError, match="title"):
            validate_transforms(
                [transform(sources=("nosuch",))],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_id_as_a_source_is_rejected(self):
        with pytest.raises(ValueError, match="Kafka key"):
            validate_transforms(
                [transform(sources=("id",))],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_produced_attribute_colliding_with_a_column_is_rejected(self):
        with pytest.raises(ValueError, match="title"):
            validate_transforms(
                [transform(schema={"title": dict(VECTOR2)})],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_produced_attribute_named_id_is_rejected(self):
        with pytest.raises(ValueError, match="id"):
            validate_transforms(
                [transform(schema={"id": dict(VECTOR2)})],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_two_transforms_producing_the_same_attribute_are_rejected(self):
        with pytest.raises(ValueError, match="embedding"):
            validate_transforms(
                [transform(name="a"), transform(name="b")],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_duplicate_transform_names_are_rejected(self):
        with pytest.raises(ValueError, match="duplicate"):
            validate_transforms(
                [transform(name="same"), transform(name="same", schema={"other": dict(VECTOR2)})],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_chaining_is_rejected(self):
        # transforms run on translator output, so one cannot read another's output
        producer = transform(name="a", sources=("title",), schema={"derived": dict(VECTOR2)})
        consumer = transform(name="b", sources=("derived",), schema={"other": dict(VECTOR2)})
        with pytest.raises(ValueError, match="derived"):
            validate_transforms(
                [producer, consumer], table=table_schema("title"), columns={"id", "title"}
            )

    def test_spec_without_a_type_is_rejected(self):
        with pytest.raises(ValueError, match="type"):
            validate_transforms(
                [transform(schema={"embedding": {"ann": True}})],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_empty_sources_is_rejected(self):
        with pytest.raises(ValueError, match="source"):
            validate_transforms(
                [transform(sources=())], table=table_schema("title"), columns={"id", "title"}
            )

    def test_more_than_two_vector_attributes_are_rejected(self):
        three = [
            transform(name=f"t{i}", schema={f"v{i}": dict(VECTOR2)}) for i in range(3)
        ]
        with pytest.raises(ValueError, match="two vector"):
            validate_transforms(
                three, table=table_schema("title"), columns={"id", "title"}
            )

    def test_two_vector_attributes_are_allowed(self):
        two = [transform(name=f"t{i}", schema={f"v{i}": dict(VECTOR2)}) for i in range(2)]
        merged = validate_transforms(
            two, table=table_schema("title"), columns={"id", "title"}
        )
        assert "v0" in merged and "v1" in merged

    def test_ann_on_a_non_vector_type_is_rejected(self):
        with pytest.raises(ValueError, match="ann"):
            validate_transforms(
                [transform(schema={"embedding": {"type": "string", "ann": True}})],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_vector_without_ann_is_rejected(self):
        # turbopuffer refuses the write: "vector attribute must have ann:true"
        with pytest.raises(ValueError, match="ann"):
            validate_transforms(
                [transform(schema={"embedding": {"type": "[2]f32"}})],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_vector_transform_without_a_distance_metric_is_rejected(self):
        # turbopuffer requires distance_metric on every write to a namespace
        # holding a vector, so it cannot be discovered later
        with pytest.raises(ValueError, match="distance_metric"):
            validate_transforms(
                [transform(distance_metric=None)],
                table=table_schema("title"),
                columns={"id", "title"},
            )

    def test_conflicting_distance_metrics_are_rejected(self):
        # the write request carries a single metric for the whole namespace
        a = transform(name="a", schema={"va": dict(VECTOR2)}, distance_metric="cosine_distance")
        b = transform(name="b", schema={"vb": dict(VECTOR2)}, distance_metric="euclidean_squared")
        with pytest.raises(ValueError, match="distance_metric"):
            validate_transforms(
                [a, b], table=table_schema("title"), columns={"id", "title"}
            )

    def test_non_vector_transform_needs_no_distance_metric(self):
        merged = validate_transforms(
            [transform(schema={"slug": {"type": "string"}}, distance_metric=None)],
            table=table_schema("title"),
            columns={"id", "title"},
        )
        assert merged["slug"] == {"type": "string"}


class TestDistanceMetric:
    def test_taken_from_the_vector_transform(self):
        assert (
            write_distance_metric([transform(distance_metric="cosine_distance")])
            == "cosine_distance"
        )

    def test_none_without_vector_transforms(self):
        assert write_distance_metric([]) is None
        assert (
            write_distance_metric(
                [transform(schema={"slug": {"type": "string"}}, distance_metric=None)]
            )
            is None
        )

    def test_non_transform_object_is_rejected_with_a_useful_message(self):
        with pytest.raises(ValueError, match="Transform"):
            validate_transforms(
                [object()], table=table_schema("title"), columns={"id", "title"}
            )


class TestRetainGroups:
    def test_multi_source_transform_yields_a_group(self):
        groups = retain_groups([transform(sources=("title", "body"))])
        assert groups == [frozenset({"title", "body"})]

    def test_single_source_transform_yields_nothing(self):
        assert retain_groups([transform(sources=("title",))]) == []


# ------------------------------------------------------- recompute + batching


def calls_recorder(dims=2):
    calls = []

    def compute(rows):
        calls.append([dict(r) for r in rows])
        return [{"embedding": [float(i)] * dims} for i, _ in enumerate(rows)]

    return calls, compute


class TestRecomputeRule:
    def test_upsert_always_recomputes(self):
        calls, compute = calls_recorder()
        ops = list(
            apply_transforms(
                [Upsert(id=1, row={"id": 1, "title": "x"})],
                [transform(compute=compute)],
            )
        )
        assert ops[0].row["embedding"] == [0.0, 0.0]
        assert len(calls) == 1

    def test_patch_with_all_sources_recomputes(self):
        calls, compute = calls_recorder()
        ops = list(
            apply_transforms(
                [Patch(id=1, columns={"title": "x"})], [transform(compute=compute)]
            )
        )
        assert ops[0].columns["embedding"] == [0.0, 0.0]

    def test_patch_without_the_source_is_untouched(self):
        calls, compute = calls_recorder()
        ops = list(
            apply_transforms(
                [Patch(id=1, columns={"price": 5})], [transform(compute=compute)]
            )
        )
        assert ops[0] == Patch(id=1, columns={"price": 5})
        assert calls == []

    def test_patch_with_only_some_sources_is_not_computed(self):
        # the `all` rule: a partial source set must never reach compute()
        calls, compute = calls_recorder()
        ops = list(
            apply_transforms(
                [Patch(id=1, columns={"title": "x"})],
                [transform(sources=("title", "body"), compute=compute)],
            )
        )
        assert ops[0] == Patch(id=1, columns={"title": "x"})
        assert calls == []

    def test_overlapping_sources_never_compute_on_partial_input(self):
        """The regression this rule exists for, driven through the real
        Translator: transforms A(title, body) and B(body, tags); an update to
        `title` alone retains `body` for A, and B must NOT fire on it."""
        a = transform(
            name="a",
            sources=("title", "body"),
            schema={"embed_a": dict(VECTOR2)},
            compute=lambda rows: [{"embed_a": [1.0, 1.0]} for _ in rows],
        )
        b_calls = []

        def b_compute(rows):
            b_calls.append([dict(r) for r in rows])
            return [{"embed_b": [0.0, 0.0]} for _ in rows]

        b = FunctionTransform(
            name="b",
            sources=("body", "tags"),
            schema={"embed_b": dict(VECTOR2)},
            compute=b_compute,
            batch_size=100,
        )

        codec = IdCodec.from_key_schema(
            {"type": "record", "name": "k", "fields": [{"name": "id", "type": "long"}]}
        )
        translator = Translator(codec, retain_groups=retain_groups([a, b]))
        patch = translator.translate(
            ChangeEvent(
                key={"id": 1},
                before={"id": 1, "title": "old", "body": "b", "tags": "t"},
                after={"id": 1, "title": "new", "body": "b", "tags": "t"},
                ts=1,
                partition=0,
                offset=0,
            )
        )
        assert set(patch.columns) == {"title", "body"}  # tags not retained

        out = list(apply_transforms([patch], [a, b]))
        assert "embed_a" in out[0].columns
        assert "embed_b" not in out[0].columns
        assert b_calls == [], "B ran without its `tags` source"

    def test_deletes_are_passed_through_untouched(self):
        calls, compute = calls_recorder()
        ops = list(
            apply_transforms([Delete(id=1)], [transform(compute=compute)])
        )
        assert ops == [Delete(id=1)]
        assert calls == []

    def test_no_transforms_is_a_passthrough(self):
        ops = [Upsert(id=1, row={"id": 1}), Delete(id=2)]
        assert list(apply_transforms(ops, [])) == ops


class TestBatching:
    def test_batches_respect_batch_size(self):
        calls, compute = calls_recorder()
        ops = [Upsert(id=i, row={"id": i, "title": "x"}) for i in range(250)]
        list(apply_transforms(ops, [transform(compute=compute, batch_size=100)]))
        assert [len(c) for c in calls] == [100, 100, 50]

    def test_rows_carry_exactly_the_sources_plus_id(self):
        calls, compute = calls_recorder()
        ops = [Upsert(id=7, row={"id": 7, "title": "x", "price": 3})]
        list(apply_transforms(ops, [transform(compute=compute)]))
        assert calls[0] == [{"id": 7, "title": "x"}]

    def test_rows_are_in_op_order(self):
        calls, compute = calls_recorder()
        ops = [Upsert(id=i, row={"id": i, "title": str(i)}) for i in range(3)]
        list(apply_transforms(ops, [transform(compute=compute)]))
        assert [r["title"] for r in calls[0]] == ["0", "1", "2"]

    def test_two_transforms_both_apply(self):
        a = transform(name="a", schema={"va": dict(VECTOR2)},
                      compute=lambda rows: [{"va": [1.0, 1.0]} for _ in rows])
        b = transform(name="b", schema={"vb": dict(VECTOR2)},
                      compute=lambda rows: [{"vb": [2.0, 2.0]} for _ in rows])
        ops = list(apply_transforms([Upsert(id=1, row={"id": 1, "title": "x"})], [a, b]))
        assert ops[0].row["va"] == [1.0, 1.0]
        assert ops[0].row["vb"] == [2.0, 2.0]

    def test_none_result_is_written_through_explicitly(self):
        t = transform(compute=lambda rows: [{"embedding": None} for _ in rows])
        ops = list(apply_transforms([Patch(id=1, columns={"title": "x"})], [t]))
        assert ops[0].columns["embedding"] is None

    def test_is_a_generator_that_does_not_consume_everything_up_front(self):
        pulled = []

        def source():
            for i in range(300):
                pulled.append(i)
                yield Upsert(id=i, row={"id": i, "title": "x"})

        stream = apply_transforms(source(), [transform(batch_size=50)])
        next(stream)
        assert len(pulled) <= 50


class TestOutputValidation:
    def _run(self, compute, ops=None, **kw):
        ops = ops or [Upsert(id=1, row={"id": 1, "title": "x"})]
        return list(apply_transforms(ops, [transform(compute=compute, **kw)]))

    def test_wrong_result_count_is_an_error(self):
        with pytest.raises(TransformError, match="embed"):
            self._run(lambda rows: [])

    def test_missing_key_is_an_error(self):
        with pytest.raises(TransformError, match="embedding"):
            self._run(lambda rows: [{} for _ in rows])

    def test_unexpected_key_is_an_error(self):
        with pytest.raises(TransformError, match="surprise"):
            self._run(lambda rows: [{"embedding": [1.0, 2.0], "surprise": 1} for _ in rows])

    def test_wrong_vector_dimension_is_an_error(self):
        with pytest.raises(TransformError, match="2"):
            self._run(lambda rows: [{"embedding": [1.0, 2.0, 3.0]} for _ in rows])

    def test_array_like_result_is_converted_via_tolist(self):
        class FakeArray:
            def tolist(self):
                return [1.0, 2.0]

        ops = self._run(lambda rows: [{"embedding": FakeArray()} for _ in rows])
        assert ops[0].row["embedding"] == [1.0, 2.0]

    def test_opaque_object_is_rejected(self):
        with pytest.raises(TransformError, match="embedding"):
            self._run(lambda rows: [{"embedding": object()} for _ in rows])

    def test_compute_raising_becomes_a_transform_error_naming_the_transform(self):
        def boom(rows):
            raise RuntimeError("model unavailable")

        with pytest.raises(TransformError, match="embed") as excinfo:
            self._run(boom)
        assert isinstance(excinfo.value.__cause__, RuntimeError)

    def test_transform_error_names_the_documents(self):
        def boom(rows):
            raise RuntimeError("nope")

        with pytest.raises(TransformError, match="42"):
            self._run(boom, ops=[Upsert(id=42, row={"id": 42, "title": "x"})])
