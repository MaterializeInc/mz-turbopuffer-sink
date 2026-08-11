import json

import pytest

from mz_tpuf_sink import models
from mz_tpuf_sink.models import Delete, Patch, Upsert, op_size_bytes


class TestOpSizeBytes:
    def test_upsert_sized_from_its_row(self):
        op = Upsert(id=1, row={"id": 1, "name": "widget"})
        assert op_size_bytes(op) == len(json.dumps(op.row))

    def test_patch_sized_from_its_columns(self):
        op = Patch(id=1, columns={"name": "widget"})
        assert op_size_bytes(op) == len(json.dumps(op.columns))

    def test_delete_sized_from_its_id(self):
        assert op_size_bytes(Delete(id=1)) == len(json.dumps(1))

    def test_non_serializable_values_still_measured(self):
        class Opaque:
            def __str__(self):
                return "xxxxx"

        assert op_size_bytes(Patch(id=1, columns={"x": Opaque()})) > 0


class TestVectorFastPath:
    """A 1536-float vector costs ~29 KB of transient string to measure by
    json.dumps, and measuring happens on every chunking pass."""

    VECTOR = [0.123456789] * 1536

    def test_vector_estimate_is_close_to_real_json_length(self):
        op = Upsert(id=1, row={"id": 1, "embedding": self.VECTOR})
        actual = len(json.dumps(op.row))
        assert op_size_bytes(op) == pytest.approx(actual, rel=0.20)

    def _spy_on_dumps(self, monkeypatch):
        """Record every payload handed to json.dumps, still delegating."""
        seen = []
        real = models.json.dumps

        def spy(payload, **kwargs):
            seen.append(payload)
            return real(payload, **kwargs)

        monkeypatch.setattr(models.json, "dumps", spy)
        return seen

    def _contains_vector(self, seen):
        return any(
            isinstance(p, dict) and any(v is self.VECTOR for v in p.values())
            for p in seen
        )

    def test_vector_is_never_serialized_to_measure_it(self, monkeypatch):
        seen = self._spy_on_dumps(monkeypatch)
        op_size_bytes(Upsert(id=1, row={"id": 1, "embedding": self.VECTOR}))
        op_size_bytes(Patch(id=1, columns={"embedding": self.VECTOR}))
        assert not self._contains_vector(seen)

    def test_integer_vectors_take_the_fast_path_too(self, monkeypatch):
        # width is sampled, so a ragged list of integers is only approximate;
        # embedding vectors, the case this exists for, are uniform-width
        vector = list(range(500))
        actual = len(json.dumps({"v": vector}))  # before the spy is installed
        seen = self._spy_on_dumps(monkeypatch)
        size = op_size_bytes(Patch(id=1, columns={"v": vector}))
        assert actual / 2 < size < actual * 2
        assert not any(
            isinstance(p, dict) and any(v is vector for v in p.values()) for p in seen
        )

    def test_uniform_width_integer_vector_is_accurate(self):
        vector = [1000] * 500
        size = op_size_bytes(Patch(id=1, columns={"v": vector}))
        assert size == pytest.approx(len(json.dumps({"v": vector})), rel=0.20)

    def test_mixed_row_counts_both_vector_and_scalar_columns(self):
        op = Upsert(id=1, row={"id": 1, "name": "widget", "embedding": self.VECTOR})
        actual = len(json.dumps(op.row))
        assert op_size_bytes(op) == pytest.approx(actual, rel=0.20)

    def test_string_list_is_not_treated_as_a_vector(self):
        op = Patch(id=1, columns={"tags": ["alpha", "beta"]})
        assert op_size_bytes(op) == len(json.dumps(op.columns))

    def test_empty_list_is_handled(self):
        assert op_size_bytes(Patch(id=1, columns={"v": []})) > 0
