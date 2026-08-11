import base64
import datetime as dt
from decimal import Decimal

import pytest

from mz_tpuf_sink import translate as translate_module
from mz_tpuf_sink.ids import IdCodec
from mz_tpuf_sink.models import ChangeEvent, Delete, Patch, Upsert
from mz_tpuf_sink.translate import Translator, to_attr


@pytest.fixture(autouse=True)
def reset_precision_warning():
    """to_attr warns once per process; keep tests order-independent."""
    translate_module._warned_decimal_precision = False
    yield


def make_codec():
    return IdCodec.from_key_schema(
        {"type": "record", "name": "k", "fields": [{"name": "id", "type": "long"}]}
    )


def event(before, after, key=None):
    return ChangeEvent(
        key=key or {"id": 1},
        before=before,
        after=after,
        ts=100,
        partition=0,
        offset=0,
    )


class TestEnvelopeTranslation:
    def setup_method(self):
        self.translator = Translator(make_codec())

    def test_insert_becomes_full_upsert(self):
        op = self.translator.translate(event(None, {"id": 1, "name": "a", "n": 2}))
        assert op == Upsert(id=1, row={"id": 1, "name": "a", "n": 2})

    def test_update_becomes_patch_of_changed_columns_only(self):
        op = self.translator.translate(
            event({"id": 1, "name": "a", "n": 2}, {"id": 1, "name": "b", "n": 2})
        )
        assert op == Patch(id=1, columns={"name": "b"})

    def test_update_setting_column_to_null_patches_null(self):
        op = self.translator.translate(
            event({"id": 1, "name": "a"}, {"id": 1, "name": None})
        )
        assert op == Patch(id=1, columns={"name": None})

    def test_update_with_no_changes_returns_none(self):
        op = self.translator.translate(event({"id": 1, "n": 2}, {"id": 1, "n": 2}))
        assert op is None

    def test_delete_becomes_delete_by_id(self):
        op = self.translator.translate(event({"id": 1, "name": "a"}, None))
        assert op == Delete(id=1)

    def test_empty_envelope_is_an_error(self):
        with pytest.raises(ValueError, match="before.*after"):
            self.translator.translate(event(None, None))

    def test_id_column_never_patched(self):
        # doc id comes from the key; the value's own "id" column is not an attribute
        op = self.translator.translate(event({"id": 1, "n": 2}, {"id": 1, "n": 3}))
        assert op == Patch(id=1, columns={"n": 3})

    def test_warns_once_when_dropping_unrelated_id_column(self, caplog):
        # key field is "user_id" but the value has its own "id" column, which
        # is silently unrepresentable in turbopuffer: warn the operator once
        import logging

        codec = IdCodec.from_key_schema(
            {"type": "record", "name": "k", "fields": [{"name": "user_id", "type": "long"}]}
        )
        translator = Translator(codec)
        with caplog.at_level(logging.WARNING):
            for offset in range(2):
                translator.translate(
                    ChangeEvent(
                        key={"user_id": 5},
                        before=None,
                        after={"user_id": 5, "id": 99},
                        ts=100,
                        partition=0,
                        offset=offset,
                    )
                )
        warnings = [r for r in caplog.records if '"id"' in r.message]
        assert len(warnings) == 1


class TestSourceRetention:
    """A transform deriving from several columns needs all of them, but a patch
    carries only what changed. When any column of a group changes, the rest of
    that group is retained from `after`."""

    def translator(self, *groups):
        return Translator(make_codec(), retain_groups=[frozenset(g) for g in groups])

    def test_no_groups_leaves_patches_untouched(self):
        op = self.translator().translate(
            event({"id": 1, "a": 1, "b": 2}, {"id": 1, "a": 9, "b": 2})
        )
        assert op == Patch(id=1, columns={"a": 9})

    def test_sibling_source_is_retained_when_one_changes(self):
        op = self.translator(("a", "b")).translate(
            event({"id": 1, "a": 1, "b": 2}, {"id": 1, "a": 9, "b": 2})
        )
        assert op == Patch(id=1, columns={"a": 9, "b": 2})

    def test_retained_value_comes_from_after_not_before(self):
        # both change; each must carry its new value
        op = self.translator(("a", "b")).translate(
            event({"id": 1, "a": 1, "b": 2}, {"id": 1, "a": 9, "b": 8})
        )
        assert op == Patch(id=1, columns={"a": 9, "b": 8})

    def test_untouched_group_is_not_retained(self):
        op = self.translator(("c", "d")).translate(
            event({"id": 1, "a": 1, "c": 3, "d": 4}, {"id": 1, "a": 9, "c": 3, "d": 4})
        )
        assert op == Patch(id=1, columns={"a": 9})

    def test_no_change_update_still_returns_none(self):
        op = self.translator(("a", "b")).translate(
            event({"id": 1, "a": 1, "b": 2}, {"id": 1, "a": 1, "b": 2})
        )
        assert op is None

    def test_retention_does_not_cascade_between_groups(self):
        # groups {a,b} and {b,c}: changing `a` retains `b` for the first group,
        # but that must not go on to trigger the second and drag in `c` — each
        # spurious retention is a paid embedding call downstream
        op = self.translator(("a", "b"), ("b", "c")).translate(
            event(
                {"id": 1, "a": 1, "b": 2, "c": 3},
                {"id": 1, "a": 9, "b": 2, "c": 3},
            )
        )
        assert op == Patch(id=1, columns={"a": 9, "b": 2})

    def test_retained_values_are_mapped_through_to_attr(self):
        ts = dt.datetime(2026, 8, 11, tzinfo=dt.timezone.utc)
        op = self.translator(("a", "when")).translate(
            event({"id": 1, "a": 1, "when": ts}, {"id": 1, "a": 9, "when": ts})
        )
        assert op == Patch(id=1, columns={"a": 9, "when": "2026-08-11T00:00:00+00:00"})

    def test_single_column_groups_are_dropped(self):
        # the changed column is already in the patch, so a one-source transform
        # needs no retention at all
        translator = self.translator(("a",))
        assert translator._retain_groups == ()

    def test_column_absent_from_after_is_not_invented(self):
        op = self.translator(("a", "missing")).translate(
            event({"id": 1, "a": 1}, {"id": 1, "a": 9})
        )
        assert op == Patch(id=1, columns={"a": 9})

    def test_inserts_and_deletes_are_unaffected(self):
        translator = self.translator(("a", "b"))
        assert translator.translate(event(None, {"id": 1, "a": 1, "b": 2})) == Upsert(
            id=1, row={"id": 1, "a": 1, "b": 2}
        )
        assert translator.translate(event({"id": 1, "a": 1}, None)) == Delete(id=1)

    def test_patch_columns_never_exceed_the_after_row(self):
        # the guarantee that protects attributes living only in turbopuffer:
        # retention only ever adds keys drawn from `after`
        after = {"id": 1, "a": 9, "b": 2, "c": 3}
        op = self.translator(("a", "b")).translate(
            event({"id": 1, "a": 1, "b": 2, "c": 3}, after)
        )
        assert set(op.columns) <= set(after)


class TestTypeMapping:
    def test_datetime_maps_to_iso_string(self):
        value = dt.datetime(2026, 8, 11, 12, 0, 0, tzinfo=dt.timezone.utc)
        assert to_attr(value) == "2026-08-11T12:00:00+00:00"

    def test_date_maps_to_iso_string(self):
        assert to_attr(dt.date(2026, 8, 11)) == "2026-08-11"

    def test_decimal_maps_to_float(self):
        # turbopuffer can range-filter and sort numbers, not numeric strings
        assert to_attr(Decimal("1.50")) == 1.5

    def test_decimal_drops_avro_scale_padding(self):
        # Materialize numeric arrives as Avro decimal with scale 39
        padded = Decimal("9.990000000000000000000000000000000000000")
        assert to_attr(padded) == 9.99

    def test_integral_decimal_is_still_a_float(self):
        assert to_attr(Decimal("30.00")) == 30.0
        assert isinstance(to_attr(Decimal("30.00")), float)

    def test_warns_once_when_decimal_precision_exceeds_float(self, caplog):
        import logging

        # 20 significant digits cannot survive a float round-trip
        lossy = Decimal("1.2345678901234567890")
        with caplog.at_level(logging.WARNING):
            to_attr(lossy)
            to_attr(lossy)
        warnings = [r for r in caplog.records if "precision" in r.message]
        assert len(warnings) == 1

    def test_no_warning_for_ordinary_decimals(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            to_attr(Decimal("9.990000000000000000000000000000000000000"))
        assert [r for r in caplog.records if "precision" in r.message] == []

    def test_bytes_map_to_base64_string(self):
        assert to_attr(b"\x00\x01") == base64.b64encode(b"\x00\x01").decode()

    def test_nested_record_maps_to_json_string(self):
        assert to_attr({"a": 1, "b": "x"}) == '{"a":1,"b":"x"}'

    def test_list_of_primitives_passes_through(self):
        assert to_attr([1, 2, 3]) == [1, 2, 3]

    def test_list_of_records_maps_to_json_string(self):
        assert to_attr([{"a": 1}]) == '[{"a":1}]'

    def test_primitives_pass_through(self):
        assert to_attr("x") == "x"
        assert to_attr(3) == 3
        assert to_attr(1.5) == 1.5
        assert to_attr(True) is True
        assert to_attr(None) is None

    def test_upsert_rows_have_mapped_values(self):
        translator = Translator(make_codec())
        ts = dt.datetime(2026, 8, 11, tzinfo=dt.timezone.utc)
        op = translator.translate(event(None, {"id": 1, "created": ts}))
        assert op == Upsert(id=1, row={"id": 1, "created": "2026-08-11T00:00:00+00:00"})

    def test_diff_compares_raw_values_before_mapping(self):
        # equal datetimes must not produce a patch even though mapping changes repr
        translator = Translator(make_codec())
        ts = dt.datetime(2026, 8, 11, tzinfo=dt.timezone.utc)
        op = translator.translate(
            event({"id": 1, "created": ts}, {"id": 1, "created": ts})
        )
        assert op is None
