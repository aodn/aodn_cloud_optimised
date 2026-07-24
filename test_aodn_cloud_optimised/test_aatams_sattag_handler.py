"""Unit tests for AatamsSatelliteTaggingHandler numeric-coercion and the
per-column-tolerant schema cast that together resolve the CSV/schema type
conflicts (fluoro_* double<->large_string, n_*/max_dbar double<->int64).

These tests construct a bare handler instance with ``__new__`` and set only the
attributes the tested methods need, so no S3/cluster setup is required.
"""

import logging
import unittest

import numpy as np
import pandas as pd
import pyarrow as pa

from aodn_cloud_optimised.lib.AatamsSatelliteTaggingHandler import (
    AatamsSatelliteTaggingHandler,
)


def _make_handler(pyarrow_schema=None, schema=None, raise_error=False):
    handler = AatamsSatelliteTaggingHandler.__new__(AatamsSatelliteTaggingHandler)
    handler.logger = logging.getLogger("test_aatams")
    handler.logger.addHandler(logging.NullHandler())
    handler.uuid_log = "test-uuid"
    handler.pyarrow_schema = pyarrow_schema or pa.schema([])
    handler.schema = schema or {}
    handler.raise_error = raise_error
    return handler


class TestPackedExpansion(unittest.TestCase):
    def test_packed_strings_expand_to_numeric(self):
        """Comma-packed depth/value columns explode into float rows."""
        handler = _make_handler(
            pa.schema(
                [
                    pa.field("temp_dbar", pa.float64()),
                    pa.field("temp_vals", pa.float64()),
                    pa.field("ref", pa.string()),
                ]
            )
        )
        df = pd.DataFrame(
            {
                "ref": ["rs3-Fred-07"],
                "temp_dbar": ["4,10,12"],
                "temp_vals": ["5.4,5.3,5.1"],
            }
        )
        out = handler._expand_packed_string_columns(df)

        self.assertEqual(len(out), 3)  # one row per depth level
        self.assertTrue(pd.api.types.is_float_dtype(out["temp_dbar"]))
        self.assertTrue(pd.api.types.is_float_dtype(out["temp_vals"]))
        self.assertEqual(list(out["temp_dbar"]), [4.0, 10.0, 12.0])
        # scalar text column is repeated, left untouched
        self.assertEqual(list(out["ref"]), ["rs3-Fred-07"] * 3)

    def test_single_value_string_profile_coerced_to_numeric(self):
        """A lone string value (no comma) in a numeric column becomes float."""
        handler = _make_handler(
            pa.schema(
                [
                    pa.field("fluoro_dbar", pa.float64()),
                    pa.field("fluoro_vals", pa.float64()),
                ]
            )
        )
        # object dtype, no commas anywhere -> not exploded, must be coerced
        df = pd.DataFrame(
            {
                "fluoro_dbar": pd.Series(["12.5", "8.0"], dtype=object),
                "fluoro_vals": pd.Series(["0.1", "0.2"], dtype=object),
            }
        )
        out = handler._expand_packed_string_columns(df)

        self.assertTrue(pd.api.types.is_float_dtype(out["fluoro_dbar"]))
        self.assertTrue(pd.api.types.is_float_dtype(out["fluoro_vals"]))
        self.assertEqual(list(out["fluoro_dbar"]), [12.5, 8.0])

    def test_non_numeric_value_in_numeric_column_is_left_untouched(self):
        """Un-parseable value keeps the column as-is without data loss."""
        handler = _make_handler(pa.schema([pa.field("fluoro_vals", pa.float64())]))
        df = pd.DataFrame({"fluoro_vals": pd.Series(["12.5", "junk"], dtype=object)})

        out = handler._coerce_numeric_string_columns(df)

        # real values preserved, not silently turned into NaN
        self.assertEqual(out["fluoro_vals"].tolist(), ["12.5", "junk"])

    def test_declared_string_column_not_coerced(self):
        """A schema-declared string column is never numeric-coerced."""
        handler = _make_handler(pa.schema([pa.field("created", pa.string())]))
        df = pd.DataFrame(
            {"created": pd.Series(["2020-01-01", "2020-02-02"], dtype=object)}
        )

        out = handler._coerce_numeric_string_columns(df)
        self.assertEqual(out["created"].dtype, object)


class TestCastTableTolerance(unittest.TestCase):
    def _schema(self):
        return pa.schema(
            [
                pa.field("max_dbar", pa.float64()),
                pa.field("n_temp", pa.int32()),
                pa.field("n_sal", pa.int32()),
                pa.field("fluoro_vals", pa.float64()),
            ]
        )

    def test_bad_column_does_not_drag_down_whole_table(self):
        """One un-castable column stays as-is; the rest get their schema type."""
        handler = _make_handler()
        # max_dbar read as int64, n_temp/n_sal as int-valued floats, and a
        # fluoro_vals packed string that cannot cast to double.
        table = pa.table(
            {
                "max_dbar": pa.array([10, 15], pa.int64()),
                "n_temp": pa.array([10.0, 12.0], pa.float64()),
                "n_sal": pa.array([9.0, 11.0], pa.float64()),
                "fluoro_vals": pa.array(["4,10,12", "8,9"], pa.large_string()),
            }
        )

        out = handler.cast_table_by_schema(table, self._schema())

        self.assertEqual(out.schema.field("max_dbar").type, pa.float64())
        self.assertEqual(out.schema.field("n_temp").type, pa.int32())
        self.assertEqual(out.schema.field("n_sal").type, pa.int32())
        # bad column kept its original type instead of failing the whole file
        self.assertEqual(out.schema.field("fluoro_vals").type, pa.large_string())
        self.assertEqual(out.column("n_temp").to_pylist(), [10, 12])

    def test_all_castable_columns_follow_schema(self):
        handler = _make_handler()
        table = pa.table(
            {
                "max_dbar": pa.array([10, 15], pa.int64()),
                "n_temp": pa.array([10.0, 12.0], pa.float64()),
                "n_sal": pa.array([9, 11], pa.int64()),
                "fluoro_vals": pa.array([0.1, 0.2], pa.float64()),
            }
        )

        out = handler.cast_table_by_schema(table, self._schema())
        self.assertEqual(out.schema, self._schema())

    def test_raises_when_raise_error_is_set(self):
        """With raise_error, an un-castable column fails the file loudly."""
        handler = _make_handler(raise_error=True)
        table = pa.table(
            {
                "max_dbar": pa.array([10, 15], pa.int64()),
                "n_temp": pa.array([10.0, 12.0], pa.float64()),
                "n_sal": pa.array([9, 11], pa.int64()),
                "fluoro_vals": pa.array(["4,10,12", "8,9"], pa.large_string()),
            }
        )

        with self.assertRaises(ValueError) as ctx:
            handler.cast_table_by_schema(table, self._schema())

        # The message must name the offending column, unlike the bare
        # "ArrowInvalid" it replaces.
        self.assertIn("fluoro_vals", str(ctx.exception))

    def test_does_not_raise_when_all_columns_cast(self):
        handler = _make_handler(raise_error=True)
        table = pa.table(
            {
                "max_dbar": pa.array([10, 15], pa.int64()),
                "n_temp": pa.array([10.0, 12.0], pa.float64()),
                "n_sal": pa.array([9, 11], pa.int64()),
                "fluoro_vals": pa.array([0.1, 0.2], pa.float64()),
            }
        )

        out = handler.cast_table_by_schema(table, self._schema())
        self.assertEqual(out.schema, self._schema())


if __name__ == "__main__":
    unittest.main()
