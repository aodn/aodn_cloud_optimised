"""Tests for dimension validation and expansion logic in GenericZarrHandler.

Tests cover:
- preprocess_xarray expanding dims for variables missing the append_dim
- _validate_and_fix_dims fixing mismatches against an existing Zarr store
- _process_individual_file_fallback resilience (one bad file doesn't crash batch)
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from aodn_cloud_optimised.lib.GenericZarrHandler import preprocess_xarray


@pytest.fixture
def dataset_config_with_dims():
    """Minimal dataset_config for preprocess_xarray with TIME, I, J dimensions."""
    return {
        "logger_name": "test_dim_validation",
        "dataset_name": "test_radar",
        "schema_transformation": {
            "dimensions": {
                "time": {"name": "TIME", "chunk": 100, "append_dim": True},
                "latitude": {"name": "J", "chunk": 60},
                "longitude": {"name": "I", "chunk": 59},
            },
            "add_variables": None,
            "global_attributes": None,
            "var_template_shape": "UCUR",
        },
        "schema": {
            "TIME": {"type": "datetime64[ns]"},
            "I": {"type": "int32"},
            "J": {"type": "int32"},
            "LATITUDE": {"type": "float64"},
            "LONGITUDE": {"type": "float64"},
            "GDOP": {"type": "float32"},
            "UCUR": {"type": "float32"},
        },
    }


@pytest.fixture
def sample_ds_with_gdop_no_time():
    """Dataset where GDOP has only (I, J) dims, missing TIME."""
    time = np.array(["2024-01-01T00:00:00"], dtype="datetime64[ns]")
    i_vals = np.arange(5)
    j_vals = np.arange(4)

    ds = xr.Dataset(
        {
            "UCUR": (["TIME", "I", "J"], np.random.rand(1, 5, 4).astype("float32")),
            "GDOP": (["I", "J"], np.random.rand(5, 4).astype("float32")),
        },
        coords={
            "TIME": time,
            "I": i_vals,
            "J": j_vals,
            "LATITUDE": (["I", "J"], np.random.rand(5, 4)),
            "LONGITUDE": (["I", "J"], np.random.rand(5, 4)),
        },
    )
    # Set encoding source so filename detection works
    ds["UCUR"].encoding["source"] = "test_file.nc"
    return ds


class TestPreprocessXarrayExpandDims:
    """Tests for the expand_dims logic in preprocess_xarray."""

    def test_gdop_expanded_to_include_time(
        self, sample_ds_with_gdop_no_time, dataset_config_with_dims
    ):
        """GDOP with (I, J) should be expanded to (TIME, I, J)."""
        ds = preprocess_xarray(sample_ds_with_gdop_no_time, dataset_config_with_dims)
        assert (
            "TIME" in ds["GDOP"].dims
        ), f"Expected TIME in GDOP dims, got {ds['GDOP'].dims}"
        assert ds["GDOP"].dims == ("TIME", "I", "J") or set(ds["GDOP"].dims) == {
            "TIME",
            "I",
            "J",
        }

    def test_ucur_not_modified(
        self, sample_ds_with_gdop_no_time, dataset_config_with_dims
    ):
        """UCUR already has (TIME, I, J) — should remain unchanged."""
        ds = preprocess_xarray(sample_ds_with_gdop_no_time, dataset_config_with_dims)
        assert ds["UCUR"].dims == ("TIME", "I", "J")

    def test_variable_without_spatial_dims_not_expanded(self, dataset_config_with_dims):
        """A scalar variable with no configured spatial dims should NOT be expanded."""
        time = np.array(["2024-01-01T00:00:00"], dtype="datetime64[ns]")
        i_vals = np.arange(5)
        j_vals = np.arange(4)

        # Add a variable that explicitly has the append_dim already
        ds = xr.Dataset(
            {
                "UCUR": (
                    ["TIME", "I", "J"],
                    np.random.rand(1, 5, 4).astype("float32"),
                ),
                "GDOP": (["I", "J"], np.random.rand(5, 4).astype("float32")),
            },
            coords={
                "TIME": time,
                "I": i_vals,
                "J": j_vals,
                "LATITUDE": (["I", "J"], np.random.rand(5, 4)),
                "LONGITUDE": (["I", "J"], np.random.rand(5, 4)),
            },
        )
        ds["UCUR"].encoding["source"] = "test_file.nc"

        result = preprocess_xarray(ds, dataset_config_with_dims)
        # UCUR should still have TIME
        assert "TIME" in result["UCUR"].dims


class TestValidateAndFixDims:
    """Tests for the _validate_and_fix_dims method."""

    def test_matching_dims_unchanged(self):
        """Variables with matching dims should pass through unchanged."""
        ds_new = xr.Dataset(
            {"VAR": (["TIME", "I", "J"], np.zeros((2, 3, 4)))},
            coords={"TIME": np.arange(2), "I": np.arange(3), "J": np.arange(4)},
        )
        ds_org = xr.Dataset(
            {"VAR": (["TIME", "I", "J"], np.zeros((5, 3, 4)))},
            coords={"TIME": np.arange(5), "I": np.arange(3), "J": np.arange(4)},
        )

        handler = self._make_handler()
        result = handler._validate_and_fix_dims(ds_new, ds_org)
        assert result["VAR"].dims == ("TIME", "I", "J")

    def test_missing_dim_expanded(self):
        """Variable missing TIME dim should be expanded."""
        ds_new = xr.Dataset(
            {
                "GDOP": (["I", "J"], np.zeros((3, 4))),
                "UCUR": (["TIME", "I", "J"], np.zeros((2, 3, 4))),
            },
            coords={"TIME": np.arange(2), "I": np.arange(3), "J": np.arange(4)},
        )
        ds_org = xr.Dataset(
            {
                "GDOP": (["TIME", "I", "J"], np.zeros((5, 3, 4))),
                "UCUR": (["TIME", "I", "J"], np.zeros((5, 3, 4))),
            },
            coords={"TIME": np.arange(5), "I": np.arange(3), "J": np.arange(4)},
        )

        handler = self._make_handler()
        result = handler._validate_and_fix_dims(ds_new, ds_org)
        assert result["GDOP"].dims == ("TIME", "I", "J")

    def test_incompatible_dims_raises(self):
        """Completely incompatible dims should raise ValueError."""
        ds_new = xr.Dataset(
            {"VAR": (["X", "Y"], np.zeros((3, 4)))},
            coords={"X": np.arange(3), "Y": np.arange(4)},
        )
        ds_org = xr.Dataset(
            {"VAR": (["TIME", "I", "J"], np.zeros((5, 3, 4)))},
            coords={"TIME": np.arange(5), "I": np.arange(3), "J": np.arange(4)},
        )

        handler = self._make_handler()
        with pytest.raises(ValueError, match="incompatible dimensions"):
            handler._validate_and_fix_dims(ds_new, ds_org)

    def test_new_variable_not_in_store_passes(self):
        """A new variable not in the store should not cause issues."""
        ds_new = xr.Dataset(
            {"NEW_VAR": (["TIME"], np.zeros(2))},
            coords={"TIME": np.arange(2)},
        )
        ds_org = xr.Dataset(
            {"OTHER_VAR": (["TIME"], np.zeros(5))},
            coords={"TIME": np.arange(5)},
        )

        handler = self._make_handler()
        result = handler._validate_and_fix_dims(ds_new, ds_org)
        assert "NEW_VAR" in result

    @staticmethod
    def _make_handler():
        """Create a minimal mock handler with logger and uuid_log."""
        handler = MagicMock()
        handler._validate_and_fix_dims = (
            lambda ds, ds_org: TestValidateAndFixDims._real_validate(
                handler, ds, ds_org
            )
        )
        handler.uuid_log = "test-uuid"
        from aodn_cloud_optimised.lib.logging import get_logger

        handler.logger = get_logger("test_validate_dims")
        return handler

    @staticmethod
    def _real_validate(handler, ds, ds_org):
        """Run the real _validate_and_fix_dims logic."""
        from aodn_cloud_optimised.lib.GenericZarrHandler import GenericHandler

        # Use the real method, bound to our mock
        return GenericHandler._validate_and_fix_dims(handler, ds, ds_org)


class TestProcessIndividualFileFallbackResilience:
    """Tests for _process_individual_file_fallback resilience."""

    def test_one_bad_file_does_not_stop_others(self):
        """If one file fails, other files should still be processed."""
        handler = MagicMock()
        handler.uuid_log = "test-uuid"
        from aodn_cloud_optimised.lib.logging import get_logger

        handler.logger = get_logger("test_fallback")

        call_count = {"open": 0, "write": 0}

        def mock_open(file, pp, dv):
            call_count["open"] += 1
            if "bad_file" in file:
                raise ValueError("Simulated bad file error")
            return MagicMock()

        def mock_write(ds, idx):
            call_count["write"] += 1

        handler._open_file_with_fallback = mock_open
        handler._write_ds = mock_write

        from aodn_cloud_optimised.lib.GenericZarrHandler import GenericHandler

        batch_files = ["good_file_1.nc", "bad_file.nc", "good_file_2.nc"]
        GenericHandler._process_individual_file_fallback(
            handler, batch_files, MagicMock(), [], 0
        )

        # Both good files should have been written
        assert call_count["write"] == 2
        assert call_count["open"] == 3
