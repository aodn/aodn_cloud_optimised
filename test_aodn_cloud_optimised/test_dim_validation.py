"""Tests for dimension validation and expansion logic in GenericZarrHandler.

Tests cover:
- preprocess_xarray expanding dims for variables missing the append_dim
- _validate_and_fix_dims fixing mismatches against an existing Zarr store
- _process_individual_file_fallback resilience (one bad file doesn't crash batch)
- preprocess_xarray raising for files missing a 1D coordinate index
- handle_missing_coordinate_index scanning and excluding such files
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


class TestPreprocessXarrayDimHandling:
    """Tests that preprocess_xarray preserves original dimensions.

    Dimension expansion is intentionally NOT done in preprocess_xarray because
    it can corrupt TIME encoding in Zarr stores. Instead, dimension mismatches
    are fixed in _validate_and_fix_dims just before writing.
    """

    def test_gdop_keeps_original_dims(
        self, sample_ds_with_gdop_no_time, dataset_config_with_dims
    ):
        """GDOP with (I, J) should stay as (I, J) after preprocessing."""
        ds = preprocess_xarray(sample_ds_with_gdop_no_time, dataset_config_with_dims)
        assert ds["GDOP"].dims == (
            "I",
            "J",
        ), f"Expected GDOP to keep (I, J) dims, got {ds['GDOP'].dims}"

    def test_ucur_not_modified(
        self, sample_ds_with_gdop_no_time, dataset_config_with_dims
    ):
        """UCUR already has (TIME, I, J) — should remain unchanged."""
        ds = preprocess_xarray(sample_ds_with_gdop_no_time, dataset_config_with_dims)
        assert ds["UCUR"].dims == ("TIME", "I", "J")


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


class TestPreprocessMissingCoordinateIndex:
    """Tests for preprocess_xarray raising on missing 1D coordinate index."""

    def _make_config(self):
        return {
            "logger_name": "test_missing_coord",
            "dataset_name": "test_radar",
            "schema_transformation": {
                "dimensions": {
                    "time": {"name": "TIME", "chunk": 100, "append_dim": True},
                    "latitude": {"name": "J", "chunk": 4},
                    "longitude": {"name": "I", "chunk": 5},
                },
                "add_variables": None,
                "global_attributes": None,
                "var_template_shape": "UCUR",
            },
            "schema": {
                "TIME": {"type": "datetime64[ns]"},
                "I": {"type": "int32"},
                "J": {"type": "int32"},
                "UCUR": {"type": "float32"},
            },
        }

    def _make_good_ds(self):
        """Dataset with proper 1D coordinate indexes for I and J."""
        time = np.array(["2024-01-01T00:00:00"], dtype="datetime64[ns]")
        ds = xr.Dataset(
            {"UCUR": (["TIME", "I", "J"], np.ones((1, 3, 2), dtype="float32"))},
            coords={
                "TIME": time,
                "I": np.arange(3),
                "J": np.arange(2),
            },
        )
        ds["UCUR"].encoding["source"] = "good_file.nc"
        return ds

    def _make_bad_ds(self):
        """Dataset with dimension 'I' but no coordinate variable for it."""
        time = np.array(["2024-01-02T00:00:00"], dtype="datetime64[ns]")
        # Create with coords first, then drop I to simulate missing coordinate
        ds = xr.Dataset(
            {"UCUR": (["TIME", "I", "J"], np.ones((1, 3, 2), dtype="float32"))},
            coords={
                "TIME": time,
                "J": np.arange(2),
                # Note: 'I' is intentionally NOT provided as a coordinate
            },
        )
        ds["UCUR"].encoding["source"] = "bad_file.nc"
        return ds

    def test_preprocess_raises_for_missing_coordinate_index(self):
        """preprocess_xarray should raise ValueError for files missing a coordinate index."""
        config = self._make_config()
        bad_ds = self._make_bad_ds()

        with pytest.raises(
            ValueError,
            match="has dimension 'I' with no corresponding 1D coordinate index",
        ):
            preprocess_xarray(bad_ds, config)

    def test_preprocess_succeeds_for_complete_file(self):
        """preprocess_xarray should succeed when all dimensions have coordinate indexes."""
        config = self._make_config()
        good_ds = self._make_good_ds()
        result = preprocess_xarray(good_ds, config)
        assert result is not None

    def test_open_mfds_retrying_excludes_bad_file(self):
        """_open_mfds_retrying should retry without the file whose name appears in the traceback."""
        from aodn_cloud_optimised.lib.GenericZarrHandler import GenericHandler

        handler = MagicMock()
        handler.uuid_log = "test-uuid"
        from aodn_cloud_optimised.lib.logging import get_logger

        handler.logger = get_logger("test_retrying")

        config = self._make_config()
        good_ds = self._make_good_ds()

        good_file = MagicMock()
        good_file.path = "imos-data/good_file.nc"
        bad_file = MagicMock()
        bad_file.path = "imos-data/IMOS_ACORN_V_20160623T030000Z_bad_file.nc"

        call_args = []

        def mock_open_mfds(pp, dv, files, engine):
            call_args.append([f.path for f in files])
            if any("bad_file" in f.path for f in files):
                # Simulate preprocess failing for the bad file — the filename
                # must appear in the exception message as preprocess_xarray does.
                raise ValueError(
                    f"File 'IMOS_ACORN_V_20160623T030000Z_bad_file.nc' has dimension 'I' "
                    f"with no corresponding 1D coordinate index — file is structurally incomplete."
                )
            return good_ds

        handler._open_mfds = mock_open_mfds
        # MagicMock replaces class attributes with auto-mocks; restore the real regex.
        handler._PREPROCESS_BAD_FILE_RE = GenericHandler._PREPROCESS_BAD_FILE_RE

        result = GenericHandler._open_mfds_retrying(
            handler, MagicMock(), [], [good_file, bad_file], "h5netcdf"
        )

        # First call includes both files; second call has only the good file.
        assert len(call_args) == 2
        assert len(call_args[0]) == 2
        assert len(call_args[1]) == 1
        assert "good_file" in call_args[1][0]
        assert result is good_ds

    def test_open_mfds_retrying_excludes_multiple_bad_files(self):
        """_open_mfds_retrying must handle a batch with MULTIPLE bad files, removing
        them one at a time without being confused by chained exception context."""
        from aodn_cloud_optimised.lib.GenericZarrHandler import GenericHandler

        handler = MagicMock()
        handler.uuid_log = "test-uuid"
        from aodn_cloud_optimised.lib.logging import get_logger

        handler.logger = get_logger("test_retrying_multi")
        handler._PREPROCESS_BAD_FILE_RE = GenericHandler._PREPROCESS_BAD_FILE_RE

        good_ds = MagicMock()
        good_file = MagicMock()
        good_file.path = "imos-data/good_file.nc"
        bad_file1 = MagicMock()
        bad_file1.path = "imos-data/bad_file_1.nc"
        bad_file2 = MagicMock()
        bad_file2.path = "imos-data/bad_file_2.nc"

        call_args = []

        def mock_open_mfds(pp, dv, files, engine):
            call_args.append([f.path for f in files])
            # Fail on bad_file_1 first, then bad_file_2, then succeed.
            if any("bad_file_1" in f.path for f in files):
                raise ValueError(
                    "File 'bad_file_1.nc' has dimension 'I' "
                    "with no corresponding 1D coordinate index — file is structurally incomplete."
                )
            if any("bad_file_2" in f.path for f in files):
                raise ValueError(
                    "File 'bad_file_2.nc' has dimension 'I' "
                    "with no corresponding 1D coordinate index — file is structurally incomplete."
                )
            return good_ds

        handler._open_mfds = mock_open_mfds

        result = GenericHandler._open_mfds_retrying(
            handler, MagicMock(), [], [good_file, bad_file1, bad_file2], "h5netcdf"
        )

        # Three calls: [good, bad1, bad2] → [good, bad2] → [good]
        assert len(call_args) == 3
        assert len(call_args[0]) == 3
        assert len(call_args[1]) == 2
        assert len(call_args[2]) == 1
        assert "good_file" in call_args[2][0]
        assert result is good_ds


class TestGridSizeMismatchHandling:
    """Tests for GridSizeMismatchError detection and rejection in _write_ds."""

    def _make_ds(self, lat_size, lon_size, n_time=2):
        """Build a minimal dataset with given spatial dimensions."""
        import pandas as pd

        times = pd.date_range("2024-01-01", periods=n_time, freq="h")
        return xr.Dataset(
            {
                "UCUR": (
                    ["TIME", "LATITUDE", "LONGITUDE"],
                    np.ones((n_time, lat_size, lon_size), dtype="float32"),
                )
            },
            coords={
                "TIME": times,
                "LATITUDE": np.linspace(-24, -22, lat_size),
                "LONGITUDE": np.linspace(151, 153, lon_size),
            },
        )

    def test_write_ds_raises_grid_size_mismatch(self):
        """_write_ds raises GridSizeMismatchError when batch spatial dims differ from store."""
        from aodn_cloud_optimised.lib.GenericZarrHandler import (
            GenericHandler,
            GridSizeMismatchError,
        )

        handler = MagicMock()
        handler.uuid_log = "test-uuid"
        handler.append_dim_varname = "TIME"
        handler.chunks = {"TIME": 100, "LATITUDE": 64, "LONGITUDE": 72}
        handler.cloud_optimised_output_path = "s3://bucket/test.zarr"
        handler.s3_client_opts_output = {}
        from aodn_cloud_optimised.lib.logging import get_logger

        handler.logger = get_logger("test_grid_mismatch")
        handler._find_duplicated_values = MagicMock()

        # New batch: 80×80 grid
        ds_new = self._make_ds(lat_size=80, lon_size=80)
        ds_new = ds_new.chunk({"TIME": 100, "LATITUDE": 80, "LONGITUDE": 80})

        # Existing store: 64×72 grid
        ds_org = self._make_ds(lat_size=64, lon_size=72, n_time=5)

        with (
            patch(
                "aodn_cloud_optimised.lib.GenericZarrHandler.prefix_exists",
                return_value=True,
            ),
            patch(
                "aodn_cloud_optimised.lib.GenericZarrHandler.xr.open_zarr",
                return_value=ds_org,
            ),
        ):
            with pytest.raises(
                GridSizeMismatchError, match="incompatible spatial grid"
            ):
                GenericHandler._write_ds(handler, ds_new, idx=1)

    def test_publish_batch_skips_grid_size_mismatch(self):
        """publish_cloud_optimised_fileset_batch logs and skips a batch with wrong grid."""
        from unittest.mock import patch

        from aodn_cloud_optimised.lib.GenericZarrHandler import (
            GenericHandler,
            GridSizeMismatchError,
        )

        handler = MagicMock()
        handler.uuid_log = "test-uuid"
        handler.cluster_mode = False
        handler.schema = {}
        from aodn_cloud_optimised.lib.logging import get_logger

        handler.logger = get_logger("test_grid_skip")

        # _write_ds raises GridSizeMismatchError
        handler._write_ds = MagicMock(
            side_effect=GridSizeMismatchError(
                "Batch has incompatible spatial grid — LATITUDE: batch=80 vs store=64."
            )
        )
        # try_open_dataset returns a valid dataset
        mock_ds = MagicMock()
        handler.try_open_dataset = MagicMock(return_value=mock_ds)

        # batch_files as fake file-like objects
        class FakeFile:
            def __init__(self, name):
                self.path = f"bucket/prefix/{name}"

        batch_files = [FakeFile("file_a.nc"), FakeFile("file_b.nc")]

        with (
            patch.object(
                GenericHandler,
                "batch_process_fileset",
                return_value=iter([["uri_a", "uri_b"]]),
            ),
            patch(
                "aodn_cloud_optimised.lib.GenericZarrHandler.create_fileset",
                return_value=batch_files,
            ),
        ):
            # Should complete without raising and without calling fallback
            GenericHandler.publish_cloud_optimised_fileset_batch(
                handler, ["uri_a", "uri_b"]
            )

        # fallback_to_individual_processing must NOT have been called
        handler.fallback_to_individual_processing.assert_not_called()
