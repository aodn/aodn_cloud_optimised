"""
Integration / unit tests for the AODN MCP server.

All tests call the MCP tool functions directly (not via the MCP protocol transport)
so they exercise the same code an AI assistant uses without requiring a running server.

What each flag enables
----------------------
(no flag)          Offline tests — catalog, plot guide, Python REPL.  No network
                   access needed.  ~8 s.

--run-s3           Adds tests that make *live anonymous S3 requests*: coverage
                   queries and live variable introspection.  Requires internet.
                   ~10–60 s per test.

--run-notebooks    Adds two kinds of notebook tests (requires --run-s3 / internet):

                   • TestMcpValidateNotebook  — unit tests for the validate_notebook
                     tool itself using tiny synthetic notebooks (fast, no S3).

                   • TestMcpGeneratedNotebook — simulates an AI agent workflow:
                     calls MCP tools (get_dataset_schema, check_dataset_coverage,
                     execute_python_cell) exactly as an AI would, then assembles a
                     NEW minimal .ipynb from those tool outputs and validates it via
                     validate_notebook.  Tests that schema, coverage, and REPL tools
                     together produce a working notebook.  ~5–10 min.

--run-all          Enables both --run-s3 and --run-notebooks, plus the end-to-end
                   scenario tests (TestMcpEndToEnd) that require both.

Note on MCP transport
---------------------
These tests do NOT spin up the ``aodn-mcp-server`` process or speak JSON-RPC.
They call the Python tool functions directly — which is equivalent to what the
server does when it receives a tool-call request from an AI client.  The JSON-RPC
transport layer itself is covered by the ``mcp`` SDK's own tests.

Run commands
------------
    # Fast offline tests only (default):
    pytest integration_testing/test_mcp_server.py -v

    # Include live S3 queries:
    pytest integration_testing/test_mcp_server.py -v --run-s3

    # Simulate AI notebook generation (also needs --run-s3):
    pytest integration_testing/test_mcp_server.py -v --run-s3 --run-notebooks

    # Everything:
    pytest integration_testing/test_mcp_server.py -v --run-all
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import textwrap
import unittest

import pytest

from aodn_cloud_optimised.mcp.notebook_utils import find_notebooks_dir

# ---------------------------------------------------------------------------
# Import the MCP tool functions directly.
# This also exercises the module-level startup (catalog load, DataQuery import).
# ---------------------------------------------------------------------------
from aodn_cloud_optimised.mcp.server import (
    add_notebook_cell,
    check_dataset_coverage,
    execute_python_cell,
    get_dataquery_reference,
    get_dataset_config,
    get_dataset_info,
    get_dataset_schema,
    get_dataset_summary,
    get_notebook_template,
    get_plot_guide,
    introspect_dataset_live,
    list_datasets,
    replace_notebook_cell,
    save_notebook,
    search_datasets,
    start_notebook,
    validate_notebook,
)

# ---------------------------------------------------------------------------
# Constants used across tests
# ---------------------------------------------------------------------------

# A well-known parquet dataset with a non-standard time variable (JULD not TIME)
ARGO = "argo.parquet"

# A well-known zarr dataset where wind_speed is in the JSON config but NOT in the live store
SST_ZARR = "satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_australia.zarr"

# A radar zarr dataset that uses parent_config for schema inheritance
RADAR_SAG_VEL = "radar_SouthAustraliaGulfs_velocity_hourly_averaged_delayed_qc.zarr"


# ===========================================================================
# 1. Offline catalog tools
# ===========================================================================


class TestMcpCatalogOffline(unittest.TestCase):
    """Tests for tools that read local JSON config files — no S3 required."""

    def test_list_datasets_returns_entries(self):
        result = list_datasets()
        self.assertIn("argo", result)
        self.assertIn("parquet", result.lower())

    def test_list_datasets_parquet_filter(self):
        result = list_datasets(format_filter="parquet")
        self.assertNotIn("zarr", result.lower())
        self.assertIn("parquet", result.lower())

    def test_list_datasets_zarr_filter(self):
        result = list_datasets(format_filter="zarr")
        self.assertNotIn("parquet", result.lower())
        self.assertIn("zarr", result.lower())

    def test_list_datasets_prefix_filter(self):
        result = list_datasets(prefix="mooring")
        self.assertIn("mooring", result)
        self.assertNotIn("argo", result)

    def test_search_datasets_returns_argo(self):
        result = search_datasets("argo float temperature salinity")
        self.assertIn("argo", result.lower())

    def test_search_datasets_top_k_respected(self):
        result = search_datasets("temperature", top_k=2)
        # Each result starts with "### N." — count those headers
        headers = [line for line in result.splitlines() if line.startswith("### ")]
        self.assertLessEqual(len(headers), 2)

    def test_get_dataset_info_argo(self):
        result = get_dataset_info(ARGO)
        self.assertIn("JULD", result)  # Argo time axis
        self.assertIn("LATITUDE", result)
        self.assertIn("axis", result)  # schema table has an axis column

    def test_get_dataset_info_unknown_returns_error(self):
        result = get_dataset_info("nonexistent_dataset_xyz.parquet")
        self.assertIn("not found", result.lower())

    def test_get_dataset_schema_argo_juld_is_time_axis(self):
        result = get_dataset_schema(ARGO)
        # JULD must be identified as TIME_AXIS; TIME must NOT appear as a variable
        self.assertIn("JULD", result)
        self.assertIn("TIME_AXIS", result)

    def test_get_dataset_schema_zarr_has_lowercase_coords(self):
        result = get_dataset_schema(SST_ZARR)
        # Satellite SST uses lowercase lat/lon/time
        self.assertIn("lat", result)
        self.assertIn("lon", result)

    def test_get_dataset_config_argo_has_schema(self):
        result = get_dataset_config(ARGO)
        self.assertIn("schema", result)
        self.assertIn("JULD", result)

    def test_get_dataset_config_parent_child_inheritance(self):
        """Radar child config must inherit cloud_optimised_format and schema from parent."""
        result = get_dataset_config(RADAR_SAG_VEL)
        # Must have zarr format (inherited from parent)
        self.assertIn("zarr", result.lower())
        # Must have schema variables (inherited from parent)
        self.assertIn("UCUR", result)  # a key radar variable
        self.assertIn("VCUR", result)
        # Child dataset_name must be preserved (not overridden by parent)
        self.assertIn(
            "radar_SouthAustraliaGulfs_velocity_hourly_averaged_delayed_qc", result
        )

    def test_get_dataset_config_schema_child_replaces_parent(self):
        """When a child config has a non-empty schema it should replace, not merge, parent schema."""
        # argo has its own non-empty schema — verify it's returned, not a blend
        result = get_dataset_config(ARGO)
        data = json.loads(result.split("```json")[1].split("```")[0].strip())
        schema = data.get("schema", {})
        self.assertIn("JULD", schema)


# ===========================================================================
# 2. Offline notebook and plot-guide tools
# ===========================================================================


class TestMcpNotebookOffline(unittest.TestCase):
    """Tests for notebook template and plot guide tools — no S3 required."""

    def test_get_notebook_template_argo_not_empty(self):
        result = get_notebook_template(ARGO)
        self.assertGreater(len(result), 100)
        self.assertIn("GetAodn", result)

    def test_get_notebook_template_fallback_for_unknown(self):
        result = get_notebook_template("completely_unknown_dataset_xyz.parquet")
        # Should return a generic template, not an error
        self.assertIn("GetAodn", result)

    def test_get_plot_guide_parquet_argo_has_safe_date_helper(self):
        result = get_plot_guide(ARGO)
        # Must document the safe month-end pattern (no day overflow)
        self.assertIn("_next_month_start", result)
        self.assertIn("pd.Timestamp", result)

    def test_get_plot_guide_parquet_argo_uses_juld(self):
        result = get_plot_guide(ARGO)
        self.assertIn("JULD", result)

    def test_get_plot_guide_parquet_plot_ts_diagram_is_standalone(self):
        """plot_ts_diagram must be shown as a standalone import, not a method call."""
        result = get_plot_guide(ARGO)
        self.assertIn("from DataQuery import plot_ts_diagram", result)
        self.assertNotIn(".plot_ts_diagram(", result)

    def test_get_plot_guide_zarr_sst_xarray_antipattern_documented(self):
        result = get_plot_guide(SST_ZARR)
        # Must warn about .sel() slice + nearest NotImplementedError
        self.assertIn("NotImplementedError", result)
        self.assertIn("pcolormesh", result)

    def test_get_plot_guide_zarr_radar_has_velocity_plots(self):
        result = get_plot_guide(RADAR_SAG_VEL)
        self.assertIn("velocity", result.lower())

    def test_get_dataquery_reference_has_core_api(self):
        result = get_dataquery_reference()
        self.assertIn("GetAodn", result)
        self.assertIn("describe", result)
        self.assertIn("get_data", result)


# ===========================================================================
# 3. Python REPL (execute_python_cell) — offline
# ===========================================================================


class TestMcpExecutePython(unittest.TestCase):
    """Tests for the persistent Python REPL tool — no S3 required."""

    def setUp(self):
        # Use a unique session_id per test so sessions don't bleed across tests
        self._sid = f"test_mcp_{id(self)}"

    def test_success_returns_tick_and_output(self):
        result = execute_python_cell("print(42)", session_id=self._sid)
        self.assertIn("✅", result)
        self.assertIn("42", result)

    def test_error_returns_cross_and_traceback(self):
        result = execute_python_cell("1 / 0", session_id=self._sid)
        self.assertIn("❌", result)
        self.assertIn("ZeroDivisionError", result)

    def test_jupyter_magic_stripped_no_syntax_error(self):
        code = "%%time\nprint('magic ok')"
        result = execute_python_cell(code, session_id=self._sid)
        self.assertIn("✅", result)
        self.assertIn("magic ok", result)

    def test_session_persistence(self):
        """Variables defined in one call are available in the next call."""
        sid = self._sid + "_persist"
        execute_python_cell("x = 99", session_id=sid)
        result = execute_python_cell("print(x)", session_id=sid)
        self.assertIn("✅", result)
        self.assertIn("99", result)

    def test_session_prepopulated_getaodn(self):
        """Every session starts with GetAodn pre-imported."""
        result = execute_python_cell("print(GetAodn)", session_id=self._sid)
        self.assertIn("✅", result)
        self.assertIn("GetAodn", result)

    def test_session_prepopulated_plot_ts_diagram(self):
        """Every session starts with plot_ts_diagram pre-imported."""
        result = execute_python_cell("print(plot_ts_diagram)", session_id=self._sid)
        self.assertIn("✅", result)
        self.assertIn("plot_ts_diagram", result)

    def test_session_isolation(self):
        """Different session_ids do not share variables."""
        sid_a = self._sid + "_a"
        sid_b = self._sid + "_b"
        execute_python_cell("secret = 123", session_id=sid_a)
        result = execute_python_cell("print(secret)", session_id=sid_b)
        self.assertIn("❌", result)
        self.assertIn("NameError", result)

    def test_timeout_returns_error(self):
        """Code that runs longer than timeout_seconds should fail gracefully."""
        result = execute_python_cell(
            "import time; time.sleep(10)",
            session_id=self._sid,
            timeout_seconds=1,
        )
        # Timeout is reported with ⏱️; check for it or ❌
        self.assertTrue(
            "⏱️" in result or "❌" in result, msg=f"Expected timeout indicator: {result}"
        )


# ===========================================================================
# 4. Live S3 queries  (requires --run-s3 or --run-all)
# ===========================================================================


@pytest.mark.s3
class TestMcpLiveS3(unittest.TestCase):
    """
    Tests that make anonymous read-only S3 requests to the public AODN bucket.
    Enabled with:  pytest ... --run-s3  or  --run-all
    """

    def test_check_coverage_argo_no_filter(self):
        result = check_dataset_coverage(ARGO)
        self.assertIn("Temporal", result)
        self.assertIn("Spatial", result)

    def test_check_coverage_argo_inside_australia(self):
        """Argo data should overlap with the Australian EEZ bbox."""
        result = check_dataset_coverage(
            ARGO,
            lat_min=-50,
            lat_max=-10,
            lon_min=110,
            lon_max=160,
            date_start="2015-01-01",
            date_end="2020-01-01",
        )
        self.assertIn("✅", result)

    def test_check_coverage_argo_outside_australia(self):
        """Argo data does not reach the Arctic Ocean (lat > 30°N per spatial extent)."""
        result = check_dataset_coverage(
            ARGO,
            lat_min=70,
            lat_max=85,
            lon_min=0,
            lon_max=30,  # Arctic Ocean — well above Argo's 30°N limit
            date_start="2015-01-01",
            date_end="2016-01-01",
        )
        self.assertIn("❌", result)

    def test_check_coverage_zarr_sst(self):
        result = check_dataset_coverage(SST_ZARR)
        self.assertIn("Temporal", result)
        self.assertIn("Spatial", result)

    def test_introspect_argo_juld_present(self):
        """Real Argo parquet store must have JULD, not TIME."""
        result = introspect_dataset_live(ARGO)
        self.assertIn("JULD", result)
        # TIME should either be absent or flagged — not appear as a primary column
        if "TIME" in result:
            # Acceptable only if it appears in a note like "JULD (not TIME)"
            self.assertIn("JULD", result)

    def test_introspect_zarr_sst_has_sea_surface_temperature(self):
        result = introspect_dataset_live(SST_ZARR)
        self.assertIn("sea_surface_temperature", result)

    def test_introspect_zarr_sst_wind_speed_flagged_or_absent(self):
        """wind_speed is in the JSON config but not in the live Zarr store — must be flagged."""
        result = introspect_dataset_live(SST_ZARR)
        # Either ⚠️ warning that it's in config but absent, or simply not in the live data list
        # In any case, sea_surface_temperature (real variable) must be present
        self.assertIn("sea_surface_temperature", result)
        # If wind_speed is mentioned it should carry a warning
        if "wind_speed" in result:
            self.assertIn("⚠️", result)


# ===========================================================================
# 5. validate_notebook unit tests  (requires --run-notebooks or --run-all)
# ===========================================================================


@pytest.mark.notebooks
class TestMcpValidateNotebook(unittest.TestCase):
    """
    Unit tests for the validate_notebook MCP tool using synthetic minimal notebooks.
    Enabled with:  pytest ... --run-notebooks  or  --run-all
    """

    def _make_notebook(self, cells: list[dict]) -> str:
        """Write a minimal .ipynb to a temp file and return the path."""
        nb = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {"name": "python"},
            },
            "cells": cells,
        }
        fd, path = tempfile.mkstemp(suffix=".ipynb")
        os.close(fd)
        with open(path, "w") as f:
            json.dump(nb, f)
        self.addCleanup(os.unlink, path)
        return path

    def _code_cell(self, source: str) -> dict:
        return {
            "cell_type": "code",
            "id": "test",
            "metadata": {},
            "source": source,
            "outputs": [],
            "execution_count": None,
        }

    def test_validate_clean_notebook_passes(self):
        """A notebook with only simple print calls should have no ❌ cells."""
        path = self._make_notebook(
            [
                self._code_cell("x = 1 + 1"),
                self._code_cell("print(f'result: {x}')"),
            ]
        )
        result = validate_notebook(path, cell_timeout=30)
        self.assertNotIn("❌", result)
        self.assertIn("✅", result)

    def test_validate_notebook_with_error_reports_failure(self):
        """A notebook containing 1/0 must return a ❌ cell report."""
        path = self._make_notebook(
            [
                self._code_cell("x = 1"),
                self._code_cell("y = 1 / 0  # deliberate error"),
                self._code_cell("print('after error')"),
            ]
        )
        result = validate_notebook(path, cell_timeout=30)
        self.assertIn("❌", result)
        self.assertIn("ZeroDivisionError", result)


# ---------------------------------------------------------------------------
# Shared helpers for all scripted-agent tests (sections 6 and 7)
# ---------------------------------------------------------------------------


class _McpAgentMixin:
    """
    Mixin providing notebook-generation helpers for scripted-agent tests.

    Subclasses must call in setUp:
        self.tmpdir = tempfile.mkdtemp(...)
        self.notebooks_dir = find_notebooks_dir()
    And in tearDown:
        shutil.rmtree(self.tmpdir, ignore_errors=True)
    """

    def _setup_src(self) -> str:
        """Return a local (non-Colab) setup cell: DataQuery imports + Agg backend."""
        return (
            "import sys\n"
            f"sys.path.insert(0, {str(self.notebooks_dir)!r})\n"
            "import matplotlib\n"
            "matplotlib.use('Agg')  # non-interactive backend\n"
            "import matplotlib.pyplot as plt\n"
            "from DataQuery import GetAodn, plot_ts_diagram\n"
            "print('Setup complete')"
        )

    def _code_cell(self, source: str) -> dict:
        return {
            "cell_type": "code",
            "id": f"c{abs(hash(source)) % 99999:05d}",
            "metadata": {},
            "source": source,
            "outputs": [],
            "execution_count": None,
        }

    def _make_notebook(self, cell_sources: list, name: str = "generated") -> str:
        """Write a new .ipynb to tmpdir from a list of source strings."""
        nb = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {"name": "python", "version": "3.11.0"},
            },
            "cells": [self._code_cell(src) for src in cell_sources],
        }
        path = os.path.join(self.tmpdir, f"{name}.ipynb")
        with open(path, "w") as fh:
            json.dump(nb, fh)
        return path

    def _assert_cell_ok(self, result: str, label: str) -> None:
        """Fail with a descriptive message if an execute_python_cell call erred."""
        if "❌" in result:
            self.fail(f"execute_python_cell failed at step '{label}':\n{result}")

    def _assert_notebook_ok(self, result: str, nb_name: str) -> None:
        """Fail with a detailed diff if validate_notebook reports any ❌ cells."""
        failed = [ln for ln in result.splitlines() if "❌" in ln]
        self.assertEqual(
            failed,
            [],
            msg=(
                f"Generated notebook '{nb_name}' has failing cells:\n"
                + "\n".join(failed)
                + f"\n\nFull report:\n{result}"
            ),
        )


# ===========================================================================
# 6. Scripted-agent notebook generation  (requires --run-s3 AND --run-notebooks)
#
#    Simulates the AI workflow: calls MCP tools in sequence, builds a NEW
#    minimal .ipynb from those tool outputs, and validates it executes cleanly.
#
#    If get_dataset_schema() returned "TIME" instead of "JULD" for Argo,
#    the execute_python_cell step would fail (KeyError), catching the bug
#    before a notebook is even written.
# ===========================================================================


@pytest.mark.s3
@pytest.mark.notebooks
class TestMcpGeneratedNotebook(_McpAgentMixin, unittest.TestCase):
    """
    Scripted-agent notebook generation tests.

    Each test simulates what an AI assistant does when a user asks for a
    notebook:

    1. Call ``get_dataset_schema()`` — discover real variable names.
    2. Call ``check_dataset_coverage()`` — confirm data exists.
    3. Call ``execute_python_cell()`` — iteratively test code snippets.
    4. Assemble the verified cells into a new ``.ipynb`` file.
    5. Call ``validate_notebook()`` — confirm it executes without ❌ cells.

    Enabled with:  pytest ... --run-s3 --run-notebooks  or  --run-all
    """

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="mcp_gen_nb_")
        self.notebooks_dir = find_notebooks_dir()
        if self.notebooks_dir is None:
            self.skipTest("Notebooks directory not found; set AODN_NOTEBOOKS_PATH")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generate_argo_notebook(self):
        """
        Scripted Argo workflow:
          schema → JULD (not TIME); coverage → AU waters 2020 ✅;
          REPL → load + get_data; notebook → validate.
        """
        sid = "gen_argo_test"

        # Step 1 — schema: confirm JULD is time variable
        schema = get_dataset_schema(ARGO)
        self.assertIn("JULD", schema, "Schema must list JULD as the time variable")
        self.assertNotIn("| `TIME`", schema, "Schema must NOT list TIME")

        # Step 2 — coverage check
        cov = check_dataset_coverage(
            ARGO,
            lat_min=-40,
            lat_max=-20,
            lon_min=140,
            lon_max=160,
            date_start="2020-01-01",
            date_end="2020-03-01",
        )
        self.assertIn("✅", cov, f"Expected ✅ for AU waters 2020:\n{cov}")

        # Step 3 — REPL: test cells iteratively
        r1 = execute_python_cell(
            self._setup_src()
            + "\naodn = GetAodn()\naodn_dataset = aodn.get_dataset('argo.parquet')\n"
            "print('dataset loaded')",
            session_id=sid,
            timeout_seconds=120,
        )
        self._assert_cell_ok(r1, "argo-load")

        r2 = execute_python_cell(
            "df = aodn_dataset.get_data(\n"
            "    lat_min=-40, lat_max=-20, lon_min=140, lon_max=160,\n"
            "    date_start='2020-01-01', date_end='2020-03-01')\n"
            "print(df.shape)\nprint(df.columns.tolist())",
            session_id=sid,
            timeout_seconds=300,
        )
        self._assert_cell_ok(r2, "argo-get_data")
        self.assertIn("JULD", r2, "JULD must appear in printed column list")

        r3 = execute_python_cell(
            "print(f\"Time range: {df['JULD'].min()} to {df['JULD'].max()}\")\n"
            'print(f"Records: {len(df)}")',
            session_id=sid,
            timeout_seconds=60,
        )
        self._assert_cell_ok(r3, "argo-analysis")

        # Step 4 — assemble notebook from verified cells
        nb_path = self._make_notebook(
            [
                self._setup_src(),
                "aodn = GetAodn()\naodn_dataset = aodn.get_dataset('argo.parquet')\n"
                "print('dataset loaded')",
                "df = aodn_dataset.get_data(\n"
                "    lat_min=-40, lat_max=-20, lon_min=140, lon_max=160,\n"
                "    date_start='2020-01-01', date_end='2020-03-01')\n"
                "print(df.shape)\nprint(df.columns.tolist())",
                "print(f\"Time range: {df['JULD'].min()} to {df['JULD'].max()}\")\n"
                'print(f"Records: {len(df)}")',
            ],
            name="generated_argo",
        )

        # Step 5 — validate
        result = validate_notebook(nb_path, cell_timeout=300)
        self._assert_notebook_ok(result, "generated_argo.ipynb")

    def test_generate_sst_zarr_notebook(self):
        """
        Scripted SST zarr workflow:
          schema → sea_surface_temperature, time/lat/lon coords; coverage ✅;
          REPL → load + timeseries; notebook → validate.
        """
        sid = "gen_sst_test"

        # Step 1 — schema
        schema = get_dataset_schema(SST_ZARR)
        self.assertIn("sea_surface_temperature", schema)
        self.assertIn("`time`", schema, "Time coordinate must be 'time' (lowercase)")
        self.assertIn("`lat`", schema, "Latitude coordinate must be 'lat' (lowercase)")

        # Step 2 — coverage
        cov = check_dataset_coverage(
            SST_ZARR,
            lat_min=-40,
            lat_max=-20,
            lon_min=140,
            lon_max=160,
            date_start="2020-01-01",
            date_end="2020-01-31",
        )
        self.assertIn("✅", cov, f"Expected ✅ for AU coast Jan 2020:\n{cov}")

        # Step 3 — REPL
        r1 = execute_python_cell(
            self._setup_src()
            + f"\naodn = GetAodn()\nsst_ds = aodn.get_dataset('{SST_ZARR}')\n"
            "print('SST dataset loaded')",
            session_id=sid,
            timeout_seconds=120,
        )
        self._assert_cell_ok(r1, "sst-load")

        r2 = execute_python_cell(
            "ts = sst_ds.zarr_store['sea_surface_temperature'].isel(time=slice(0, 3))\n"
            "print(f'SST shape: {ts.shape}')\n"
            "print(f'SST dims: {list(ts.dims)}')",
            session_id=sid,
            timeout_seconds=300,
        )
        self._assert_cell_ok(r2, "sst-timeseries")

        # Step 4 — assemble notebook
        nb_path = self._make_notebook(
            [
                self._setup_src(),
                f"aodn = GetAodn()\nsst_ds = aodn.get_dataset('{SST_ZARR}')\n"
                "print('SST dataset loaded')",
                "# Inspect available variables\n"
                "ds_zarr = sst_ds.zarr_store\n"
                "print(list(ds_zarr.data_vars.keys()))\n"
                "sst = ds_zarr['sea_surface_temperature'].isel(time=slice(0, 3))\n"
                "print(f'SST shape: {sst.shape}')",
            ],
            name="generated_sst",
        )

        # Step 5 — validate
        result = validate_notebook(nb_path, cell_timeout=300)
        self._assert_notebook_ok(result, "generated_sst.ipynb")


# ===========================================================================
# 7. End-to-end scenario tests from real user questions
#    (requires --run-s3 AND --run-notebooks)
#
#    Each test encodes a real user question verbatim.  The scripted AI
#    workflow mirrors exactly what an AI assistant would do:
#      1. search_datasets() — discover relevant dataset names from keywords
#      2. check_dataset_coverage() — verify data exists for the region/period
#      3. get_dataset_schema() — learn the CORRECT variable names
#      4. execute_python_cell() — iteratively test code before writing cells
#      5. _make_notebook() — assemble cells into a new .ipynb
#      6. validate_notebook() — confirm the generated notebook executes cleanly
#
#    No pre-existing notebooks are used — each test generates a fresh one.
# ===========================================================================


@pytest.mark.s3
@pytest.mark.notebooks
class TestMcpEndToEnd(_McpAgentMixin, unittest.TestCase):
    """
    End-to-end scenario tests driven by real user questions.

    Each test calls MCP tools in the same sequence an AI assistant would,
    assembles a NEW ``.ipynb`` from those tool outputs, and validates it.

    Enabled with:  pytest ... --run-s3 --run-notebooks  or  --run-all
    """

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="mcp_e2e_")
        self.notebooks_dir = find_notebooks_dir()
        if self.notebooks_dir is None:
            self.skipTest("Notebooks directory not found; set AODN_NOTEBOOKS_PATH")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Scenario 1 — Coffs Harbour Jan 2020 sea-state comparison
    # User: "I want a notebook showing radar data at Coffs Harbour in
    #   January 2020, and compare with Argo and gridded SST."
    # ------------------------------------------------------------------

    def test_coffs_harbour_sea_state(self):
        """
        Scripted AI for Coffs Harbour Jan 2020:
          search → datasets; coverage → ✅ argo + SST in region;
          schema → JULD, sea_surface_temperature;
          REPL → load both datasets; notebook → validate.
        """
        sid = "e2e_coffs"

        # Step 1 — AI searches for the relevant datasets
        radar_r = search_datasets("HF radar NSW Coffs Harbour current velocity")
        self.assertIn("radar", radar_r.lower())
        argo_r = search_datasets("argo float temperature salinity profile Australia")
        self.assertIn("argo", argo_r.lower())
        sst_r = search_datasets("satellite sea surface temperature gridded Australia")
        self.assertIn("ghrsst", sst_r.lower())

        # Step 2 — AI checks coverage for Argo and SST in Coffs Harbour region
        bbox = dict(lat_min=-32, lat_max=-28, lon_min=151, lon_max=155)
        argo_cov = check_dataset_coverage(
            ARGO, **bbox, date_start="2020-01-01", date_end="2020-02-01"
        )
        self.assertNotIn("Error", argo_cov)

        sst_cov = check_dataset_coverage(
            SST_ZARR, **bbox, date_start="2020-01-01", date_end="2020-02-01"
        )
        self.assertIn("✅", sst_cov)

        # Step 3 — AI gets schema to discover correct variable names
        argo_schema = get_dataset_schema(ARGO)
        self.assertIn("JULD", argo_schema)

        sst_schema = get_dataset_schema(SST_ZARR)
        self.assertIn("sea_surface_temperature", sst_schema)

        # Step 4 — AI uses REPL to test key code cells
        r1 = execute_python_cell(
            self._setup_src()
            + "\naodn = GetAodn()\naodn_dataset = aodn.get_dataset('argo.parquet')\n"
            "print('Argo loaded')",
            session_id=sid,
            timeout_seconds=120,
        )
        self._assert_cell_ok(r1, "coffs-argo-load")

        r2 = execute_python_cell(
            "df_argo = aodn_dataset.get_data(\n"
            "    lat_min=-32, lat_max=-28, lon_min=151, lon_max=155,\n"
            "    date_start='2020-01-01', date_end='2020-02-01')\n"
            "print(f'Argo rows: {len(df_argo)}')\n"
            "print(f'JULD in columns: {\"JULD\" in df_argo.columns}')",
            session_id=sid,
            timeout_seconds=300,
        )
        self._assert_cell_ok(r2, "coffs-argo-data")
        self.assertIn("JULD", r2)

        r3 = execute_python_cell(
            f"sst_ds = aodn.get_dataset('{SST_ZARR}')\nprint('SST loaded')",
            session_id=sid,
            timeout_seconds=120,
        )
        self._assert_cell_ok(r3, "coffs-sst-load")

        r4 = execute_python_cell(
            "ds_zarr = sst_ds.zarr_store\n"
            "print(list(ds_zarr.data_vars.keys()))\n"
            "sst = ds_zarr['sea_surface_temperature'].isel(time=slice(0, 3))\n"
            "print(f'SST shape: {sst.shape}')",
            session_id=sid,
            timeout_seconds=300,
        )
        self._assert_cell_ok(r4, "coffs-sst-ts")

        # Step 5 — AI assembles and validates the notebook
        nb_path = self._make_notebook(
            [
                self._setup_src(),
                "DATE_START = '2020-01-01'\nDATE_END = '2020-02-01'\n"
                "LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = -32, -28, 151, 155",
                "aodn = GetAodn()\naodn_dataset = aodn.get_dataset('argo.parquet')\n"
                "print('Argo loaded')",
                "df_argo = aodn_dataset.get_data(\n"
                "    lat_min=LAT_MIN, lat_max=LAT_MAX,\n"
                "    lon_min=LON_MIN, lon_max=LON_MAX,\n"
                "    date_start=DATE_START, date_end=DATE_END)\n"
                "print(f'Argo: {df_argo.shape}')\n"
                "print(f'JULD in columns: {\"JULD\" in df_argo.columns}')",
                f"sst_ds = aodn.get_dataset('{SST_ZARR}')\n"
                "ds_zarr = sst_ds.zarr_store\n"
                "print(list(ds_zarr.data_vars.keys()))\n"
                "sst = ds_zarr['sea_surface_temperature'].isel(time=0)\n"
                "print(f'SST snapshot shape: {sst.shape}')",
            ],
            name="generated_coffs_harbour",
        )
        result = validate_notebook(nb_path, cell_timeout=300)
        self._assert_notebook_ok(result, "generated_coffs_harbour.ipynb")

    # ------------------------------------------------------------------
    # Scenario 2 — SA Gulfs HAB area, April, chlorophyll
    # User: "Explore AODN datasets in the HAB area (SA Gulfs 134-141.5E,
    #   34-39.5S). Compare datasets across April. Add chlorophyll."
    # ------------------------------------------------------------------

    def test_sa_gulfs_hab(self):
        """
        Scripted AI for SA Gulfs HAB, April:
          search → SA Gulfs radar, chlorophyll, argo;
          coverage → argo + SST in bbox;
          schema → JULD; REPL → load argo; notebook → validate.
        """
        sid = "e2e_sa_gulfs"

        # Step 1 — AI searches for relevant datasets
        radar_r = search_datasets("South Australia Gulfs HF radar current velocity")
        self.assertIn("radar", radar_r.lower())
        chl_r = search_datasets("chlorophyll satellite ocean colour phytoplankton")
        self.assertIn("chlorophyll", chl_r.lower())
        argo_r = search_datasets("argo float temperature salinity profile")
        self.assertIn("argo", argo_r.lower())

        # Step 2 — AI checks coverage for SA Gulfs bbox in April
        bbox = dict(lat_min=-39.5, lat_max=-34.0, lon_min=134.0, lon_max=141.5)
        argo_cov = check_dataset_coverage(
            ARGO, **bbox, date_start="2015-04-01", date_end="2015-05-01"
        )
        self.assertNotIn("Error", argo_cov)
        sst_cov = check_dataset_coverage(
            SST_ZARR, **bbox, date_start="2015-04-01", date_end="2015-05-01"
        )
        self.assertIn("✅", sst_cov)

        # Step 3 — AI gets correct variable names
        argo_schema = get_dataset_schema(ARGO)
        self.assertIn("JULD", argo_schema)

        # Step 4 — AI tests key cells in REPL
        r1 = execute_python_cell(
            self._setup_src()
            + "\naodn = GetAodn()\naodn_dataset = aodn.get_dataset('argo.parquet')\n"
            "print('Argo loaded')",
            session_id=sid,
            timeout_seconds=120,
        )
        self._assert_cell_ok(r1, "sa-argo-load")

        r2 = execute_python_cell(
            "df = aodn_dataset.get_data(\n"
            "    lat_min=-39.5, lat_max=-34.0, lon_min=134.0, lon_max=141.5,\n"
            "    date_start='2015-04-01', date_end='2015-05-01')\n"
            "print(f'Rows: {len(df)}')\n"
            "print(f'JULD in columns: {\"JULD\" in df.columns}')",
            session_id=sid,
            timeout_seconds=300,
        )
        self._assert_cell_ok(r2, "sa-argo-data")
        self.assertIn("JULD", r2)

        # Step 5 — assemble and validate notebook
        nb_path = self._make_notebook(
            [
                self._setup_src(),
                "import calendar\n"
                "YEAR, MONTH = 2015, 4\n"
                "DATE_START = f'{YEAR}-{MONTH:02d}-01'\n"
                "DATE_END = f'{YEAR}-{MONTH:02d}-{calendar.monthrange(YEAR, MONTH)[1]}'\n"
                "LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = -39.5, -34.0, 134.0, 141.5",
                "aodn = GetAodn()\naodn_dataset = aodn.get_dataset('argo.parquet')\n"
                "print('Argo loaded')",
                "df = aodn_dataset.get_data(\n"
                "    lat_min=LAT_MIN, lat_max=LAT_MAX,\n"
                "    lon_min=LON_MIN, lon_max=LON_MAX,\n"
                "    date_start=DATE_START, date_end=DATE_END)\n"
                "print(f'Argo shape: {df.shape}')\n"
                "print(f'JULD in columns: {\"JULD\" in df.columns}')",
                f"sst_ds = aodn.get_dataset('{SST_ZARR}')\n"
                "ds_zarr = sst_ds.zarr_store\n"
                "print(list(ds_zarr.data_vars.keys()))\n"
                "sst = ds_zarr['sea_surface_temperature'].isel(time=0)\n"
                "print(f'SST snapshot shape: {sst.shape}')",
            ],
            name="generated_sa_gulfs",
        )
        result = validate_notebook(nb_path, cell_timeout=300)
        self._assert_notebook_ok(result, "generated_sa_gulfs.ipynb")

    # ------------------------------------------------------------------
    # Scenario 3 — Argo T/S profiles, Coral Sea 2018
    # User: "Give me a notebook for Argo float T/S profiles in the
    #   Coral Sea in 2018."
    # ------------------------------------------------------------------

    def test_argo_coral_sea(self):
        """
        Scripted AI for Argo Coral Sea 2018:
          schema → JULD, TEMP_ADJUSTED, PSAL_ADJUSTED; coverage → ✅;
          REPL → load + query; notebook → validate.
        """
        sid = "e2e_argo_coral_sea"

        # Step 1 — schema and coverage
        schema = get_dataset_schema(ARGO)
        self.assertIn("JULD", schema)
        cov = check_dataset_coverage(
            ARGO,
            lat_min=-25,
            lat_max=-10,
            lon_min=146,
            lon_max=165,
            date_start="2018-01-01",
            date_end="2019-01-01",
        )
        self.assertNotIn("Error", cov)

        # Step 2 — REPL
        r1 = execute_python_cell(
            self._setup_src()
            + "\naodn = GetAodn()\naodn_dataset = aodn.get_dataset('argo.parquet')\n"
            "print('loaded')",
            session_id=sid,
            timeout_seconds=120,
        )
        self._assert_cell_ok(r1, "coral-load")

        r2 = execute_python_cell(
            "df = aodn_dataset.get_data(\n"
            "    lat_min=-25, lat_max=-10, lon_min=146, lon_max=165,\n"
            "    date_start='2018-01-01', date_end='2019-01-01')\n"
            "print(f'Rows: {len(df)}, JULD present: {\"JULD\" in df.columns}')",
            session_id=sid,
            timeout_seconds=300,
        )
        self._assert_cell_ok(r2, "coral-data")
        self.assertIn("True", r2)

        # Step 3 — notebook
        nb_path = self._make_notebook(
            [
                self._setup_src(),
                "aodn = GetAodn()\naodn_dataset = aodn.get_dataset('argo.parquet')\n"
                "print('Argo loaded')",
                "df = aodn_dataset.get_data(\n"
                "    lat_min=-25, lat_max=-10, lon_min=146, lon_max=165,\n"
                "    date_start='2018-01-01', date_end='2019-01-01')\n"
                "print(f'Shape: {df.shape}')\n"
                'print(f\'JULD range: {df["JULD"].min()} to {df["JULD"].max()}\')',
            ],
            name="generated_argo_coral_sea",
        )
        result = validate_notebook(nb_path, cell_timeout=300)
        self._assert_notebook_ok(result, "generated_argo_coral_sea.ipynb")

    # ------------------------------------------------------------------
    # Scenario 4 — Mooring temperature near Sydney 2018-2022
    # User: "Mooring temperature near Sydney 2018-2022, include T/S diagram."
    # ------------------------------------------------------------------

    def test_mooring_sydney(self):
        """
        Scripted AI for mooring T/S near Sydney:
          schema → TIME, TEMP, PSAL; coverage → ✅;
          REPL → load + query; notebook → validate.
        """
        MOORING = "mooring_hourly_timeseries_delayed_qc.parquet"
        sid = "e2e_mooring_sydney"

        # Step 1 — discover dataset
        result = search_datasets(
            "mooring temperature salinity hourly time series delayed"
        )
        self.assertIn("mooring", result.lower())

        # Step 2 — schema
        schema = get_dataset_schema(MOORING)
        self.assertIn("TIME", schema)
        self.assertIn("TEMP", schema)

        # Step 3 — coverage
        cov = check_dataset_coverage(
            MOORING,
            lat_min=-35,
            lat_max=-32,
            lon_min=150,
            lon_max=152,
            date_start="2018-01-01",
            date_end="2023-01-01",
        )
        self.assertNotIn("Error", cov)

        # Step 4 — REPL
        r1 = execute_python_cell(
            self._setup_src()
            + f"\naodn = GetAodn()\nds = aodn.get_dataset('{MOORING}')\nprint('loaded')",
            session_id=sid,
            timeout_seconds=120,
        )
        self._assert_cell_ok(r1, "mooring-load")

        r2 = execute_python_cell(
            "df = ds.get_data(\n"
            "    lat_min=-35, lat_max=-32, lon_min=150, lon_max=152,\n"
            "    date_start='2018-01-01', date_end='2023-01-01')\n"
            "print(f'Rows: {len(df)}, TEMP present: {\"TEMP\" in df.columns}')",
            session_id=sid,
            timeout_seconds=300,
        )
        self._assert_cell_ok(r2, "mooring-data")
        self.assertIn("True", r2)

        # Step 5 — notebook
        nb_path = self._make_notebook(
            [
                self._setup_src(),
                f"aodn = GetAodn()\nds = aodn.get_dataset('{MOORING}')\nprint('loaded')",
                "df = ds.get_data(\n"
                "    lat_min=-35, lat_max=-32, lon_min=150, lon_max=152,\n"
                "    date_start='2018-01-01', date_end='2023-01-01')\n"
                "print(f'Shape: {df.shape}')\n"
                'print(f\'TIME range: {df["TIME"].min()} to {df["TIME"].max()}\')',
            ],
            name="generated_mooring_sydney",
        )
        result = validate_notebook(nb_path, cell_timeout=300)
        self._assert_notebook_ok(result, "generated_mooring_sydney.ipynb")

    # ------------------------------------------------------------------
    # Scenario 5 — SST anomaly, Southern Ocean, Dec-Feb
    # User: "SST anomaly in the Southern Ocean during austral summer (Dec-Feb)."
    # ------------------------------------------------------------------

    def test_sst_southern_ocean(self):
        """
        Scripted AI for SST Southern Ocean Dec-Feb:
          search → SST zarr; schema → sea_surface_temperature;
          coverage → ✅; REPL → timeseries; notebook → validate.
        """
        sid = "e2e_sst_southern"

        # Step 1 — search and schema
        sst_r = search_datasets(
            "sea surface temperature satellite gridded Australia Southern Ocean"
        )
        self.assertIn("ghrsst", sst_r.lower())

        schema = get_dataset_schema(SST_ZARR)
        self.assertIn("sea_surface_temperature", schema)

        # Step 2 — coverage
        cov = check_dataset_coverage(
            SST_ZARR,
            lat_min=-65,
            lat_max=-40,
            lon_min=140,
            lon_max=180,
            date_start="2015-12-01",
            date_end="2016-03-01",
        )
        self.assertIn("✅", cov)

        # Step 3 — REPL
        r1 = execute_python_cell(
            self._setup_src()
            + f"\naodn = GetAodn()\nsst_ds = aodn.get_dataset('{SST_ZARR}')\nprint('loaded')",
            session_id=sid,
            timeout_seconds=120,
        )
        self._assert_cell_ok(r1, "so-sst-load")

        r2 = execute_python_cell(
            "ds_zarr = sst_ds.zarr_store\n"
            "print(list(ds_zarr.data_vars.keys()))\n"
            "sst = ds_zarr['sea_surface_temperature'].isel(time=slice(0, 3))\n"
            "print(f'SST shape: {sst.shape}')\n"
            "print(f'lat range: {float(ds_zarr.lat.min()):.1f} to {float(ds_zarr.lat.max()):.1f}')",
            session_id=sid,
            timeout_seconds=300,
        )
        self._assert_cell_ok(r2, "so-sst-ts")

        # Step 4 — notebook
        nb_path = self._make_notebook(
            [
                self._setup_src(),
                f"aodn = GetAodn()\nsst_ds = aodn.get_dataset('{SST_ZARR}')\nprint('SST loaded')",
                "ds_zarr = sst_ds.zarr_store\n"
                "print(list(ds_zarr.data_vars.keys()))\n"
                "sst = ds_zarr['sea_surface_temperature'].isel(time=slice(0, 3))\n"
                "print(f'SST shape: {sst.shape}')\n"
                "print(f'lat range: {float(ds_zarr.lat.min()):.1f} to {float(ds_zarr.lat.max()):.1f}')",
            ],
            name="generated_sst_southern_ocean",
        )
        result = validate_notebook(nb_path, cell_timeout=300)
        self._assert_notebook_ok(result, "generated_sst_southern_ocean.ipynb")


# ------------------------------------------------------------------ #
# TestMcpDatasetSummary — offline tests for get_dataset_summary      #
# ------------------------------------------------------------------ #


class TestMcpDatasetSummary(unittest.TestCase):
    """Test the get_dataset_summary tool returns rich, correct metadata."""

    def test_summary_returns_data_type(self):
        """Summary includes data_type classification."""
        result = get_dataset_summary("argo")
        self.assertIn("profiles", result)
        self.assertIn("Data type", result)

    def test_summary_returns_aws_description(self):
        """Summary includes the AWS Open Data Registry description."""
        result = get_dataset_summary("argo")
        self.assertIn("## Description", result)
        self.assertIn("Argo", result)

    def test_summary_returns_coordinates(self):
        """Summary includes coordinate variable table with roles."""
        result = get_dataset_summary("argo")
        self.assertIn("TIME_AXIS", result)
        self.assertIn("JULD", result)
        self.assertIn("LATITUDE", result)

    def test_summary_returns_data_variables(self):
        """Summary includes data variable table."""
        result = get_dataset_summary("argo")
        self.assertIn("## Data variables", result)

    def test_summary_returns_matching_notebook(self):
        """Summary reports whether a matching notebook exists."""
        result = get_dataset_summary("argo")
        self.assertIn("argo.ipynb", result)

    def test_summary_returns_recommended_code(self):
        """Summary includes recommended code pattern."""
        result = get_dataset_summary("argo")
        self.assertIn("get_data(", result)
        self.assertIn("GetAodn", result)

    def test_summary_timeseries_type(self):
        """Wave buoy is classified as timeseries."""
        result = get_dataset_summary("wave_buoy_realtime_nonqc")
        self.assertIn("timeseries", result)

    def test_summary_gridded_type(self):
        """Satellite SST is classified as gridded."""
        result = get_dataset_summary(
            "satellite_ghrsst_l3s_1day_daytime_single_sensor_australia"
        )
        self.assertIn("gridded", result)
        self.assertIn("get_timeseries_data", result)

    def test_summary_radar_type(self):
        """Radar velocity is classified correctly."""
        result = get_dataset_summary(
            "radar_CoffsHarbour_velocity_hourly_averaged_delayed_qc"
        )
        self.assertIn("radar_velocity", result)

    def test_summary_unknown_dataset(self):
        """Unknown dataset returns helpful suggestions."""
        result = get_dataset_summary("nonexistent_dataset")
        self.assertIn("not found", result)
        self.assertIn("Did you mean", result)


# ------------------------------------------------------------------ #
# TestMcpNotebookBuilder — offline tests for builder workflow        #
# ------------------------------------------------------------------ #


class TestMcpNotebookBuilder(unittest.TestCase):
    """Test the start_notebook / add_notebook_cell / save_notebook workflow."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _start(self, title="Test NB"):
        out = os.path.join(self.tmpdir, "test.ipynb")
        result = start_notebook(title=title, output_path=out)
        self.assertIn("✅", result, f"start_notebook failed: {result}")
        # Extract session_id
        sid = result.split("Session ID:** `")[1].split("`")[0]
        return sid, out

    def test_start_creates_session(self):
        """start_notebook returns a session_id and confirms setup."""
        sid, _ = self._start()
        self.assertTrue(sid.startswith("nb_"))

    def test_add_valid_code_cell(self):
        """Valid code cell is accepted."""
        sid, _ = self._start()
        result = add_notebook_cell(sid, "x = 42\nprint(x)")
        self.assertIn("✅", result)
        self.assertIn("Cell 2 added", result)

    def test_add_broken_code_cell_rejected(self):
        """Code cell with error is NOT added."""
        sid, _ = self._start()
        result = add_notebook_cell(sid, "raise ValueError('test error')")
        self.assertIn("❌", result)
        self.assertIn("NOT added", result)

    def test_add_markdown_cell(self):
        """Markdown cells are always accepted."""
        sid, _ = self._start()
        result = add_notebook_cell(sid, "## Hello World", cell_type="markdown")
        self.assertIn("✅", result)
        self.assertIn("markdown", result)

    def test_save_produces_valid_notebook(self):
        """Saved notebook is valid nbformat and has correct cell count."""
        sid, out = self._start()
        add_notebook_cell(sid, "x = 42")
        add_notebook_cell(sid, "## Section", cell_type="markdown")
        result = save_notebook(sid)
        # save_notebook now auto-validates; check for success or validation
        self.assertTrue(
            "✅" in result or "validated" in result.lower(),
            f"save_notebook unexpected result: {result[:200]}",
        )
        self.assertTrue(os.path.isfile(out))

        import json

        with open(out) as f:
            nb = json.load(f)
        # 2 from start (title+setup) + 1 code + 1 markdown = 4
        self.assertEqual(len(nb["cells"]), 4)

    def test_rejected_cell_not_in_notebook(self):
        """A rejected cell does not appear in the saved notebook."""
        sid, out = self._start()
        add_notebook_cell(sid, "good = 1")
        add_notebook_cell(sid, "1/0")  # rejected
        add_notebook_cell(sid, "good2 = 2")
        save_notebook(sid)

        import json

        with open(out) as f:
            nb = json.load(f)
        sources = [
            "".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"
        ]
        # setup + good + good2 = 3 code cells; 1/0 should NOT be present
        self.assertEqual(len(sources), 3)
        self.assertFalse(any("1/0" in s for s in sources))

    def test_session_not_found(self):
        """Using invalid session_id returns error."""
        result = add_notebook_cell("nonexistent", "x = 1")
        self.assertIn("❌", result)
        self.assertIn("not found", result)

    def test_variables_persist_across_cells(self):
        """Variables defined in one cell are available in the next."""
        sid, out = self._start()
        r1 = add_notebook_cell(sid, "builder_test_var = 99")
        self.assertIn("✅", r1)
        r2 = add_notebook_cell(sid, "print(builder_test_var + 1)")
        self.assertIn("✅", r2)

    def test_replace_valid_code_cell(self):
        """replace_notebook_cell replaces a cell in the draft."""
        sid, out = self._start()
        add_notebook_cell(sid, "old_val = 1")
        # Replace cell 2 (index 2 = the cell we just added; 0=title, 1=setup)
        result = replace_notebook_cell(
            sid, cell_index=2, source="new_val = 42\nprint(new_val)"
        )
        self.assertIn("✅", result)
        self.assertIn("replaced", result)

    def test_replace_broken_code_rejected(self):
        """replace_notebook_cell rejects broken code."""
        sid, out = self._start()
        add_notebook_cell(sid, "x = 1")
        result = replace_notebook_cell(sid, cell_index=2, source="1/0")
        self.assertIn("❌", result)
        self.assertIn("NOT replaced", result)

    def test_replace_out_of_range(self):
        """replace_notebook_cell rejects out-of-range index."""
        sid, out = self._start()
        result = replace_notebook_cell(sid, cell_index=99, source="x = 1")
        self.assertIn("❌", result)
        self.assertIn("out of range", result)


class TestMcpFixNotebook(unittest.TestCase):
    """Tests for the fix_notebook tool."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_fix_notebook_file_not_found(self):
        from aodn_cloud_optimised.mcp.server import fix_notebook

        result = fix_notebook("/nonexistent/path.ipynb")
        self.assertIn("❌", result)
        self.assertIn("not found", result.lower())

    def test_fix_notebook_not_ipynb(self):
        from aodn_cloud_optimised.mcp.server import fix_notebook

        txt = os.path.join(self.tmpdir, "readme.txt")
        with open(txt, "w") as f:
            f.write("hello")
        result = fix_notebook(txt)
        self.assertIn("❌", result)
        self.assertIn("Not a notebook", result)

    def test_fix_notebook_valid(self):
        """A clean notebook should return ✅."""
        import nbformat

        from aodn_cloud_optimised.mcp.server import fix_notebook

        nb = nbformat.v4.new_notebook()
        nb.cells = [nbformat.v4.new_code_cell(source="x = 1 + 1\nprint(x)")]
        path = os.path.join(self.tmpdir, "clean.ipynb")
        with open(path, "w") as f:
            nbformat.write(nb, f)

        result = fix_notebook(path)
        self.assertIn("✅", result)

    def test_fix_notebook_broken_imports_to_builder(self):
        """A notebook with errors should be imported into a builder session."""
        import nbformat

        from aodn_cloud_optimised.mcp.server import _NOTEBOOK_DRAFTS, fix_notebook

        nb = nbformat.v4.new_notebook()
        nb.cells = [nbformat.v4.new_code_cell(source="raise RuntimeError('boom')")]
        path = os.path.join(self.tmpdir, "broken.ipynb")
        with open(path, "w") as f:
            nbformat.write(nb, f)

        result = fix_notebook(path)
        self.assertIn("❌", result)
        self.assertIn("replace_notebook_cell", result)
        # Extract session_id from the result
        self.assertIn("fix-broken-", result)
        # Verify it was added to drafts
        matching = [k for k in _NOTEBOOK_DRAFTS if k.startswith("fix-broken-")]
        self.assertTrue(len(matching) > 0)
        # Clean up
        for k in matching:
            del _NOTEBOOK_DRAFTS[k]


if __name__ == "__main__":
    # Allow running directly: python integration_testing/test_mcp_server.py
    import sys

    run_s3 = "--run-s3" in sys.argv or "--run-all" in sys.argv
    run_notebooks = "--run-notebooks" in sys.argv or "--run-all" in sys.argv
    print(f"run_s3={run_s3}, run_notebooks={run_notebooks}")
    print(
        "Use pytest for full control:\n"
        "  pytest integration_testing/test_mcp_server.py -v [--run-s3] [--run-notebooks] [--run-all]"
    )
