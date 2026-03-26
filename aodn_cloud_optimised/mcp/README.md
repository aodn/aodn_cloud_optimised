# AODN MCP Server

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server that
exposes the AODN (Australian Ocean Data Network) cloud-optimised dataset catalog
to AI assistants such as **GitHub Copilot CLI**, **Gemini CLI**, and
**Claude Desktop**.

The server enables an AI to discover datasets, understand their schemas, verify
real S3 data coverage, and generate — and validate — Jupyter notebooks for
oceanographic analysis.

---

## Installation

```bash
# From the repo root, install with the mcp extra:
pip install -e ".[mcp]"

# or with Poetry:
poetry install --extras mcp
```

This installs the `aodn-mcp-server` entry point and its dependencies
(`mcp>=1.0.0`, `nbformat`, `nbconvert`).

---

## Quick Start

### Configure environment variables

```bash
# Path to the notebooks directory (auto-detected if running from source)
export AODN_NOTEBOOKS_PATH=/path/to/aodn_cloud_optimised/notebooks

# Path to the dataset config directory (auto-detected if running from source)
export AODN_DATASET_CONFIG_PATH=/path/to/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset
```

### Start the server

```bash
aodn-mcp-server
```

The server communicates over **stdio** (default MCP transport), making it
compatible with any MCP-aware client.

---

## Connecting AI Clients

### GitHub Copilot CLI (Linux)

Create or edit `~/.copilot/mcp-config.json` (use `/mcp add` inside the CLI or
edit the file directly):

> **⚠️ Use the full absolute path to `aodn-mcp-server`.**  AI CLI tools spawn
> MCP servers without your shell's PATH or conda activation.  Find the path
> with `which aodn-mcp-server`.

```json
{
  "mcpServers": {
    "aodn": {
      "type": "stdio",
      "command": "/home/<your-user>/miniforge3/envs/<env>/bin/aodn-mcp-server",
      "args": [],
      "env": {
        "AODN_NOTEBOOKS_PATH": "/path/to/aodn_cloud_optimised/notebooks",
        "AODN_CONFIG_PATH": "/path/to/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
      },
      "tools": ["*"]
    }
  }
}
```

> **Note — Tool name prefixing:** Copilot CLI (v1.0.x) namespaces MCP tool
> names with the server key (`aodn`) and calls them as shell commands, e.g.
> `aodn-search_datasets "mooring temperature"`.  The package registers
> standalone executables for every tool so these calls succeed:
>
> ```bash
> aodn-search_datasets "wave buoy Tasmania"
> aodn-list_datasets --format parquet
> aodn-get_dataset_info argo.parquet
> aodn-get_dataset_schema satellite_ghrsst_l3s_1d_nrt
> aodn-check_dataset_coverage argo \
>     --lat-min -45 --lat-max -10 --lon-min 140 --lon-max 155
> aodn-introspect_dataset_live argo.parquet
> aodn-get_notebook_template argo.parquet
> aodn-get_plot_guide argo.parquet
> aodn-get_dataquery_reference
> ```
>
> All executables accept `--help` for a usage summary.

### Gemini CLI (Linux)

Add to `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "aodn": {
      "command": "/home/<your-user>/miniforge3/envs/<env>/bin/aodn-mcp-server",
      "args": [],
      "env": {
        "AODN_NOTEBOOKS_PATH": "/path/to/aodn_cloud_optimised/notebooks",
        "AODN_DATASET_CONFIG_PATH": "/path/to/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
      }
    }
  }
}
```

### Claude Desktop (Linux `~/.config/Claude/claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "aodn": {
      "command": "/home/<your-user>/miniforge3/envs/<env>/bin/aodn-mcp-server",
      "env": {
        "AODN_NOTEBOOKS_PATH": "/path/to/aodn_cloud_optimised/notebooks",
        "AODN_DATASET_CONFIG_PATH": "/path/to/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
      }
    }
  }
}
```

---

## Available Tools

| Tool | Description |
|------|-------------|
| `list_datasets` | List all datasets, optionally filtered by format or name prefix |
| `search_datasets` | Fuzzy keyword search across names, descriptions, and CF attributes — includes data type |
| `get_dataset_info` | Full metadata: schema variables with CF roles, S3 ARN, catalogue URL |
| `get_dataset_schema` | **Authoritative variable listing** — exact column names with roles, data type, AWS description, recommended code |
| `get_dataset_summary` | **Single-call dataset profile** — everything needed to use a dataset: type, description, variables, matching notebook, code pattern |
| `check_dataset_coverage` | Live S3 query — temporal extent, spatial extent, and overlap with requested bbox/period |
| `introspect_dataset_live` | Real variable list from the live S3 store (catches config/store divergence) |
| `validate_notebook` | Execute a notebook cell-by-cell via `nbconvert` and return a per-cell ✅/❌ report |
| `execute_python_cell` | Persistent Python REPL — test notebook cells before writing them |
| `start_notebook` | **Start building a validated notebook** — initialises draft with title + DataQuery setup |
| `add_notebook_cell` | **Add a validated cell** — code cells are executed first; broken cells are rejected |
| `save_notebook` | **Save and validate notebook** — writes `.ipynb`, then re-executes in a fresh kernel; fails if any cell errors |
| `replace_notebook_cell` | **Fix a cell in a draft** — replace by index after `save_notebook` reports failures |
| `fix_notebook` | **Rescue an existing broken notebook** — validates, imports into builder session if errors found |
| `get_dataset_config` | Full raw JSON config (parent/child merged via `load_dataset_config`) |
| `get_notebook_template` | Canonical `.ipynb` as readable text; falls back to a generic template |
| `get_plot_guide` | Ready-to-paste plotting snippets with real variable names |
| `get_dataquery_reference` | `DataQuery.py` public API reference (classes, signatures, docstrings) |

---

## Recommended AI Workflow

The server instructions guide the AI through this **validated builder** sequence:

1. **`search_datasets`** — find relevant datasets for the user's request.
2. **`get_dataset_summary`** — understand data type, variables, description, and code patterns.
3. **`check_dataset_coverage`** — confirm data actually exists in the requested region and time period.
4. **`start_notebook`** — initialise a validated notebook draft.
5. **`add_notebook_cell`** — add cells one by one; **code cells are executed before being committed** — broken cells are rejected.
6. **`save_notebook`** — write the notebook to disk, then **re-execute in a fresh Jupyter kernel**. If any cell fails, the draft stays open.
7. **`replace_notebook_cell`** — fix broken cells reported by `save_notebook`, then call `save_notebook` again.
8. **`fix_notebook`** — rescue an existing broken `.ipynb` by importing it into the builder for fixing.

`save_notebook` will not succeed until every cell passes full-kernel validation.
The builder makes it **impossible** to deliver a broken notebook.

---

## Architecture

```
aodn_cloud_optimised/mcp/
├── __init__.py
├── catalog.py          # Dataset catalog built from config/dataset/*.json
│                       #   Uses load_dataset_config() for parent/child merging
├── cli.py              # CLI entry points: aodn-search_datasets, aodn-list_datasets, …
│                       #   Allows Copilot CLI to call MCP tools as shell commands
├── notebook_utils.py   # Notebook discovery and .ipynb → readable text conversion
└── server.py           # FastMCP server — all 12 tools and 1 resource defined here
```

### Configuration and parent/child inheritance

Every dataset has a JSON config under `config/dataset/`. Some datasets
(e.g. all HF radar and satellite child configs) extend a `parent_config`.
The server uses `load_dataset_config()` from `lib/config.py` to resolve
parent/child configs with the correct semantics: **child schema replaces
parent schema** (not merges).

### DataQuery.py

`DataQuery.py` (`lib/DataQuery.py`) is a standalone script used inside
notebooks. The server loads it dynamically via `importlib.util` to avoid
import side-effects. Every `execute_python_cell` session is pre-populated
with `GetAodn` and `plot_ts_diagram`.

### No S3 calls at startup

The catalog is built entirely from local JSON files — fast, offline-capable,
no AWS credentials needed. Live S3 calls happen only when the AI explicitly
calls `check_dataset_coverage` or `introspect_dataset_live`.

---

## Critical Code Patterns the Server Enforces

The server instructions and `get_plot_guide` explicitly warn about these
common notebook bugs:

| Pattern | Wrong | Correct |
|---------|-------|---------|
| Month-end date | `calendar.monthrange(yr, m)[1] + 1` → day 31 overflow | `pd.Timestamp(year=yr, month=m, day=1) + pd.DateOffset(months=1)` |
| numpy datetime64 f-string | `f'{arr[0]:%Y-%m-%d}'` → `ValueError` | `pd.Timestamp(arr[0]).strftime('%Y-%m-%d')` |
| DataQuery functions | `ds.plot_ts_diagram(df)` → `AttributeError` | `plot_ts_diagram(df)` (standalone import) |
| xarray `.sel()` | `.sel(time=slice(...), lat=y, method='nearest')` → `NotImplementedError` | `.sel(time=slice(...)).sel(lat=y, method='nearest')` |
| pcolormesh | 3-D array passed directly → `TypeError` | `.isel(time=0).values` first |

---

## Testing

```bash
# Offline tests (fast, no S3):
pytest integration_testing/test_mcp_server.py -v

# Include live S3 queries:
pytest integration_testing/test_mcp_server.py -v --run-s3

# Include notebook execution (slow):
pytest integration_testing/test_mcp_server.py -v --run-notebooks

# Run everything:
pytest integration_testing/test_mcp_server.py -v --run-all
```

See [`docs/development/mcp-testing.rst`](../../docs/development/mcp-testing.rst)
for full details on the test suite.

---

## Further Documentation

Full Sphinx documentation is in [`docs/mcp_server.rst`](../../docs/mcp_server.rst)
and can be built with:

```bash
cd docs && make html
```
