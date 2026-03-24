"""
AODN Cloud Optimised MCP Server.

Exposes the AODN dataset catalog, schema information, and Jupyter notebook
templates to AI assistants via the Model Context Protocol (MCP).

Run via the installed entry point::

    aodn-mcp-server

Or directly::

    python -m aodn_cloud_optimised.mcp.server

Configure in Gemini CLI (``~/.gemini/settings.json``)::

    {
      "mcpServers": {
        "aodn": {
          "command": "aodn-mcp-server",
          "env": {
            "AODN_NOTEBOOKS_PATH": "/path/to/aodn_cloud_optimised/notebooks",
            "AODN_CONFIG_PATH": "/path/to/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
          },
          "trust": true
        }
      }
    }

Tools exposed:
    - list_datasets              — Browse all available datasets
    - search_datasets            — Fuzzy-search by topic / variable / keyword
    - get_dataset_info           — Formatted schema & metadata for a specific dataset
    - get_dataset_schema         — Authoritative variable listing with coordinate roles
    - get_dataset_config         — Full raw JSON config for a specific dataset
    - check_dataset_coverage     — Live S3 query for temporal/spatial/metadata coverage
    - get_notebook_template      — Notebook content as readable text
    - get_plot_guide             — Dataset-specific plotting code snippets
    - get_dataquery_reference    — DataQuery.py public API reference

Resources exposed:
    - catalog://datasets   — Machine-readable JSON catalog of all datasets
"""

from __future__ import annotations

import ast
import importlib.util
import json
import textwrap
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.mcp.catalog import (
    DatasetEntry,
    _find_config_dir,
    get_catalog,
)
from aodn_cloud_optimised.mcp.notebook_utils import (
    find_notebooks_dir,
    get_notebook_content,
    list_available_notebooks,
)

# ---------------------------------------------------------------------------
# DataQuery.py — lazy dynamic import
# ---------------------------------------------------------------------------


def _load_dataquery() -> Any:
    """
    Dynamically import DataQuery.py from the package's ``lib/`` directory.

    DataQuery.py is a standalone file (no internal package imports) used by
    all AODN notebooks.  We load it via ``importlib.util`` rather than a
    normal import so we can locate it relative to this file regardless of
    how the package is installed.

    Returns:
        The DataQuery module object.

    Raises:
        ImportError: If DataQuery.py cannot be found or loaded.
    """
    dq_path = Path(__file__).parents[1] / "lib" / "DataQuery.py"
    if not dq_path.is_file():
        raise ImportError(f"DataQuery.py not found at {dq_path}")
    spec = importlib.util.spec_from_file_location("DataQuery", dq_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec for {dq_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


mcp = FastMCP(
    name="aodn-cloud-optimised",
    instructions=textwrap.dedent(
        """\
        You have access to the AODN (Australian Ocean Data Network) Cloud Optimised
        dataset catalog. Use the tools below in this recommended order:

        1. **Discover** datasets relevant to a user's request with `search_datasets`.
        2. **Verify actual coverage** with `check_dataset_coverage`.
        3. **Get real variable names** — call BOTH `get_dataset_schema` AND
           `introspect_dataset_live` (especially critical for Zarr).
        4. **Retrieve** notebook template with `get_notebook_template`.
        5. **Get plotting code** with `get_plot_guide`.
        6. **TEST EVERY CELL** with `execute_python_cell` before writing it to the
           notebook.  Use a consistent `session_id` to share state across cells.
        7. Write the notebook file only after all cells pass `execute_python_cell`.
        8. **Validate the finished notebook** with `validate_notebook`.  Fix every
           ❌ cell and re-validate until clean.  **Never deliver a notebook that
           has not passed validation.**

        Datasets come in two formats:
        - **Parquet** — tabular time series (point/mooring/vessel/glider data)
        - **Zarr** — gridded rasters (satellite imagery, radar, model output)

        ## CRITICAL: Variable names

        **NEVER invent variable names.** Always call `introspect_dataset_live` first.
        Known surprises:
        - Argo: time is `JULD` not `TIME`; the column `TIME` does not exist
        - GHRSST SST: `wind_speed` is in the JSON config but NOT in the Zarr store
        - Radar zarr coords are `TIME`, `LATITUDE`, `LONGITUDE` (uppercase)
        - Satellite zarr coords are `time`, `lat`, `lon` (lowercase)

        ## CRITICAL: xarray pitfalls

        **xarray `.sel()` with slice + method='nearest' raises NotImplementedError.**
        You cannot combine a range slice and a nearest-neighbour lookup in one call.
        ALWAYS split into two chained `.sel()` calls:

        ```python
        # WRONG — raises NotImplementedError:
        ds.sel(time=slice(t0, t1), lat=y, lon=x, method='nearest')

        # CORRECT — slice first, then nearest:
        ds.sel(time=slice(t0, t1)).sel(lat=y, lon=x, method='nearest')
        ```

        ## CRITICAL: pandas pitfalls

        **Never rename columns to a name that already exists in the DataFrame.**
        If `df` has both `TEMP` and `TEMP_ADJUSTED`, renaming `TEMP_ADJUSTED→TEMP`
        creates duplicate columns, which causes `ValueError` in many pandas operations
        including `plot_ts_diagram`.  Instead, pass the original column names directly:

        ```python
        # WRONG — creates duplicate 'TEMP' column:
        df.rename(columns={'TEMP_ADJUSTED': 'TEMP', ...})

        # CORRECT — pass actual column names:
        plot_ts_diagram(df, temp_col='TEMP_ADJUSTED', psal_col='PSAL_ADJUSTED', z_col='PRES_ADJUSTED')
        ```

        ## CRITICAL: pcolormesh / cartopy pitfalls

        When plotting a gridded field with `ax.pcolormesh(lon, lat, data)`:
        - `data` must be 2-D (shape `[nlat, nlon]`).  Call `.mean(dim='time')` or
          index a single time step first.
        - If lat is in **descending** order (common in satellite data), reverse it:
          `data = data[::-1, :]` and `lat = lat[::-1]`.
        - Use `shading='auto'` to avoid the "C one smaller than X/Y" TypeError.

        ## CRITICAL: Date arithmetic — never overflow the day-of-month

        **Never construct `np.datetime64(f'{yr}-{m:02d}-{last_day+1:02d}')` as an
        exclusive upper bound** — `last_day+1` is 31 for April/June/September/November
        and will raise `ValueError: Day out of range`.

        Always use the safe month-end helper pattern instead:

        ```python
        # WRONG — raises ValueError for months with 30 days:
        t1 = np.datetime64(f'{yr}-{ANALYSIS_MONTH:02d}-{calendar.monthrange(yr,ANALYSIS_MONTH)[1]+1:02d}')

        # CORRECT — step to the first day of the next month:
        def _next_month_start(yr, m):
            ts = pd.Timestamp(year=yr, month=m, day=1) + pd.DateOffset(months=1)
            return np.datetime64(ts.strftime('%Y-%m-%d'))
        t1 = _next_month_start(yr, ANALYSIS_MONTH)
        ```

        Add `_next_month_start` in the parameters cell at the top of every notebook
        that loops over months.  Use it everywhere you need an exclusive month-end.

        ## CRITICAL: numpy datetime64 formatting in f-strings

        **Never write `f'{time_array[0]:%Y-%m-%d}'`** — the `%Y-%m-%d` format
        spec fails with `ValueError: Invalid format specifier` when the value is a
        `numpy.datetime64` (which is backed by a string internally).

        Always convert through `pd.Timestamp` first:

        ```python
        # WRONG — raises ValueError for numpy.datetime64:
        print(f"Range: {time_array[0]:%Y-%m-%d} → {time_array[-1]:%Y-%m-%d}")

        # CORRECT:
        print(f"Range: {pd.Timestamp(time_array[0]).strftime('%Y-%m-%d')} → "
              f"{pd.Timestamp(time_array[-1]).strftime('%Y-%m-%d')}")
        ```

        ## CRITICAL: DataQuery standalone functions vs class methods

        Several functions in DataQuery.py are **module-level functions**, not
        methods of any dataset class.  Calling them on an object raises
        `AttributeError`.

        ```python
        # WRONG — raises AttributeError:
        ds_argo.plot_ts_diagram(df)
        ds.plot_timeseries(df)

        # CORRECT — import and call directly:
        from DataQuery import GetAodn, plot_ts_diagram
        plot_ts_diagram(df, temp_col='TEMP', psal_col='PSAL', z_col='DEPTH')
        ```

        The following are standalone functions (not methods):
        `plot_ts_diagram`, `plot_timeseries`, `plot_surface_map`,
        `plot_gridded_variable`, `plot_scatter`.

        ## CRITICAL: Default output format

        **When a user asks for data analysis, exploration, or visualisation
        without specifying a format, always produce a Jupyter notebook** (`.ipynb`).
        Never return only a code snippet or markdown explanation when a notebook
        would be more useful.  If the user is continuing a previous analysis,
        add sections to the existing notebook rather than creating a new one.

        ## CRITICAL: Notebook initialisation

        Every notebook **must** contain an initialisation cell:
        ```python
        from DataQuery import GetAodn, plot_ts_diagram
        aodn = GetAodn()
        ```
        Without `aodn = GetAodn()` every `aodn.get_dataset(…)` call raises NameError.

        ## Jupyter magic rules

        - `%%time` must be the **sole content of the first line** of the cell.
        - `%time expr` times a **single-line** expression only; never multi-line.
        - Never write any code after `%%time` on the same line.
        """
    ),
)


# ---------------------------------------------------------------------------
# Helper formatters
# ---------------------------------------------------------------------------


def _format_entry_detail(entry: DatasetEntry) -> str:
    """Render a DatasetEntry as a detailed markdown string."""
    lines: list[str] = [
        f"# {entry.dataset_name}",
        "",
        f"**Format:** {entry.cloud_optimised_format.upper()}",
        f"**Title:** {entry.title or '(none)'}",
        f"**S3 ARN:** {entry.s3_arn or '(not available)'}",
        f"**Catalogue URL:** {entry.catalogue_url or '(none)'}",
        "",
        "## Description",
        entry.description or "(no description)",
        "",
        "## Schema Variables",
        "",
    ]

    if entry.variables:
        lines.append("| Variable | Type | standard_name | long_name | units | axis |")
        lines.append("|---|---|---|---|---|---|")
        for v in entry.variables:
            lines.append(
                f"| {v.name} | {v.type} | {v.standard_name} | {v.long_name} | {v.units} | {v.axis} |"
            )
    else:
        lines.append("(no schema information available)")

    # Partitioning info
    raw = entry._raw
    partitioning = (raw.get("schema_transformation", {}) or {}).get("partitioning", [])
    if partitioning:
        lines += ["", "## Partitioning", ""]
        for p in partitioning:
            src = p.get("source_variable", "")
            ptype = p.get("type", "column")
            detail = ""
            if ptype == "time_extent":
                te = p.get("time_extent", {})
                detail = f"time variable=`{te.get('time_varname', '')}`, period=`{te.get('partition_period', '')}`"
            elif ptype == "spatial_extent":
                se = p.get("spatial_extent", {})
                detail = f"lat=`{se.get('lat_varname', '')}`, lon=`{se.get('lon_varname', '')}`, resolution={se.get('spatial_resolution', '')}°"
            lines.append(f"- **{src}** ({ptype}) {detail}")

    return "\n".join(lines)


def _format_search_result(entry: DatasetEntry, score: float, rank: int) -> str:
    """Render a single search result as a compact markdown block."""
    variables_preview = ", ".join(v.name for v in entry.variables[:8] if v.name)
    if len(entry.variables) > 8:
        variables_preview += f", … (+{len(entry.variables) - 8} more)"

    desc_preview = textwrap.shorten(entry.description or entry.title, width=200)

    return "\n".join(
        [
            f"### {rank}. `{entry.dataset_name}` [{entry.cloud_optimised_format.upper()}]",
            f"**Score:** {score:.2f}",
            f"**Title:** {entry.title or '(none)'}",
            f"**Description:** {desc_preview}",
            f"**Key variables:** {variables_preview or '(none listed)'}",
            f"**Catalogue:** {entry.catalogue_url or '(none)'}",
        ]
    )


# ---------------------------------------------------------------------------
# DataQuery.py reference — parsed once at startup
# ---------------------------------------------------------------------------


def _extract_dataquery_reference() -> str:
    """
    Parse DataQuery.py with the ``ast`` module and return a concise API reference
    (class and function signatures + docstrings). No code is executed.
    """
    dq_path = Path(__file__).parents[1] / "lib" / "DataQuery.py"
    if not dq_path.is_file():
        return "(DataQuery.py not found — ensure the package is installed correctly)"

    try:
        source = dq_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except Exception as exc:
        return f"(Failed to parse DataQuery.py: {exc})"

    sections: list[str] = [
        "# DataQuery.py — Public API Reference",
        "",
        f"Version: extracted from `{dq_path.name}`",
        "",
    ]

    # Collect all function nodes that are direct children of a class (method nodes)
    method_nodes: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    method_nodes.add(id(item))

    # First pass: classes with their public methods
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            if node.name.startswith("_"):
                continue
            docstring = ast.get_docstring(node) or ""
            sections.append(f"## class `{node.name}`")
            if docstring:
                sections.append(textwrap.indent(docstring.strip(), "    "))
            sections.append("")

            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if item.name.startswith("_"):
                        continue
                    sig = _get_function_signature(item)
                    fdoc = ast.get_docstring(item) or ""
                    sections.append(f"### `{item.name}({sig})`")
                    if fdoc:
                        first_para = fdoc.strip().split("\n\n")[0]
                        sections.append(textwrap.indent(first_para, "    "))
                    sections.append("")

    # Second pass: top-level public functions (not inside any class)
    sections.append("## Top-Level Functions")
    sections.append("")
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("_"):
                continue
            sig = _get_function_signature(node)
            fdoc = ast.get_docstring(node) or ""
            sections.append(f"### `{node.name}({sig})`")
            if fdoc:
                first_para = fdoc.strip().split("\n\n")[0]
                sections.append(textwrap.indent(first_para, "    "))
            sections.append("")

    return "\n".join(sections)


def _get_function_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """Return a compact signature string for a function AST node."""
    args = node.args
    parts: list[str] = []

    # positional args (with defaults right-aligned)
    defaults_offset = len(args.args) - len(args.defaults)
    for i, arg in enumerate(args.args):
        annotation = ""
        if arg.annotation:
            try:
                annotation = f": {ast.unparse(arg.annotation)}"
            except Exception:
                pass
        default = ""
        default_idx = i - defaults_offset
        if default_idx >= 0:
            try:
                default = f" = {ast.unparse(args.defaults[default_idx])}"
            except Exception:
                pass
        parts.append(f"{arg.arg}{annotation}{default}")

    if args.vararg:
        parts.append(f"*{args.vararg.arg}")
    for arg in args.kwonlyargs:
        parts.append(arg.arg)
    if args.kwarg:
        parts.append(f"**{args.kwarg.arg}")

    return ", ".join(parts)


# Build the reference once at module load
_DATAQUERY_REFERENCE: str = _extract_dataquery_reference()

# Persistent Python session namespaces for execute_python_cell
_PYTHON_SESSIONS: dict[str, dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# MCP Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_datasets(
    format_filter: str = "",
    prefix: str = "",
) -> str:
    """
    List all available AODN cloud-optimised datasets.

    Args:
        format_filter: Optional filter — "parquet" or "zarr". Leave empty for all.
        prefix: Optional dataset name prefix (e.g. "mooring", "satellite", "vessel").

    Returns:
        A formatted list of datasets with their format and a brief description.
    """
    catalog = get_catalog()
    entries = catalog.list_all(
        format_filter=format_filter or None,
        prefix=prefix or None,
    )

    if not entries:
        return "No datasets found matching the given filters."

    lines = [
        f"## AODN Dataset Catalog ({len(entries)} datasets)",
        "",
    ]
    for e in entries:
        lines.append(f"- **{e.dataset_name}** [{e.cloud_optimised_format.upper()}]")
        if e.title:
            lines.append(f"  {e.title}")
        elif e.description:
            lines.append(f"  {textwrap.shorten(e.description, width=120)}")

    nb_dir = find_notebooks_dir()
    nb_note = (
        f"\n\nNotebooks directory: `{nb_dir}`"
        if nb_dir
        else "\n\n⚠ Notebooks directory not found. Set `AODN_NOTEBOOKS_PATH`."
    )
    lines.append(nb_note)

    try:
        cfg_dir = _find_config_dir()
        lines.append(f"Config directory:    `{cfg_dir}`")
    except FileNotFoundError:
        lines.append("⚠ Config directory not found. Set `AODN_CONFIG_PATH`.")

    return "\n".join(lines)


@mcp.tool()
def search_datasets(
    query: str,
    top_k: int = 5,
) -> str:
    """
    Search the AODN dataset catalog using a natural-language or keyword query.

    Searches across dataset names, descriptions, and CF variable attributes
    (standard_name, long_name). Uses fuzzy token matching so approximate terms work.

    Args:
        query: Search terms, e.g. "mooring temperature" or "sea surface temperature
               satellite" or "chlorophyll southern ocean".
        top_k: Maximum number of results to return (default 5).

    Returns:
        Ranked list of matching datasets with key metadata.
    """
    catalog = get_catalog()
    results = catalog.search(query, top_k=max(1, top_k))

    if not results:
        return (
            f"No datasets found matching '{query}'. "
            "Try broader terms or use `list_datasets` to browse the full catalog."
        )

    lines = [
        f"## Search results for '{query}' (top {len(results)})",
        "",
    ]
    for rank, (entry, score) in enumerate(results, start=1):
        lines.append(_format_search_result(entry, score, rank))
        lines.append("")

    lines.append(
        "---\nUse `get_dataset_schema` for the authoritative variable list, "
        "`get_dataset_info` for full metadata, "
        "or `get_notebook_template` to retrieve the matching notebook."
    )
    return "\n".join(lines)


@mcp.tool()
def get_dataset_info(dataset_name: str) -> str:
    """
    Retrieve full metadata and schema for a specific AODN dataset.

    Returns the dataset description, S3 location (ARN), all schema variables with
    their CF attributes (standard_name, long_name, units, type), partitioning
    strategy, and a link to the AODN metadata catalogue.

    Args:
        dataset_name: The dataset identifier, e.g. "mooring_temperature_logger_delayed_qc".
                      Optionally include the format extension: "argo.parquet".

    Returns:
        Detailed dataset information in markdown format.
    """
    catalog = get_catalog()
    entry = catalog.get(dataset_name)
    if entry is None:
        close = catalog.search(dataset_name, top_k=3)
        suggestions = ", ".join(f"`{e.dataset_name}`" for e, _ in close)
        return (
            f"Dataset `{dataset_name}` not found in the catalog.\n\n"
            f"Did you mean: {suggestions}?\n\n"
            "Use `list_datasets` or `search_datasets` to discover available datasets."
        )
    return _format_entry_detail(entry)


@mcp.tool()
def get_dataset_schema(dataset_name: str) -> str:
    """
    Return the authoritative list of variables (column names) for a specific
    AODN dataset, with their roles (time axis, lat/lon, data variables) clearly
    identified from the CF attributes in the config schema.

    **Always call this tool before generating any notebook code that references
    variable names.** Variable names differ between datasets (e.g. the time
    column may be "TIME", "JULD", "detection_timestamp" etc.) — never assume.

    Args:
        dataset_name: The dataset identifier, e.g. "argo" or "argo.parquet".

    Returns:
        A structured markdown listing of every schema variable, with each
        variable's role (TIME_AXIS / LAT / LON / DEPTH / DATA), type, units,
        long_name, and standard_name.  Coordinate variables are highlighted at
        the top so the correct names are immediately visible.
    """
    import re

    stem = re.sub(r"\.(parquet|zarr)$", "", dataset_name)
    catalog = get_catalog()
    entry = catalog.get(stem)

    if entry is None:
        close = catalog.search(stem, top_k=3)
        suggestions = ", ".join(f"`{e.dataset_name}`" for e, _ in close)
        return (
            f"Dataset `{stem}` not found.\n\nDid you mean: {suggestions}?\n\n"
            "Use `search_datasets` to discover available datasets."
        )

    raw_schema: dict[str, Any] = entry._raw.get("schema", {}) or {}

    # Classify every variable by its CF role
    roles: dict[str, str] = {}
    for vname, attrs in raw_schema.items():
        if not isinstance(attrs, dict):
            continue
        axis = (attrs.get("axis") or "").upper()
        sname = (attrs.get("standard_name") or "").lower()
        lname = (attrs.get("long_name") or "").lower()
        vlow = vname.lower()

        if (
            axis == "T"
            or sname == "time"
            or vlow in ("time", "juld", "juld_location", "detection_timestamp")
        ):
            roles[vname] = "TIME_AXIS"
        elif axis == "Y" or sname in ("latitude",) or vlow in ("latitude", "lat"):
            roles[vname] = "LAT"
        elif axis == "X" or sname in ("longitude",) or vlow in ("longitude", "lon"):
            roles[vname] = "LON"
        elif (
            axis == "Z"
            or sname in ("depth", "pressure")
            or vlow in ("depth", "nominal_depth", "pres", "pres_adjusted")
        ):
            roles[vname] = "DEPTH"
        else:
            roles[vname] = "DATA"

    # Sort: coordinates first, then data variables alphabetically
    role_order = {"TIME_AXIS": 0, "LAT": 1, "LON": 2, "DEPTH": 3, "DATA": 4}
    sorted_vars = sorted(
        raw_schema.keys(), key=lambda v: (role_order.get(roles.get(v, "DATA"), 4), v)
    )

    lines: list[str] = [
        f"# Schema variables: `{stem}`",
        "",
        f"**Format:** {entry.cloud_optimised_format.upper()}",
        f"**Total variables:** {len(raw_schema)}",
        "",
        "## Coordinate / axis variables",
        "",
        "These are the **exact column names** to use in code for time, position, and depth.",
        "",
        "| Role | Variable name | Type | Units | standard_name | long_name |",
        "|---|---|---|---|---|---|",
    ]

    coord_found = False
    for vname in sorted_vars:
        role = roles.get(vname, "DATA")
        if role == "DATA":
            continue
        coord_found = True
        attrs = raw_schema.get(vname, {})
        lines.append(
            f"| **{role}** | `{vname}` | {attrs.get('type', '')} "
            f"| {attrs.get('units', '')} "
            f"| {attrs.get('standard_name', '')} "
            f"| {attrs.get('long_name', '')} |"
        )

    if not coord_found:
        lines.append("| (none identified) | | | | | |")

    lines += [
        "",
        "## All data variables",
        "",
        "| Variable name | Type | Units | standard_name | long_name |",
        "|---|---|---|---|---|",
    ]

    for vname in sorted_vars:
        role = roles.get(vname, "DATA")
        if role != "DATA":
            continue
        attrs = raw_schema.get(vname, {})
        lines.append(
            f"| `{vname}` | {attrs.get('type', '')} "
            f"| {attrs.get('units', '')} "
            f"| {attrs.get('standard_name', '')} "
            f"| {attrs.get('long_name', '')} |"
        )

    # Also expose partitioning-derived coordinate names as a sanity check
    partitioning = (entry._raw.get("schema_transformation", {}) or {}).get(
        "partitioning", []
    )
    time_varnames = [
        p.get("time_extent", {}).get("time_varname", "")
        for p in partitioning
        if p.get("type") == "time_extent"
    ]
    lat_varnames = [
        p.get("spatial_extent", {}).get("lat_varname", "")
        for p in partitioning
        if p.get("type") == "spatial_extent"
    ]
    lon_varnames = [
        p.get("spatial_extent", {}).get("lon_varname", "")
        for p in partitioning
        if p.get("type") == "spatial_extent"
    ]

    if any(time_varnames + lat_varnames + lon_varnames):
        lines += [
            "",
            "## Partitioning coordinates (used for S3 partition pruning)",
            "",
        ]
        if time_varnames:
            lines.append(
                f"- **Time partition variable:** `{'`, `'.join(t for t in time_varnames if t)}`"
            )
        if lat_varnames:
            lines.append(
                f"- **Latitude partition variable:** `{'`, `'.join(t for t in lat_varnames if t)}`"
            )
        if lon_varnames:
            lines.append(
                f"- **Longitude partition variable:** `{'`, `'.join(t for t in lon_varnames if t)}`"
            )

    lines += [
        "",
        "> **IMPORTANT:** Use only the variable names listed above in generated code.",
        "> Do not invent or assume variable names (e.g. do not assume `TIME` exists — "
        "check the TIME_AXIS row above for the actual name).",
    ]

    return "\n".join(lines)


@mcp.tool()
def check_dataset_coverage(
    dataset_name: str,
    lat_min: float | None = None,
    lat_max: float | None = None,
    lon_min: float | None = None,
    lon_max: float | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
) -> str:
    """
    Query the **actual data coverage** of an AODN dataset directly from S3.

    Makes live S3 requests (anonymous, no credentials required) to determine:
    - The real temporal extent (first and last data timestamp)
    - The real spatial extent (bounding box or polygon set)
    - Key global attributes from the dataset's metadata file

    If any of ``lat_min/lat_max/lon_min/lon_max/date_start/date_end`` are
    provided, also checks whether the dataset's coverage **overlaps** with
    that region/period and reports YES / NO / PARTIAL.

    **Always call this tool before recommending a dataset** when the user
    has specified a geographic area or time period.  A dataset can be
    thematically relevant but contain no data in the requested area or era.

    Args:
        dataset_name: Dataset identifier, e.g. "argo" or "argo.parquet".
        lat_min: Southern bound of the area of interest (degrees, −90..90).
        lat_max: Northern bound of the area of interest (degrees, −90..90).
        lon_min: Western bound of the area of interest (degrees, −180..360).
        lon_max: Eastern bound of the area of interest (degrees, −180..360).
        date_start: Start of the period of interest (ISO-8601, e.g. "2020-01-01").
        date_end:   End of the period of interest (ISO-8601, e.g. "2023-12-31").

    Returns:
        Markdown report with temporal extent, spatial bounding box, key
        metadata attributes, and (when filters are given) overlap verdict.
    """
    import re
    import traceback

    stem = re.sub(r"\.(parquet|zarr)$", "", dataset_name)
    catalog = get_catalog()
    entry = catalog.get(stem)

    if entry is None:
        close = catalog.search(stem, top_k=3)
        suggestions = ", ".join(f"`{e.dataset_name}`" for e, _ in close)
        return f"Dataset `{stem}` not found in catalog.\n\nDid you mean: {suggestions}?"

    fmt = entry.cloud_optimised_format
    dname = f"{stem}.{fmt}"

    lines: list[str] = [
        f"# Data coverage: `{stem}`",
        "",
        f"**Format:** {fmt.upper()}",
        "",
    ]

    try:
        dq = _load_dataquery()
    except ImportError as exc:
        return f"Cannot load DataQuery.py: {exc}"

    try:
        aodn = dq.GetAodn()
        dataset_obj = aodn.get_dataset(dname)
    except Exception as exc:
        return (
            f"# Data coverage: `{stem}`\n\n"
            f"**Error connecting to S3:** {exc}\n\n"
            "This may be a network issue or the dataset may not yet be published."
        )

    # ------------------------------------------------------------------
    # Temporal extent
    # ------------------------------------------------------------------
    t_min = t_max = None
    try:
        if fmt == "zarr":
            t_min, t_max = dataset_obj.get_temporal_extent()
        else:
            # Fast method first (reads partition keys only)
            try:
                t_min, t_max = (
                    dataset_obj.get_temporal_extent_from_timestamp_partition()
                )
            except Exception:
                t_min, t_max = dataset_obj.get_temporal_extent()
        lines += [
            "## Temporal extent",
            "",
            f"| | Value |",
            "|---|---|",
            f"| **First data** | {t_min} |",
            f"| **Last data**  | {t_max} |",
            "",
        ]
    except Exception as exc:
        lines += [
            "## Temporal extent",
            "",
            f"*Could not retrieve temporal extent: {exc}*",
            "",
        ]

    # ------------------------------------------------------------------
    # Spatial extent
    # ------------------------------------------------------------------
    bbox: tuple[float, float, float, float] | None = (
        None  # (min_lat, min_lon, max_lat, max_lon)
    )
    try:
        spatial = dataset_obj.get_spatial_extent()
        if fmt == "zarr":
            # Returns [min_lat, min_lon, max_lat, max_lon]
            bbox = (spatial[0], spatial[1], spatial[2], spatial[3])
        else:
            # Returns a Shapely MultiPolygon — extract bounds (minx, miny, maxx, maxy)
            b = spatial.bounds  # (minx=lon, miny=lat, maxx=lon, maxy=lat)
            bbox = (b[1], b[0], b[3], b[2])  # → (min_lat, min_lon, max_lat, max_lon)
        lines += [
            "## Spatial extent",
            "",
            "| | Value |",
            "|---|---|",
            f"| **Latitude range**  | {bbox[0]:.3f}° → {bbox[2]:.3f}° |",
            f"| **Longitude range** | {bbox[1]:.3f}° → {bbox[3]:.3f}° |",
            "",
        ]
    except Exception as exc:
        lines += [
            "## Spatial extent",
            "",
            f"*Could not retrieve spatial extent: {exc}*",
            "",
        ]

    # ------------------------------------------------------------------
    # Metadata (title, institution, summary, licence)
    # ------------------------------------------------------------------
    try:
        meta = dataset_obj.get_metadata()
        useful_ga_keys = [
            "title",
            "institution",
            "summary",
            "license",
            "licence",
            "geospatial_lat_min",
            "geospatial_lat_max",
            "geospatial_lon_min",
            "geospatial_lon_max",
            "time_coverage_start",
            "time_coverage_end",
            "acknowledgement",
            "project",
        ]
        ga = meta.get(
            "global_attributes", meta
        )  # parquet nests under global_attributes
        found = {k: ga[k] for k in useful_ga_keys if k in ga and ga[k]}
        if found:
            lines += ["## Key metadata attributes", ""]
            for k, v in found.items():
                v_str = str(v)[:300]
                lines.append(f"- **{k}:** {v_str}")
            lines.append("")
    except Exception:
        pass  # Metadata is optional; failures are non-fatal

    # ------------------------------------------------------------------
    # Overlap check (when user supplied filters)
    # ------------------------------------------------------------------
    has_spatial_filter = any(
        v is not None for v in [lat_min, lat_max, lon_min, lon_max]
    )
    has_time_filter = date_start is not None or date_end is not None

    if has_spatial_filter or has_time_filter:
        lines += ["## Overlap with requested filters", ""]
        verdicts: list[str] = []

        if has_time_filter and t_min is not None and t_max is not None:
            import pandas as pd

            req_start = (
                pd.Timestamp(date_start).tz_localize("UTC") if date_start else None
            )
            req_end = pd.Timestamp(date_end).tz_localize("UTC") if date_end else None
            # Normalise dataset timestamps to UTC (they may already be tz-aware)
            ds_start = pd.Timestamp(t_min)
            ds_end = pd.Timestamp(t_max)
            if ds_start.tzinfo is None:
                ds_start = ds_start.tz_localize("UTC")
            if ds_end.tzinfo is None:
                ds_end = ds_end.tz_localize("UTC")
            if req_start and req_end:
                overlaps = req_start <= ds_end and req_end >= ds_start
            elif req_start:
                overlaps = req_start <= ds_end
            else:
                overlaps = req_end >= ds_start  # type: ignore[operator]

            if overlaps:
                verdicts.append(
                    f"✅ **Time:** requested `{date_start or '…'}` → `{date_end or '…'}` "
                    f"overlaps dataset range `{ds_start.date()}` → `{ds_end.date()}`"
                )
            else:
                verdicts.append(
                    f"❌ **Time:** requested `{date_start or '…'}` → `{date_end or '…'}` "
                    f"does NOT overlap dataset range `{ds_start.date()}` → `{ds_end.date()}`"
                )

        if has_spatial_filter and bbox is not None:
            ds_lat_min, ds_lon_min, ds_lat_max, ds_lon_max = bbox
            req_lat_min = lat_min if lat_min is not None else -90.0
            req_lat_max = lat_max if lat_max is not None else 90.0
            req_lon_min = lon_min if lon_min is not None else -180.0
            req_lon_max = lon_max if lon_max is not None else 360.0
            lat_ok = req_lat_min <= ds_lat_max and req_lat_max >= ds_lat_min
            lon_ok = req_lon_min <= ds_lon_max and req_lon_max >= ds_lon_min
            if lat_ok and lon_ok:
                verdicts.append(
                    f"✅ **Space:** requested bbox "
                    f"[{req_lat_min}°, {req_lon_min}°, {req_lat_max}°, {req_lon_max}°] "
                    f"overlaps dataset extent"
                )
            elif not lat_ok:
                verdicts.append(
                    f"❌ **Space:** requested latitude range "
                    f"[{req_lat_min}°, {req_lat_max}°] does NOT overlap "
                    f"dataset latitude range [{ds_lat_min:.2f}°, {ds_lat_max:.2f}°]"
                )
            else:
                verdicts.append(
                    f"❌ **Space:** requested longitude range "
                    f"[{req_lon_min}°, {req_lon_max}°] does NOT overlap "
                    f"dataset longitude range [{ds_lon_min:.2f}°, {ds_lon_max:.2f}°]"
                )

        for v in verdicts:
            lines.append(v)
        lines.append("")

    return "\n".join(lines)


@mcp.tool()
def introspect_dataset_live(dataset_name: str) -> str:
    """
    Open the actual S3 dataset and return the **real** variable names, dimensions,
    shapes, and dtypes.

    Unlike ``get_dataset_schema`` (which reads the JSON config file), this tool
    opens the live dataset on S3 and inspects it directly.  The JSON config can
    list variables that are absent from the actual store — e.g. the GHRSST
    satellite dataset lists ``wind_speed`` in its config but that variable is NOT
    present in the Zarr store.

    - **Zarr:** opens the xarray Dataset, lists every ``data_var`` with its
      dimensions, shape, dtype, units, long_name, and standard_name.
    - **Parquet:** reads the embedded pyarrow schema (``_common_metadata``) — fast,
      no row data downloaded.  Lists every column with its dtype.

    **Call this before writing code that accesses individual variables** (e.g.
    ``ds['var_name']``, ``get_timeseries_data(var_name='…')``).  For Zarr datasets
    this is especially important because the JSON schema and the store may diverge.

    Args:
        dataset_name: Dataset identifier, e.g. "argo" or
                      "satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_australia".

    Returns:
        Markdown table of real variable names with their dims / shape / dtype /
        units; plus a note on any variables present in the JSON schema but absent
        from the live store.
    """
    import re

    stem = re.sub(r"\.(parquet|zarr)$", "", dataset_name)
    catalog = get_catalog()
    entry = catalog.get(stem)

    if entry is None:
        close = catalog.search(stem, top_k=3)
        suggestions = ", ".join(f"`{e.dataset_name}`" for e, _ in close)
        return f"Dataset `{stem}` not found.\n\nDid you mean: {suggestions}?"

    fmt = entry.cloud_optimised_format
    dname = f"{stem}.{fmt}"

    try:
        dq = _load_dataquery()
    except ImportError as exc:
        return f"Cannot load DataQuery.py: {exc}"

    try:
        aodn = dq.GetAodn()
        ds_obj = aodn.get_dataset(dname)
        info = ds_obj.describe()
    except Exception as exc:
        return (
            f"# Live introspection: `{stem}`\n\n"
            f"**Error connecting to S3:** {exc}\n\n"
            "Falling back to JSON schema — treat variable names with caution."
        )

    lines: list[str] = [
        f"# Live introspection: `{stem}`",
        "",
        f"**Format:** {fmt.upper()}",
        f"**S3 path:** `{info.get('dataset_name', dname)}`",
        "",
    ]

    if fmt == "zarr":
        data_vars: dict = info.get("data_vars", {})
        coords: dict = info.get("coords", {})

        lines += [
            "## Coordinates (dimensions)",
            "",
            "| Name | dims | shape | dtype | units |",
            "|---|---|---|---|---|",
        ]
        for name, meta in coords.items():
            lines.append(
                f"| `{name}` | {meta['dims']} | {meta['shape']} "
                f"| {meta['dtype']} | {meta.get('units', '')} |"
            )

        lines += [
            "",
            f"## Data variables ({len(data_vars)} total)",
            "",
            "| Name | dims | shape | dtype | units | long_name |",
            "|---|---|---|---|---|---|",
        ]
        for name, meta in data_vars.items():
            lname = meta.get("long_name", "")[:60]
            lines.append(
                f"| `{name}` | {meta['dims']} | {meta['shape']} "
                f"| {meta['dtype']} | {meta.get('units', '')} | {lname} |"
            )

        # Cross-check: JSON schema vars not in live store
        schema_names = {v.name for v in entry.variables}
        live_names = set(data_vars) | set(coords)
        missing = schema_names - live_names
        extra = set(data_vars) - schema_names - set(coords)
        if missing:
            lines += [
                "",
                "## ⚠️ Variables in JSON schema but NOT in the live Zarr store",
                "",
                "> These names must NOT be used in generated code.",
                "",
            ]
            for m in sorted(missing):
                lines.append(f"- `{m}`")
        if extra:
            lines += [
                "",
                "## ℹ️ Variables in the live store but NOT in JSON schema",
                "",
            ]
            for e_ in sorted(extra):
                lines.append(f"- `{e_}`")

        if info.get("global_attrs"):
            lines += ["", "## Key global attributes", ""]
            for k, v in list(info["global_attrs"].items())[:6]:
                lines.append(f"- **{k}:** {v}")

    else:  # parquet
        columns: dict = info.get("columns", {})
        lines += [
            f"## Columns ({len(columns)} total — from pyarrow embedded schema)",
            "",
            "| Column name | dtype |",
            "|---|---|",
        ]
        for name, meta in columns.items():
            lines.append(f"| `{name}` | {meta['dtype']} |")

    lines += [
        "",
        "> **Use only the names listed above in generated code.**",
        "> JSON config schema and live store may differ — always prefer this output.",
    ]
    return "\n".join(lines)


@mcp.tool()
def validate_notebook(
    notebook_path: str,
    cell_timeout: int = 120,
    stop_on_error: bool = False,
) -> str:
    """
    Execute a Jupyter notebook cell by cell and return a per-cell pass/fail report.

    Uses ``nbconvert``'s ``ExecutePreprocessor`` to run each code cell in the
    current Python kernel.  Markdown cells are skipped.  The report includes:
    - ✅ / ❌ / ⏱️ status for every code cell
    - Full error traceback for failed cells
    - Stdout/stderr snippets for passed cells (truncated to avoid flooding)

    Use this after generating a notebook to verify it executes correctly.
    Iterate: fix the errors the report highlights, then call again.

    **Note on long-running cells:** data-loading cells (``get_data()``, large
    parquet scans) may exceed ``cell_timeout``.  Increase ``cell_timeout`` for
    notebooks with heavy S3 downloads (e.g. 300s).  Cells that time out are
    marked ⏱️ and execution continues with the next cell.

    Args:
        notebook_path: Absolute or relative path to the ``.ipynb`` file.
        cell_timeout:  Per-cell execution timeout in seconds (default 120).
        stop_on_error: If True, stop at the first cell error (default False —
                       continue so all errors are reported at once).

    Returns:
        Markdown report with one row per code cell, plus a summary line.
    """
    import re
    import traceback as tb
    from pathlib import Path as _Path

    try:
        import nbformat
        from nbconvert.preprocessors import CellExecutionError, ExecutePreprocessor
    except ImportError as exc:
        return f"nbformat/nbconvert not available: {exc}"

    nb_path = _Path(notebook_path).expanduser().resolve()
    if not nb_path.is_file():
        return f"Notebook not found: `{nb_path}`"

    try:
        with nb_path.open() as f:
            nb = nbformat.read(f, as_version=4)
    except Exception as exc:
        return f"Failed to read notebook `{nb_path}`: {exc}"

    # Run all cells, collecting per-cell results
    ep = ExecutePreprocessor(
        timeout=cell_timeout,
        kernel_name="python3",
        allow_errors=True,  # don't raise; collect errors per cell
    )

    # We need per-cell status, so run the preprocessor and inspect outputs
    try:
        nb_out, _ = ep.preprocess(nb, {"metadata": {"path": str(nb_path.parent)}})
    except Exception as exc:
        # Fatal kernel or environment failure
        return (
            f"# Notebook validation: `{nb_path.name}`\n\n"
            f"**Fatal error (kernel crash or environment issue):**\n```\n{exc}\n```"
        )

    lines: list[str] = [
        f"# Notebook validation: `{nb_path.name}`",
        "",
        f"| Cell | Type | Status | Notes |",
        "|---|---|---|---|",
    ]

    passed = failed = skipped = 0
    code_cell_idx = 0
    for cell_idx, cell in enumerate(nb_out.cells):
        if cell.cell_type != "code":
            skipped += 1
            continue
        code_cell_idx += 1

        # Check outputs for errors or timeouts
        src_preview = " ".join(cell.source.split())[:80]
        error_output = None
        timeout_hit = False
        stdout_preview = ""

        for output in cell.get("outputs", []):
            otype = output.get("output_type", "")
            if otype == "error":
                ename = output.get("ename", "Error")
                evalue = output.get("evalue", "")
                traceback_lines = output.get("traceback", [])
                # Strip ANSI escape codes
                clean_tb = "\n".join(
                    re.sub(r"\x1b\[[0-9;]*m", "", line)
                    for line in traceback_lines[-6:]  # last 6 lines
                )
                error_output = f"**{ename}: {evalue}**\n```\n{clean_tb}\n```"
            elif otype in ("stream",):
                text = output.get("text", "")
                if "Timeout" in text or "timeout" in text:
                    timeout_hit = True
                stdout_preview += text[:120]

        if timeout_hit or (
            not error_output
            and cell.get("metadata", {}).get("execution", {}).get("iopub.execute_input")
            is None
            and code_cell_idx > 1
        ):
            # Heuristic: cell never received execute_input → likely timed out
            pass

        if error_output:
            failed += 1
            note = f"{src_preview}…<br>{error_output}"
            lines.append(f"| {code_cell_idx} | code | ❌ ERROR | {note} |")
        elif timeout_hit:
            failed += 1
            lines.append(f"| {code_cell_idx} | code | ⏱️ TIMEOUT | `{src_preview}…` |")
        else:
            passed += 1
            note = stdout_preview[:80].replace("\n", " ") if stdout_preview else "ok"
            lines.append(f"| {code_cell_idx} | code | ✅ passed | {note} |")

    total = passed + failed
    lines += [
        "",
        f"**Summary:** {passed}/{total} code cells passed, {failed} failed, "
        f"{skipped} markdown cells skipped.",
    ]
    if failed == 0:
        lines.append("\n✅ **All code cells executed successfully.**")
    else:
        lines.append(
            f"\n❌ **{failed} cell(s) failed — fix the errors above and re-run validation.**"
        )

    return "\n".join(lines)


@mcp.tool()
def execute_python_cell(
    code: str,
    session_id: str = "default",
    timeout_seconds: int = 90,
) -> str:
    """
    Execute a Python code snippet in a **persistent in-process session** and
    return stdout, stderr, and any exception traceback.

    Variables, imports, and objects created in one call are available in the next
    call with the same ``session_id``.  Use this to iteratively build and test
    notebook cells — load a dataset, inspect its variables, test a plot call —
    before writing the code to the final notebook.

    **Each session is pre-populated with:**
    - ``GetAodn`` and ``plot_ts_diagram`` from DataQuery.py
    - Standard library builtins

    **Jupyter cell magics (``%%time``, ``%matplotlib``, etc.) are stripped**
    before execution so they do not cause ``SyntaxError``.

    Use ``session_id`` to keep state isolated between different notebook
    projects (e.g. ``session_id="coffs_harbour_jan2020"``).

    Common workflow::

        # Step 1 — load dataset and inspect real variables
        execute_python_cell("aodn = GetAodn()\\nds = aodn.get_dataset('argo.parquet')", "argo_nb")
        execute_python_cell("info = ds.describe()\\nprint(list(info['columns']))", "argo_nb")

        # Step 2 — test data retrieval
        execute_python_cell(\"\"\"
        df = ds.get_data(date_start='2020-01-01', date_end='2020-03-01',
                         lat_min=-35, lat_max=-27, lon_min=150, lon_max=158)
        print(df.shape, df.columns.tolist())
        \"\"\", "argo_nb")

        # Step 3 — test a plot snippet before writing the notebook
        execute_python_cell("ts = ds.get_timeseries_data(var_name='TEMP', lat=-32, lon=154, ...)", "argo_nb")

    Args:
        code:            Python source code to execute.
        session_id:      Named namespace (persists for the server's lifetime).
                         Use unique IDs per notebook project.
        timeout_seconds: Per-call wall-clock timeout (default 90 s).

    Returns:
        Markdown report: ✅ / ❌ status, stdout (up to 3000 chars),
        stderr (up to 500 chars), and full traceback on error.
    """
    import io
    import re
    import sys
    import threading
    import traceback as tb

    # Strip Jupyter magics so exec() doesn't see SyntaxErrors
    clean_lines = []
    for line in code.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("%%") or stripped.startswith("%"):
            continue
        clean_lines.append(line)
    clean_code = "\n".join(clean_lines).strip()

    if not clean_code:
        return "*(empty cell — only contained Jupyter magic lines)*"

    # Get or create the session namespace
    if session_id not in _PYTHON_SESSIONS:
        try:
            dq = _load_dataquery()
            ns: dict[str, Any] = {
                "GetAodn": dq.GetAodn,
                "plot_ts_diagram": dq.plot_ts_diagram,
                "__builtins__": __builtins__,
            }
        except Exception:
            ns = {"__builtins__": __builtins__}
        _PYTHON_SESSIONS[session_id] = ns

    namespace = _PYTHON_SESSIONS[session_id]
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()
    exception_text: str | None = None

    def _run() -> None:
        nonlocal exception_text
        old_out, old_err = sys.stdout, sys.stderr
        sys.stdout = stdout_buf
        sys.stderr = stderr_buf
        try:
            exec(compile(clean_code, "<cell>", "exec"), namespace)  # noqa: S102
        except Exception:
            exception_text = tb.format_exc()
        finally:
            sys.stdout = old_out
            sys.stderr = old_err

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)

    stdout = stdout_buf.getvalue()
    stderr = stderr_buf.getvalue()
    timed_out = thread.is_alive()

    result_lines: list[str] = []
    if timed_out:
        result_lines.append(
            f"⏱️ **TIMEOUT** — cell still running after {timeout_seconds}s. "
            "Increase `timeout_seconds` or simplify the code."
        )
    elif exception_text:
        # Keep last 30 lines of traceback (avoids flooding context)
        tb_trimmed = "\n".join(exception_text.splitlines()[-30:])
        result_lines.append(f"❌ **ERROR:**\n```\n{tb_trimmed}\n```")
    else:
        result_lines.append("✅ **Success**")

    if stdout:
        truncated = stdout[:3000]
        if len(stdout) > 3000:
            truncated += f"\n… (truncated, {len(stdout) - 3000} chars omitted)"
        result_lines.append(f"\n**stdout:**\n```\n{truncated}\n```")
    if stderr:
        result_lines.append(f"\n**stderr:**\n```\n{stderr[:500]}\n```")
    if not stdout and not stderr and not exception_text and not timed_out:
        result_lines.append("\n*(no output)*")

    return "\n".join(result_lines)


@mcp.tool()
def get_dataset_config(dataset_name: str) -> str:
    """
    Return the full raw JSON configuration for a specific AODN dataset.

    Each dataset has a JSON config file under ``config/dataset/`` that defines:
    - ``schema`` — every variable with its CF attributes (standard_name, units, etc.)
    - ``schema_transformation`` — added/dropped variables, partitioning strategy,
      global attribute overrides
    - ``aws_opendata_registry`` — dataset title, description, S3 ARN, licence, tags
    - ``run_settings`` — source S3 paths, regex filters, processing options

    The config filename matches the notebook filename (same stem, different extension).
    Use this when you need the complete, unabridged configuration rather than the
    formatted summary returned by ``get_dataset_info``.

    Args:
        dataset_name: The dataset identifier, e.g. "mooring_temperature_logger_delayed_qc".

    Returns:
        Pretty-printed JSON configuration, or an error message if not found.
    """
    import re

    stem = re.sub(r"\.(parquet|zarr)$", "", dataset_name)

    try:
        config_dir = _find_config_dir()
    except FileNotFoundError as exc:
        return str(exc)

    config_path = config_dir / f"{stem}.json"
    if not config_path.is_file():
        # Try fuzzy fallback via catalog
        catalog = get_catalog()
        close = catalog.search(stem, top_k=3)
        suggestions = ", ".join(f"`{e.dataset_name}`" for e, _ in close)
        return (
            f"Config file not found: `{config_path}`\n\n"
            f"Did you mean: {suggestions}?\n\n"
            "Use `list_datasets` or `search_datasets` to discover available datasets."
        )

    try:
        raw = load_dataset_config(str(config_path))
    except Exception as exc:
        return f"Error loading config `{config_path}`: {exc}"

    return (
        f"# Raw config: `{stem}.json`\n\n"
        f"Config path: `{config_path}`\n\n"
        "```json\n" + json.dumps(raw, indent=2) + "\n```"
    )


@mcp.tool()
def get_notebook_template(dataset_name: str) -> str:
    """
    Retrieve the Jupyter notebook template for a specific AODN dataset.

    Returns the notebook content as readable plain text (markdown cells as text,
    code cells as fenced Python blocks). Use this as a base when generating a
    new notebook for a user request — adapt the filters, variables, and plots
    to match the user's specific needs.

    If no dataset-specific notebook exists, the generic template for the dataset's
    format (parquet or zarr) is returned instead.

    Args:
        dataset_name: The dataset identifier, e.g. "mooring_temperature_logger_delayed_qc".

    Returns:
        Notebook content as readable text, or an error message if not found.
    """
    import re

    stem = re.sub(r"\.(parquet|zarr)$", "", dataset_name)

    content = get_notebook_content(stem)
    if content:
        return (
            f"# Notebook template: `{stem}`\n\n"
            "---\n"
            "> **Note:** This is the canonical template notebook. "
            "Adapt the filters, date ranges, bounding boxes, and visualisations "
            "to the user's specific request.\n\n"
            "---\n\n" + content
        )

    # Fall back to generic template based on format
    catalog = get_catalog()
    entry = catalog.get(dataset_name)
    fmt = entry.cloud_optimised_format if entry else None

    template_name = f"template_{fmt}" if fmt else None
    if template_name:
        fallback = get_notebook_content(template_name)
        if fallback:
            return (
                f"# Generic {fmt.upper()} template (no dataset-specific notebook found for `{stem}`)\n\n"
                "---\n"
                "> **Note:** No dataset-specific notebook exists for this dataset. "
                f"This is the generic `{fmt}` template. "
                "Adapt it using the dataset schema from `get_dataset_info`.\n\n"
                "---\n\n" + fallback
            )

    nb_dir = find_notebooks_dir()
    if nb_dir is None:
        return (
            f"Notebooks directory not found. "
            "Set the `AODN_NOTEBOOKS_PATH` environment variable to the path of the "
            "`notebooks/` directory in the aodn_cloud_optimised repository."
        )

    available = sorted(list_available_notebooks().keys())
    sample = available[:10]
    return (
        f"No notebook found for `{stem}`.\n\n"
        f"Available notebooks (sample): {', '.join(sample)}, …\n\n"
        "Use `list_datasets` to verify the exact dataset name."
    )


@mcp.tool()
def get_plot_guide(dataset_name: str) -> str:
    """
    Return a dataset-specific plotting guide with ready-to-use code examples.

    Identifies whether the dataset is gridded (Zarr) or non-gridded (Parquet),
    detects the actual variable names from the schema, and returns concrete
    code snippets for every applicable plot method — not generic docs.

    Covers:
    - **Non-gridded / Parquet**: spatial extent map, time series at a point,
      T-S diagram (when temperature + salinity present), custom matplotlib plots
    - **Gridded / Zarr**: time coverage heatmap, time series at a point,
      multi-day gridded map, interactive calendar viewer, radar velocity plots
      (when dataset is a radar product)

    Args:
        dataset_name: The dataset identifier, e.g. "mooring_temperature_logger_delayed_qc"
                      or "satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_australia".

    Returns:
        Markdown guide with method descriptions and copy-paste code examples
        adapted to the dataset's actual variable names.
    """
    import re

    stem = re.sub(r"\.(parquet|zarr)$", "", dataset_name)
    catalog = get_catalog()
    entry = catalog.get(stem)

    if entry is None:
        close = catalog.search(stem, top_k=3)
        suggestions = ", ".join(f"`{e.dataset_name}`" for e, _ in close)
        return (
            f"Dataset `{stem}` not found.\n\nDid you mean: {suggestions}?\n\n"
            "Use `search_datasets` to discover available datasets."
        )

    fmt = entry.cloud_optimised_format  # "parquet" or "zarr"
    is_gridded = fmt == "zarr"
    is_radar = "radar" in stem.lower()

    # --- Introspect schema variable names ---
    var_names = {v.name for v in entry.variables}
    std_names = {v.standard_name: v.name for v in entry.variables if v.standard_name}

    def _pick(*candidates: str) -> str | None:
        """Return the first candidate that exists in the schema, or None."""
        for c in candidates:
            if c in var_names:
                return c
        return None

    # Coordinate / dimension names
    lat_var = _pick("LATITUDE", "lat", "latitude")
    lon_var = _pick("LONGITUDE", "lon", "longitude")
    time_var = _pick("TIME", "time", "JULD", "detection_timestamp")

    # Scientific variable guesses from standard_name or common names
    temp_var = _pick(
        "TEMP", "sea_surface_temperature", "temperature", "TEMP_ADJUSTED"
    ) or std_names.get("sea_water_temperature")
    psal_var = _pick("PSAL", "PSAL_ADJUSTED", "salinity") or std_names.get(
        "sea_water_salinity"
    )
    depth_var = _pick("DEPTH", "NOMINAL_DEPTH", "PRES")

    # Zarr coordinate name overrides (common pattern: lowercase lat/lon/time)
    lat_override = lat_var if lat_var in ("lat", "latitude") else None
    lon_override = lon_var if lon_var in ("lon", "longitude") else None
    time_override = time_var if time_var in ("TIME",) else None

    def _kw(name: str, override: str | None) -> str:
        """Return ', name_override="value"' only when the name differs from default."""
        defaults = {
            "lat_name_override": "lat",
            "lon_name_override": "lon",
            "time_name_override": "time",
        }
        if override is None or override == defaults.get(name):
            return ""
        return f', {name}="{override}"'

    lat_kw = _kw("lat_name_override", lat_override or lat_var)
    lon_kw = _kw("lon_name_override", lon_override or lon_var)
    time_kw = _kw("time_name_override", time_override or time_var)

    # Representative scientific variable for gridded examples
    grid_var = (
        temp_var
        or _pick(
            "sea_surface_temperature", "CHL", "chlorophyll_a", "UCUR", "VCUR", "sla"
        )
        or (entry.variables[0].name if entry.variables else "var_name")
    )

    lines: list[str] = [
        f"# Plotting guide: `{stem}`",
        "",
        f"**Format:** {'Gridded (Zarr)' if is_gridded else 'Non-gridded (Parquet)'}",
        "",
        "## Confirmed schema variables",
        "",
        "These are the **exact column names** from the dataset schema. "
        "Use only these names in generated code.",
        "",
        "| Role | Variable name | Units | long_name |",
        "|---|---|---|---|",
    ]

    # Classify and list all variables from the schema for the AI
    raw_schema: dict[str, Any] = entry._raw.get("schema", {}) or {}
    role_order = {"TIME_AXIS": 0, "LAT": 1, "LON": 2, "DEPTH": 3, "DATA": 4}
    _role_map: dict[str, str] = {}
    for vname, attrs in raw_schema.items():
        if not isinstance(attrs, dict):
            continue
        axis = (attrs.get("axis") or "").upper()
        sname = (attrs.get("standard_name") or "").lower()
        vlow = vname.lower()
        if (
            axis == "T"
            or sname == "time"
            or vlow in ("time", "juld", "juld_location", "detection_timestamp")
        ):
            _role_map[vname] = "TIME_AXIS"
        elif axis == "Y" or sname in ("latitude",) or vlow in ("latitude", "lat"):
            _role_map[vname] = "LAT"
        elif axis == "X" or sname in ("longitude",) or vlow in ("longitude", "lon"):
            _role_map[vname] = "LON"
        elif (
            axis == "Z"
            or sname in ("depth", "pressure")
            or vlow in ("depth", "nominal_depth", "pres", "pres_adjusted")
        ):
            _role_map[vname] = "DEPTH"
        else:
            _role_map[vname] = "DATA"

    coord_roles = ("TIME_AXIS", "LAT", "LON", "DEPTH")
    for vname in sorted(
        raw_schema.keys(),
        key=lambda v: (role_order.get(_role_map.get(v, "DATA"), 4), v),
    ):
        attrs = raw_schema.get(vname, {})
        if not isinstance(attrs, dict):
            continue
        role = _role_map.get(vname, "DATA")
        role_label = f"**{role}**" if role in coord_roles else role
        units = attrs.get("units", "")
        lname = attrs.get("long_name", "")
        lines.append(f"| {role_label} | `{vname}` | {units} | {lname} |")

    if not raw_schema:
        lines.append("| (no schema available) | | | |")

    lines += [
        "",
        "All examples assume the dataset is already loaded:",
        "```python",
        "from DataQuery import GetAodn",
        "aodn = GetAodn()",
        f'aodn_dataset = aodn.get_dataset("{stem}.{fmt}")',
        "```",
        "",
    ]

    if not is_gridded:
        # ------------------------------------------------------------------ #
        #  NON-GRIDDED (Parquet)                                              #
        # ------------------------------------------------------------------ #
        lines += [
            "---",
            "## 1. Spatial extent map",
            "Shows which geographic areas have data (derived from spatial partition polygons).",
            "```python",
            "aodn_dataset.plot_spatial_extent()",
            "```",
            "",
            "---",
            "## 2. Load data with filters",
            "```python",
            "%%time",
            "df = aodn_dataset.get_data(",
            '    date_start="2020-01-01",',
            '    date_end="2023-01-01",',
        ]
        if lat_var and lon_var:
            lines += [
                "    lat_min=-35, lat_max=-30,",
                "    lon_min=150, lon_max=156,",
            ]
        lines += [
            ")",
            "df.head()",
            "```",
            "",
            "---",
            "## 3. Time series at a single point",
        ]
        if temp_var:
            lines += [
                f"Extracts the nearest data to (lat, lon) for `{temp_var}`"
                " and plots with an optional percentile band.",
                "```python",
                "%%time",
                "ts = aodn_dataset.get_timeseries_data(",
                f'    var_name="{temp_var}",',
                "    lat=-32.0,",
                "    lon=154.0,",
                '    date_start="2020-01-01",',
                '    date_end="2023-01-01",',
                ")",
                "ts.plot_timeseries(resample='D', show_percentiles=True)",
                "```",
            ]
        else:
            lines += [
                "```python",
                "%%time",
                "ts = aodn_dataset.get_timeseries_data(",
                '    var_name="YOUR_VAR",  # replace with a variable name from the schema',
                "    lat=-32.0, lon=154.0,",
                '    date_start="2020-01-01", date_end="2023-01-01",',
                ")",
                "ts.plot_timeseries()",
                "```",
            ]

        lines.append("")
        lines.append("---")

        if temp_var and psal_var:
            depth_col = f'"{depth_var}"' if depth_var else '"DEPTH"'
            lines += [
                f"## 4. T-S diagram (temperature vs salinity coloured by depth)",
                "Requires a DataFrame with temperature, salinity, and depth columns.",
                "```python",
                "from DataQuery import plot_ts_diagram",
                "%%time",
                "df_ts = aodn_dataset.get_data(",
                '    date_start="2020-01-01", date_end="2023-01-01",',
                f'    columns=["{temp_var}", "{psal_var}", {depth_col}],',
                ")",
                f'plot_ts_diagram(df_ts, temp_col="{temp_var}", psal_col="{psal_var}", z_col={depth_col})',
                "```",
                "",
                "---",
            ]

        lines += [
            "## Custom matplotlib plot",
            "Plot one or more variables from the loaded DataFrame directly:",
            "```python",
            "import matplotlib.pyplot as plt",
            "",
            f"fig, ax = plt.subplots(figsize=(12, 4))",
        ]
        if temp_var and time_var:
            lines += [
                f'ax.plot(df["{time_var}"], df["{temp_var}"], lw=0.8)',
                f'ax.set_ylabel("{temp_var}")',
                f'ax.set_xlabel("{time_var}")',
            ]
        elif time_var:
            first_data = next(
                (v.name for v in entry.variables if _role_map.get(v.name) == "DATA"),
                None,
            )
            plot_var = first_data or "YOUR_VAR_HERE"
            lines += [
                f'ax.plot(df["{time_var}"], df["{plot_var}"], lw=0.8)',
                f'ax.set_ylabel("{plot_var}") # replace with the variable you want',
                f'ax.set_xlabel("{time_var}")',
            ]
        else:
            lines += [
                "# WARNING: no time axis variable was identified in the schema.",
                "# Call get_dataset_schema to find the correct column names.",
                'ax.plot(df["<TIME_COLUMN>"], df["<DATA_COLUMN>"])',
            ]
        lines += [
            "ax.set_title('Time series')",
            "fig.tight_layout()",
            "plt.show()",
            "```",
        ]
        lines += [
            "",
            "---",
            "## Date arithmetic — safe month-end pattern",
            "",
            "> ⚠️ **Never use `calendar.monthrange(yr, m)[1] + 1` as a day-of-month** —",
            "> this gives 31 for April/June/September/November and raises `ValueError`.",
            "",
            "Always define this helper in notebooks that loop over calendar months:",
            "",
            "```python",
            "import pandas as pd, numpy as np",
            "",
            "def _next_month_start(yr, m):",
            '    """Exclusive upper bound for month m of year yr as np.datetime64."""',
            "    ts = pd.Timestamp(year=yr, month=m, day=1) + pd.DateOffset(months=1)",
            "    return np.datetime64(ts.strftime('%Y-%m-%d'))",
            "",
            "# Usage in a time filter loop:",
            "for yr in YEARS:",
            f"    t0 = np.datetime64(f'{{yr}}-{{MONTH:02d}}-01')",
            "    t1 = _next_month_start(yr, MONTH)          # safe — no overflow",
            f"    mask = (df[\"{time_var or 'TIME'}\"] >= t0) & (df[\"{time_var or 'TIME'}\"] < t1)",
            "```",
            "",
            "> ⚠️ **Never format numpy datetime64 with f-string spec `:%Y-%m-%d`**",
            "> — use `pd.Timestamp(arr[0]).strftime('%Y-%m-%d')` instead.",
            "",
        ]

    else:
        # ------------------------------------------------------------------ #
        #  GRIDDED (Zarr)                                                     #
        # ------------------------------------------------------------------ #
        lines += [
            "---",
            "## 1. Temporal coverage heatmap",
            "Shows data availability as a year × month grid.",
            "```python",
            "aodn_dataset.plot_time_coverage()",
            "```",
            "",
            "---",
            "## 2. Time series at a single point",
            f"Extracts `{grid_var}` at the nearest grid cell to (lat, lon) and plots it.",
            "```python",
            "%%time",
            "aodn_dataset.plot_timeseries(",
            f'    var_name="{grid_var}",',
            "    lat=-35.0,",
            "    lon=150.0,",
            '    date_start="2020-01-01",',
            '    date_end="2021-01-01",',
        ]
        if lat_kw:
            lines.append(f"   {lat_kw.lstrip(',')},")
        if lon_kw:
            lines.append(f"   {lon_kw.lstrip(',')},")
        if time_kw:
            lines.append(f"   {time_kw.lstrip(',')},")
        lines += [")", "```", ""]

        if not is_radar:
            lines += [
                "---",
                "## 3. Multi-day gridded map",
                f"Plots up to `n_days` consecutive maps of `{grid_var}` with coastlines.",
                "```python",
                "%%time",
                "aodn_dataset.plot_gridded_variable(",
                f'    var_name="{grid_var}",',
                '    date_start="2020-01-01",',
                "    lon_slice=(110, 160),",
                "    lat_slice=(-50, -10),",
                "    n_days=6,",
                '    coastline_resolution="50m",',
                "    log_scale=False,",
            ]
            if lat_kw:
                lines.append(f"   {lat_kw.lstrip(',')},")
            if lon_kw:
                lines.append(f"   {lon_kw.lstrip(',')},")
            if time_kw:
                lines.append(f"   {time_kw.lstrip(',')},")
            lines += [")", "```", ""]

            lines += [
                "---",
                "## 4. Interactive calendar viewer  *(Jupyter only)*",
                "Renders a date-picker widget that re-plots the map when the date changes.",
                "```python",
                "aodn_dataset.plot_gridded_variable_viewer_calendar(",
                f'    var_name="{grid_var}",',
                "    lon_slice=(140, 155),",
                "    lat_slice=(-45, -30),",
                "    n_days=1,",
                '    coastline_resolution="50m",',
            ]
            if lat_kw:
                lines.append(f"   {lat_kw.lstrip(',')},")
            if lon_kw:
                lines.append(f"   {lon_kw.lstrip(',')},")
            if time_kw:
                lines.append(f"   {time_kw.lstrip(',')},")
            lines += [")", "```"]

        else:
            # Radar-specific
            lines += [
                "---",
                "## 3. Radar velocity gridded plot",
                "Displays water speed (colour) and velocity vectors (arrows)"
                " for 6 consecutive hourly snapshots in a 3×2 grid.",
                "```python",
                "%%time",
                "aodn_dataset.plot_radar_water_velocity_gridded(",
                '    date_start="2020-05-01T00:00:00",',
            ]
            if time_kw:
                lines.append(f"   {time_kw.lstrip(',')},")
            lines += [")", "```", ""]

            lines += [
                "---",
                "## 4. Radar velocity rose",
                "Wind-rose style plot of time-averaged current speed and direction.",
                "```python",
                "%%time",
                "aodn_dataset.plot_radar_water_velocity_rose(",
                '    date_start="2020-01-01T00:00:00",',
                '    date_end="2021-01-01T00:00:00",',
            ]
            if time_kw:
                lines.append(f"   {time_kw.lstrip(',')},")
            lines += [")", "```"]

    # Direct xarray extraction — applies to ALL zarr datasets
    if is_gridded:
        time_dim = time_var or "time"
        lat_dim = lat_var or "lat"
        lon_dim = lon_var or "lon"
        lines += [
            "",
            "---",
            "## Direct xarray extraction (for custom / combined plots)",
            "",
            "When you need raw arrays for matplotlib/cartopy, access",
            "``aodn_dataset.zarr_store`` directly.",
            "",
            "> ⚠️ **Critical xarray rule:** never mix a `slice` and `method='nearest'`",
            "> in the **same** `.sel()` call — xarray raises `NotImplementedError`.",
            "> **Always split into two chained `.sel()` calls:**",
            "",
            "```python",
            "# WRONG — raises NotImplementedError:",
            f"# xds.sel({time_dim}=slice(date_start, date_end),",
            f"#         {lat_dim}=lat_val, {lon_dim}=lon_val, method='nearest')",
            "",
            "# CORRECT — range slice first, nearest-neighbour second:",
            "xds = aodn_dataset.zarr_store",
            "pt = (",
            "    xds",
            f'    .sel({time_dim}=slice("2020-01-01", "2020-02-01"))   # range slice',
            f'    .sel({lat_dim}=LAT, {lon_dim}=LON, method="nearest") # nearest point',
            ")",
            f'series = pt["{grid_var}"]  # xarray DataArray ready for plotting',
            "```",
            "",
            "> ⚠️ **pcolormesh shape rule:** `data` must be **2-D**.",
            f'> Use `data = xds["{grid_var}"].isel({time_dim}=0).values` or `.mean(dim="{time_dim}").values`.',
            "> Use `shading=\\'auto\\'` to avoid dimension mismatch errors.",
            "",
        ]

    lines += [
        "",
        "---",
        "> **Reminder — Jupyter magic commands:**",
        "> - `%time expr` — times a **single-line** expression only.",
        "> - `%%time` at the **top of a cell** — times the entire cell (use for multi-line calls).",
        "> - Never put code on the same line as `%%time`.",
    ]

    return "\n".join(lines)


@mcp.tool()
def get_dataquery_reference() -> str:
    """
    Return the public API reference for DataQuery.py.

    DataQuery.py is the core library used in all AODN notebooks. It provides
    classes and functions to discover datasets on S3, apply spatial/temporal
    filters, extract time series, and produce visualisations.

    This reference is parsed from the source at server startup and includes
    class descriptions, method signatures, and docstrings.

    Returns:
        Formatted API reference for DataQuery.py.
    """
    return _DATAQUERY_REFERENCE


# ---------------------------------------------------------------------------
# MCP Resources
# ---------------------------------------------------------------------------


@mcp.resource("catalog://datasets")
def catalog_resource() -> str:
    """
    Complete AODN dataset catalog as JSON.

    Each entry includes: dataset_name, cloud_optimised_format, title,
    description (truncated to 300 chars), s3_arn, catalogue_url, and a list
    of variable names with their CF standard_name.
    """
    catalog = get_catalog()
    entries = catalog.list_all()
    data: list[dict[str, Any]] = []
    for e in entries:
        data.append(
            {
                "dataset_name": e.dataset_name,
                "format": e.cloud_optimised_format,
                "title": e.title,
                "description": e.description[:300] if e.description else "",
                "s3_arn": e.s3_arn,
                "catalogue_url": e.catalogue_url,
                "variables": [
                    {
                        "name": v.name,
                        "standard_name": v.standard_name,
                        "long_name": v.long_name,
                        "units": v.units,
                    }
                    for v in e.variables
                ],
            }
        )
    return json.dumps(data, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point — starts the MCP server using stdio transport."""
    mcp.run()


if __name__ == "__main__":
    main()
