"""
CLI entrypoints for AODN MCP tools.

Each tool is exposed as a standalone executable ``aodn-<tool_name>`` so that
AI assistants (e.g. GitHub Copilot CLI) that call MCP tools as shell commands
can invoke them directly.

All executables are thin wrappers: they parse CLI arguments, call the
corresponding server function, and print the result to stdout.

Usage examples::

    aodn-search_datasets "wave buoy Tasmania"
    aodn-list_datasets --format parquet
    aodn-get_dataset_info argo.parquet
    aodn-get_dataset_schema satellite_ghrsst_l3s_1d_nrt
    aodn-check_dataset_coverage argo --lat-min -45 --lat-max -10 \\
        --lon-min 140 --lon-max 155 --date-start 2020-01-01 --date-end 2020-06-30
    aodn-introspect_dataset_live argo.parquet
    aodn-get_notebook_template argo.parquet
    aodn-get_plot_guide argo.parquet
    aodn-get_dataquery_reference
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _tool_name() -> str:
    """Return the bare tool name extracted from sys.argv[0].

    E.g. ``/usr/bin/aodn-search_datasets`` → ``search_datasets``.
    """
    prog = Path(sys.argv[0]).name  # e.g. "aodn-search_datasets"
    if prog.startswith("aodn-"):
        return prog[5:]
    return prog


# ---------------------------------------------------------------------------
# Individual entry-point functions
# ---------------------------------------------------------------------------


def _run_search_datasets() -> None:
    """aodn-search_datasets <query> [--top-k N]"""
    from aodn_cloud_optimised.mcp.server import search_datasets

    parser = argparse.ArgumentParser(
        prog="aodn-search_datasets",
        description="Fuzzy-search AODN datasets by keyword or natural language.",
    )
    parser.add_argument("query", help="Search terms, e.g. 'mooring temperature'")
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        metavar="N",
        help="Maximum number of results to return (default 5).",
    )
    args = parser.parse_args()
    print(search_datasets(query=args.query, top_k=args.top_k))


def _run_list_datasets() -> None:
    """aodn-list_datasets [--format parquet|zarr] [--prefix PREFIX]"""
    from aodn_cloud_optimised.mcp.server import list_datasets

    parser = argparse.ArgumentParser(
        prog="aodn-list_datasets",
        description="List all available AODN cloud-optimised datasets.",
    )
    parser.add_argument(
        "--format",
        dest="format_filter",
        default="",
        metavar="parquet|zarr",
        help="Filter by format.",
    )
    parser.add_argument(
        "--prefix",
        default="",
        metavar="PREFIX",
        help="Filter by dataset name prefix, e.g. 'mooring'.",
    )
    args = parser.parse_args()
    print(list_datasets(format_filter=args.format_filter, prefix=args.prefix))


def _run_get_dataset_info() -> None:
    """aodn-get_dataset_info <dataset_name>"""
    from aodn_cloud_optimised.mcp.server import get_dataset_info

    parser = argparse.ArgumentParser(
        prog="aodn-get_dataset_info",
        description="Retrieve full metadata and schema for a specific AODN dataset.",
    )
    parser.add_argument("dataset_name", help="Dataset identifier, e.g. 'argo.parquet'")
    args = parser.parse_args()
    print(get_dataset_info(dataset_name=args.dataset_name))


def _run_get_dataset_schema() -> None:
    """aodn-get_dataset_schema <dataset_name>"""
    from aodn_cloud_optimised.mcp.server import get_dataset_schema

    parser = argparse.ArgumentParser(
        prog="aodn-get_dataset_schema",
        description="Return the authoritative variable listing for a dataset.",
    )
    parser.add_argument("dataset_name", help="Dataset identifier, e.g. 'argo.parquet'")
    args = parser.parse_args()
    print(get_dataset_schema(dataset_name=args.dataset_name))


def _run_get_dataset_config() -> None:
    """aodn-get_dataset_config <dataset_name>"""
    from aodn_cloud_optimised.mcp.server import get_dataset_config

    parser = argparse.ArgumentParser(
        prog="aodn-get_dataset_config",
        description="Return the full raw JSON config for a dataset.",
    )
    parser.add_argument("dataset_name", help="Dataset identifier, e.g. 'argo.parquet'")
    args = parser.parse_args()
    print(get_dataset_config(dataset_name=args.dataset_name))


def _run_check_dataset_coverage() -> None:
    """aodn-check_dataset_coverage <dataset_name> [--lat-min ...] [--lat-max ...]
    [--lon-min ...] [--lon-max ...] [--date-start ...] [--date-end ...]"""
    from aodn_cloud_optimised.mcp.server import check_dataset_coverage

    parser = argparse.ArgumentParser(
        prog="aodn-check_dataset_coverage",
        description=(
            "Query the actual data coverage of an AODN dataset from S3. "
            "Checks temporal and spatial overlap with an optional region/period."
        ),
    )
    parser.add_argument("dataset_name", help="Dataset identifier, e.g. 'argo'")
    parser.add_argument(
        "--lat-min",
        type=float,
        default=None,
        metavar="DEG",
        help="Southern bound (degrees).",
    )
    parser.add_argument(
        "--lat-max",
        type=float,
        default=None,
        metavar="DEG",
        help="Northern bound (degrees).",
    )
    parser.add_argument(
        "--lon-min",
        type=float,
        default=None,
        metavar="DEG",
        help="Western bound (degrees).",
    )
    parser.add_argument(
        "--lon-max",
        type=float,
        default=None,
        metavar="DEG",
        help="Eastern bound (degrees).",
    )
    parser.add_argument(
        "--date-start",
        default=None,
        metavar="YYYY-MM-DD",
        help="Start of period of interest (ISO-8601).",
    )
    parser.add_argument(
        "--date-end",
        default=None,
        metavar="YYYY-MM-DD",
        help="End of period of interest (ISO-8601).",
    )
    args = parser.parse_args()
    print(
        check_dataset_coverage(
            dataset_name=args.dataset_name,
            lat_min=args.lat_min,
            lat_max=args.lat_max,
            lon_min=args.lon_min,
            lon_max=args.lon_max,
            date_start=args.date_start,
            date_end=args.date_end,
        )
    )


def _run_introspect_dataset_live() -> None:
    """aodn-introspect_dataset_live <dataset_name>"""
    from aodn_cloud_optimised.mcp.server import introspect_dataset_live

    parser = argparse.ArgumentParser(
        prog="aodn-introspect_dataset_live",
        description=(
            "Introspect a live dataset from S3: actual variable names, "
            "dtypes, shape, and coordinate roles."
        ),
    )
    parser.add_argument("dataset_name", help="Dataset identifier, e.g. 'argo.parquet'")
    args = parser.parse_args()
    print(introspect_dataset_live(dataset_name=args.dataset_name))


def _run_get_notebook_template() -> None:
    """aodn-get_notebook_template <dataset_name>"""
    from aodn_cloud_optimised.mcp.server import get_notebook_template

    parser = argparse.ArgumentParser(
        prog="aodn-get_notebook_template",
        description="Return the full content of the matching notebook as readable text.",
    )
    parser.add_argument("dataset_name", help="Dataset identifier, e.g. 'argo.parquet'")
    args = parser.parse_args()
    print(get_notebook_template(dataset_name=args.dataset_name))


def _run_get_plot_guide() -> None:
    """aodn-get_plot_guide <dataset_name>"""
    from aodn_cloud_optimised.mcp.server import get_plot_guide

    parser = argparse.ArgumentParser(
        prog="aodn-get_plot_guide",
        description="Return a concise plotting guide for a specific dataset.",
    )
    parser.add_argument("dataset_name", help="Dataset identifier, e.g. 'argo.parquet'")
    args = parser.parse_args()
    print(get_plot_guide(dataset_name=args.dataset_name))


def _run_get_dataquery_reference() -> None:
    """aodn-get_dataquery_reference"""
    from aodn_cloud_optimised.mcp.server import get_dataquery_reference

    parser = argparse.ArgumentParser(
        prog="aodn-get_dataquery_reference",
        description="Return a concise reference of DataQuery.py's public API.",
    )
    parser.parse_args()  # no arguments, but supports --help
    print(get_dataquery_reference())


def _run_get_dataset_summary() -> None:
    """aodn-get_dataset_summary <dataset_name>"""
    from aodn_cloud_optimised.mcp.server import get_dataset_summary

    parser = argparse.ArgumentParser(
        prog="aodn-get_dataset_summary",
        description="Return a comprehensive summary of a dataset.",
    )
    parser.add_argument("dataset_name", help="Dataset identifier, e.g. 'argo.parquet'")
    args = parser.parse_args()
    print(get_dataset_summary(dataset_name=args.dataset_name))


def _run_start_notebook() -> None:
    """aodn-start_notebook --title TITLE --output PATH"""
    from aodn_cloud_optimised.mcp.server import start_notebook

    parser = argparse.ArgumentParser(
        prog="aodn-start_notebook",
        description="Start building a validated Jupyter notebook.",
    )
    parser.add_argument("--title", required=True, help="Notebook title.")
    parser.add_argument("--output", required=True, help="Output .ipynb path.")
    parser.add_argument(
        "--request", default="", help="User's original question/request."
    )
    args = parser.parse_args()
    print(
        start_notebook(
            title=args.title, output_path=args.output, user_request=args.request
        )
    )


def _run_add_notebook_cell() -> None:
    """aodn-add_notebook_cell --session SESSION_ID --type code|markdown SOURCE"""
    from aodn_cloud_optimised.mcp.server import add_notebook_cell

    parser = argparse.ArgumentParser(
        prog="aodn-add_notebook_cell",
        description="Add a validated cell to a notebook draft.",
    )
    parser.add_argument(
        "--session", required=True, help="Session ID from start_notebook."
    )
    parser.add_argument(
        "--type",
        dest="cell_type",
        default="code",
        choices=["code", "markdown"],
        help="Cell type (default: code).",
    )
    parser.add_argument("source", help="Cell content (Python code or Markdown).")
    args = parser.parse_args()
    print(
        add_notebook_cell(
            session_id=args.session, source=args.source, cell_type=args.cell_type
        )
    )


def _run_save_notebook() -> None:
    """aodn-save_notebook --session SESSION_ID"""
    from aodn_cloud_optimised.mcp.server import save_notebook

    parser = argparse.ArgumentParser(
        prog="aodn-save_notebook",
        description="Save a validated notebook draft to disk.",
    )
    parser.add_argument(
        "--session", required=True, help="Session ID from start_notebook."
    )
    args = parser.parse_args()
    print(save_notebook(session_id=args.session))


def _run_replace_notebook_cell() -> None:
    """aodn-replace_notebook_cell --session SESSION_ID --index N --type code|markdown SOURCE"""
    from aodn_cloud_optimised.mcp.server import replace_notebook_cell

    parser = argparse.ArgumentParser(
        prog="aodn-replace_notebook_cell",
        description="Replace a cell in a notebook draft (validates code cells).",
    )
    parser.add_argument(
        "--session", required=True, help="Session ID from start_notebook."
    )
    parser.add_argument(
        "--index", required=True, type=int, help="Zero-based cell index."
    )
    parser.add_argument(
        "--type",
        dest="cell_type",
        default="code",
        choices=["code", "markdown"],
        help="Cell type (default: code).",
    )
    parser.add_argument("source", help="New cell content (Python code or Markdown).")
    args = parser.parse_args()
    print(
        replace_notebook_cell(
            session_id=args.session,
            cell_index=args.index,
            source=args.source,
            cell_type=args.cell_type,
        )
    )


def _run_fix_notebook() -> None:
    """aodn-fix_notebook NOTEBOOK_PATH"""
    from aodn_cloud_optimised.mcp.server import fix_notebook

    parser = argparse.ArgumentParser(
        prog="aodn-fix_notebook",
        description="Validate an existing notebook; import into builder if broken.",
    )
    parser.add_argument("notebook_path", help="Path to the .ipynb file.")
    args = parser.parse_args()
    print(fix_notebook(notebook_path=args.notebook_path))


# ---------------------------------------------------------------------------
# Dispatcher (used by all entry points)
# ---------------------------------------------------------------------------

_DISPATCH: dict[str, object] = {
    "search_datasets": _run_search_datasets,
    "list_datasets": _run_list_datasets,
    "get_dataset_info": _run_get_dataset_info,
    "get_dataset_schema": _run_get_dataset_schema,
    "get_dataset_config": _run_get_dataset_config,
    "check_dataset_coverage": _run_check_dataset_coverage,
    "introspect_dataset_live": _run_introspect_dataset_live,
    "get_notebook_template": _run_get_notebook_template,
    "get_plot_guide": _run_get_plot_guide,
    "get_dataquery_reference": _run_get_dataquery_reference,
    "get_dataset_summary": _run_get_dataset_summary,
    "start_notebook": _run_start_notebook,
    "add_notebook_cell": _run_add_notebook_cell,
    "save_notebook": _run_save_notebook,
    "replace_notebook_cell": _run_replace_notebook_cell,
    "fix_notebook": _run_fix_notebook,
}


def main() -> None:
    """Generic dispatcher: reads the tool name from sys.argv[0]."""
    tool = _tool_name()
    fn = _DISPATCH.get(tool)
    if fn is None:
        print(
            f"Unknown AODN MCP tool: '{tool}'\n"
            f"Available tools: {', '.join(sorted(_DISPATCH))}",
            file=sys.stderr,
        )
        sys.exit(1)
    fn()  # type: ignore[operator]
