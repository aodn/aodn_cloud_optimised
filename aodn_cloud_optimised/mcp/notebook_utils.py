"""
Utilities for locating and converting AODN Jupyter notebooks into readable text.

The ``notebooks/`` directory is NOT part of the installed package by default.
Resolution order:

1. ``AODN_NOTEBOOKS_PATH`` environment variable
2. ``notebooks/`` sibling to the repository root (auto-detected from package path)
3. ``notebooks/`` included as package data (when the wheel is built with notebooks)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

_NOTEBOOK_EXTS = {".ipynb"}


def find_notebooks_dir() -> Path | None:
    """
    Return the path to the AODN notebooks directory, or ``None`` if not found.

    Checks ``AODN_NOTEBOOKS_PATH`` env var first, then tries to locate
    ``notebooks/`` relative to the package source tree.
    """
    env_path = os.environ.get("AODN_NOTEBOOKS_PATH")
    if env_path:
        p = Path(env_path)
        if p.is_dir():
            return p

    # Auto-detect: go up from aodn_cloud_optimised/mcp/ to repo root
    # __file__ = .../aodn_cloud_optimised/mcp/notebook_utils.py
    # parents[0] = mcp/, parents[1] = aodn_cloud_optimised/, parents[2] = repo root
    candidate = Path(__file__).parents[2] / "notebooks"
    if candidate.is_dir():
        return candidate

    # Also check as package data (when installed with notebooks/)
    pkg_root = Path(__file__).parents[1]  # aodn_cloud_optimised/
    candidate2 = pkg_root / "notebooks"
    if candidate2.is_dir():
        return candidate2

    return None


def list_available_notebooks() -> dict[str, Path]:
    """
    Return a mapping of ``{dataset_name: notebook_path}`` for all notebooks found.

    The dataset name is the notebook filename stem (without ``.ipynb``).
    """
    nb_dir = find_notebooks_dir()
    if nb_dir is None:
        return {}
    return {p.stem: p for p in sorted(nb_dir.iterdir()) if p.suffix in _NOTEBOOK_EXTS}


def get_notebook_path(dataset_name: str) -> Path | None:
    """
    Return the path to the notebook for *dataset_name*, or ``None``.

    Strips any ``.parquet``/``.zarr`` extension from *dataset_name* before
    looking up.
    """
    import re

    stem = re.sub(r"\.(parquet|zarr)$", "", dataset_name)
    nb_dir = find_notebooks_dir()
    if nb_dir is None:
        return None
    candidate = nb_dir / f"{stem}.ipynb"
    return candidate if candidate.is_file() else None


def notebook_to_text(path: Path) -> str:
    """
    Convert a ``.ipynb`` notebook to a human-readable plain-text representation.

    Markdown cells are rendered as-is; code cells are wrapped in fenced Python
    code blocks. Cell outputs are omitted to keep the text concise.
    """
    with path.open(encoding="utf-8") as f:
        nb = json.load(f)

    lines: list[str] = []
    for i, cell in enumerate(nb.get("cells", []), start=1):
        cell_type = cell.get("cell_type", "")
        source = "".join(cell.get("source", []))
        if not source.strip():
            continue

        if cell_type == "markdown":
            lines.append(source)
            lines.append("")
        elif cell_type == "code":
            lines.append(f"```python")
            lines.append(source)
            lines.append("```")
            lines.append("")
        elif cell_type == "raw":
            lines.append(source)
            lines.append("")

    return "\n".join(lines)


def get_notebook_content(dataset_name: str) -> str | None:
    """
    Return the readable text content of the notebook for *dataset_name*.

    Returns ``None`` if no matching notebook is found.
    """
    path = get_notebook_path(dataset_name)
    if path is None:
        return None
    return notebook_to_text(path)
