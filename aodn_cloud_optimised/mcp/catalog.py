"""
Dataset catalog builder for the AODN MCP server.

Scans all ``config/dataset/*.json`` files from the installed package and builds a
searchable index. Search uses token-based fuzzy matching (via ``python-levenshtein``,
already a main dependency) across dataset names, AWS registry descriptions, and
CF variable attributes (standard_name, long_name).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any

from Levenshtein import ratio as lev_ratio

from aodn_cloud_optimised.lib.config import load_dataset_config


@dataclass
class VariableInfo:
    """Metadata for a single schema variable."""

    name: str
    type: str = ""
    standard_name: str = ""
    long_name: str = ""
    units: str = ""
    axis: str = ""


@dataclass
class DatasetEntry:
    """A single entry in the AODN dataset catalog."""

    dataset_name: str
    cloud_optimised_format: str  # "parquet" or "zarr"
    title: str = ""
    description: str = ""
    metadata_uuid: str = ""
    catalogue_url: str = ""
    s3_arn: str = ""
    variables: list[VariableInfo] = field(default_factory=list)
    data_type: str = ""
    # Raw config for full detail retrieval
    _raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def dataset_name_with_ext(self) -> str:
        return f"{self.dataset_name}.{self.cloud_optimised_format}"

    def brief_summary(self) -> str:
        """One-line summary for listing."""
        desc = self.description[:120].rstrip()
        if len(self.description) > 120:
            desc += "…"
        return f"[{self.cloud_optimised_format.upper()}] {self.dataset_name} — {desc or self.title}"


def _infer_data_type(config: dict[str, Any], variables: list[VariableInfo]) -> str:
    """Classify a dataset into an observational data type.

    Categories:
        ``gridded``           — satellite/model rasters (zarr with spatial grid)
        ``radar_velocity``    — HF radar current velocity (zarr with UCUR/VCUR)
        ``radar_wave``        — HF radar wave parameters (zarr with VAVH/WPPE)
        ``radar_wind``        — HF radar wind (zarr with WSPD/WDIR)
        ``profiles``          — depth-resolved observations (parquet with depth axis)
        ``animal_tracking``   — biologging/acoustic tracking
        ``timeseries``        — fixed-point time series (mooring, station, buoy)
        ``tabular``           — general tabular data (surveys, aggregated bio data)
    """
    name = (config.get("dataset_name") or "").lower()
    fmt = (config.get("cloud_optimised_format") or "").lower()
    var_names = {v.name.lower() for v in variables}
    var_snames = {v.standard_name.lower() for v in variables if v.standard_name}
    has_depth = any(
        v.axis.upper() == "Z"
        or v.standard_name.lower() in ("depth", "pressure")
        or v.name.lower() in ("depth", "nominal_depth", "pres", "pres_adjusted")
        for v in variables
    )

    # Radar — zarr with characteristic velocity/wave/wind variables
    if fmt == "zarr" and "radar" in name:
        if {"ucur", "vcur"} & var_names:
            return "radar_velocity"
        if {"vavh", "wppe"} & var_names:
            return "radar_wave"
        if {"wspd_mean", "wdir_mean"} & var_names or "wind" in name:
            return "radar_wind"
        return "radar_velocity"  # default for unrecognised radar

    # Animal tracking
    if any(kw in name for kw in ("animal", "tracking", "biologging", "tagging")):
        return "animal_tracking"

    # Gridded — zarr without radar markers (satellite, model)
    if fmt == "zarr":
        return "gridded"

    # Tabular survey/biology data — check before depth to avoid misclassification
    if any(
        kw in name
        for kw in (
            "aggregated",
            "diver",
            "benthic",
            "fish",
            "photoquadrat",
            "seabird",
            "seagrass",
            "kelp",
            "survey",
        )
    ):
        return "tabular"

    # Parquet with depth → profiles (Argo, XBT, CTD, glider)
    if fmt == "parquet" and has_depth:
        return "profiles"

    # Parquet timeseries keywords
    if any(
        kw in name
        for kw in (
            "mooring",
            "station",
            "wave_buoy",
            "timeseries",
            "hourly",
            "nrs",
            "realtime",
            "delayed_qc",
        )
    ):
        return "timeseries"

    # Parquet tabular (surveys, aggregated biology, vessel tracks, etc.)
    return "tabular"


def _extract_entry(config: dict[str, Any]) -> DatasetEntry:
    """Build a DatasetEntry from a raw config dict."""
    name = config.get("dataset_name", "")
    fmt = config.get("cloud_optimised_format", "parquet")
    uuid = config.get("metadata_uuid") or ""

    aws = config.get("aws_opendata_registry", {}) or {}
    title = aws.get("Name", "")
    description = aws.get("Description", "")
    doc_url = aws.get("Documentation", "")

    # Try to build catalogue URL from uuid if not present in Documentation
    catalogue_url = doc_url
    if not catalogue_url and uuid:
        catalogue_url = (
            f"https://catalogue-imos.aodn.org.au/geonetwork/srv/eng/"
            f"catalog.search#/metadata/{uuid}"
        )

    # Extract S3 ARN from Resources list
    s3_arn = ""
    for resource in aws.get("Resources", []):
        arn = resource.get("ARN", "")
        if arn.startswith("arn:aws:s3:::"):
            s3_arn = arn
            break

    # Extract variable info from schema
    variables: list[VariableInfo] = []
    for var_name, attrs in (config.get("schema", {}) or {}).items():
        if not isinstance(attrs, dict):
            continue
        variables.append(
            VariableInfo(
                name=var_name,
                type=attrs.get("type", ""),
                standard_name=attrs.get("standard_name", ""),
                long_name=attrs.get("long_name", ""),
                units=attrs.get("units", ""),
                axis=attrs.get("axis", ""),
            )
        )

    return DatasetEntry(
        dataset_name=name,
        cloud_optimised_format=fmt,
        title=title,
        description=description,
        metadata_uuid=uuid,
        catalogue_url=catalogue_url,
        s3_arn=s3_arn,
        variables=variables,
        data_type=_infer_data_type(config, variables),
        _raw=config,
    )


def _tokenize(text: str) -> list[str]:
    """Split text into lowercase tokens on whitespace and underscores."""
    return re.split(r"[\s_\-/]+", text.lower())


def _best_token_match(query_token: str, text: str) -> float:
    """Return the best Levenshtein ratio between *query_token* and any token in *text*."""
    if not text:
        return 0.0
    return max(
        (lev_ratio(query_token, ct) for ct in _tokenize(text) if ct),
        default=0.0,
    )


def _score_entry(entry: DatasetEntry, query_tokens: list[str]) -> float:
    """
    Compute a fuzzy relevance score for a dataset entry against query tokens.

    For each query token, the best match is found independently across:
    - dataset name        (weight 3) — per-token sum
    - title               (weight 2) — per-token sum
    - description         (weight 1) — per-token sum
    - variable fields     (weight 2) — MAX across all variables, per token

    Using MAX for variable scores prevents datasets with many variables from
    scoring artificially high just because of field count.
    """
    total_score = 0.0
    for qt in query_tokens:
        if not qt:
            continue

        # Fixed metadata fields — sum contributions
        total_score += _best_token_match(qt, entry.dataset_name) * 3.0
        total_score += _best_token_match(qt, entry.title) * 2.0
        total_score += _best_token_match(qt, entry.description) * 1.0

        # Variable fields — take MAX across all variables to avoid count bias
        var_score = 0.0
        for var in entry.variables:
            candidate = max(
                _best_token_match(qt, var.standard_name),
                _best_token_match(qt, var.long_name),
                _best_token_match(qt, var.name),
            )
            if candidate > var_score:
                var_score = candidate
        total_score += var_score * 2.0

    return total_score


class DatasetCatalog:
    """
    In-memory catalog of all AODN datasets built from local config JSON files.

    No S3 calls are made — the catalog is built entirely from the JSON files
    shipped with the package under ``config/dataset/``.
    """

    def __init__(self) -> None:
        self._entries: dict[str, DatasetEntry] = {}
        self._load()

    def _load(self) -> None:
        """Scan all config/dataset/*.json files and populate the catalog."""
        config_dir = _find_config_dir()
        skip_names = {
            "dataset_template",
            "radar_wave_delayed_qc_no_I_J_version_main",
            "radar_velocity_hourly_averaged_delayed_qc_no_I_J_version_main",
            "radar_wind_delayed_qc_no_I_J_version_main",
            "radar_velocity_hourly_averaged_delayed_qc_main",
            "satellite_ghrsst_main",
            "satellite_ocean_colour_1day_aqua_main",
            "satellite_ocean_colour_1day_snpp_main",
            "satellite_ocean_colour_1day_noaa20_main",
        }
        for json_path in sorted(config_dir.glob("*.json")):
            stem = json_path.stem
            if stem in skip_names:
                continue
            try:
                # load_dataset_config handles parent/child merging with the
                # same semantics used throughout the rest of the library
                # (child schema replaces parent schema entirely via merge_dicts).
                config = load_dataset_config(str(json_path))
                entry = _extract_entry(config)
                if entry.dataset_name:
                    self._entries[entry.dataset_name] = entry
            except Exception:
                pass  # Skip malformed configs silently

    def list_all(
        self,
        format_filter: str | None = None,
        prefix: str | None = None,
    ) -> list[DatasetEntry]:
        """Return all entries, optionally filtered by format or name prefix."""
        entries = list(self._entries.values())
        if format_filter:
            fmt = format_filter.lower()
            entries = [e for e in entries if e.cloud_optimised_format == fmt]
        if prefix:
            pfx = prefix.lower()
            entries = [e for e in entries if e.dataset_name.startswith(pfx)]
        return sorted(entries, key=lambda e: e.dataset_name)

    def search(self, query: str, top_k: int = 5) -> list[tuple[DatasetEntry, float]]:
        """
        Fuzzy-search the catalog for datasets relevant to *query*.

        Returns up to *top_k* ``(entry, score)`` tuples, highest score first.
        """
        query_tokens = _tokenize(query)
        scored = [
            (entry, _score_entry(entry, query_tokens))
            for entry in self._entries.values()
        ]
        scored.sort(key=lambda x: x[1], reverse=True)
        return [(e, s) for e, s in scored[:top_k] if s > 0]

    def get(self, dataset_name: str) -> DatasetEntry | None:
        """Return the entry for *dataset_name*, or ``None`` if not found."""
        entry = self._entries.get(dataset_name)
        if entry is None:
            # Try stripping a format extension (e.g. "foo.parquet" → "foo")
            stripped = re.sub(r"\.(parquet|zarr)$", "", dataset_name)
            entry = self._entries.get(stripped)
        return entry

    def __len__(self) -> int:
        return len(self._entries)


def _find_config_dir() -> Path:
    """
    Resolve the ``config/dataset/`` directory.

    Resolution order:

    1. ``AODN_CONFIG_PATH`` environment variable
    2. ``importlib.resources`` (works for installed wheels)
    3. Relative to this file (works in editable/source installs)
    """
    env_path = os.environ.get("AODN_CONFIG_PATH")
    if env_path:
        p = Path(env_path)
        if p.is_dir():
            return p

    try:
        with resources.as_file(
            resources.files("aodn_cloud_optimised").joinpath("config/dataset")
        ) as p:
            if p.is_dir():
                return p
    except (TypeError, AttributeError, ModuleNotFoundError):
        pass

    candidate = Path(__file__).parent.parent / "config" / "dataset"
    if candidate.is_dir():
        return candidate

    raise FileNotFoundError(
        "Cannot locate aodn_cloud_optimised/config/dataset/. "
        "Set AODN_CONFIG_PATH or ensure the package is installed correctly."
    )


# Module-level singleton — built once, reused across tool calls
_catalog: DatasetCatalog | None = None


def get_catalog() -> DatasetCatalog:
    """Return the shared :class:`DatasetCatalog` singleton (lazy-initialised)."""
    global _catalog
    if _catalog is None:
        _catalog = DatasetCatalog()
    return _catalog
