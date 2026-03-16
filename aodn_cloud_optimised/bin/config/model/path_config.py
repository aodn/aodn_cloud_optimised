import pathlib
import re
import typing
import urllib.parse
import warnings

import pydantic


class PathConfig(pydantic.BaseModel):
    """Input path configuration.

    Attributes:
        s3_uri: S3 URI as a POSIX path string.
        type: Type of dataset. Can be "files", "parquet", or "zarr".
        partitioning: Optional, used only for Parquet datasets (e.g., "hive").
        filter: List of regex patterns to filter files (only valid for type="files").
        year_range: Optional Year filter: None, one year, or a two-year inclusive range, or a list of exclusive years to process. (only valid for type="files")

    """

    s3_uri: str
    type: typing.Literal["files", "parquet", "zarr"] | None = pydantic.Field(
        default=None,
        description="Dataset type. One of 'files', 'parquet', or 'zarr'. Defaults to 'files' if not specified.",
    )
    partitioning: str | None = pydantic.Field(
        default=None,
        description="Partitioning scheme, only valid when type='parquet'. Currently supports 'hive'.",
    )
    filter: str | list[str] = pydantic.Field(
        default_factory=list,
        description="List of regex pattern used to filter matching files. Must contain None or one pattern only.",
    )
    year_range: list[int] | None = pydantic.Field(
        default=None,
        description="Must be None (no filtering), a single year [YYYY], a two-year range [YYYY, YYYY], or a list of exclusive years to process [YYYY, YYYY, YYYY]",
    )

    @pydantic.field_validator("year_range", mode="after")
    def validate_year_range(cls, v: list[int] | None) -> list[int] | None:
        if v is None or len(v) == 0:
            return None  # No year filtering

        # Validate all items are int
        if not all(isinstance(year, int) for year in v):
            raise ValueError("year_range must be a list of integers")
        # If one year, return as single-item list
        if len(v) == 1:
            return v

        if len(v) == 2:
            start, end = v
            if start > end:
                raise ValueError("year_range start year must be <= end year")
            elif start == end:
                raise ValueError("year_range start year must be != end year")
            # Return inclusive range list
            return list(range(start, end + 1))

        # More than 2 years, treat as explicit list
        # Validate sorted ascending and unique
        if sorted(v) != v:
            raise ValueError("year_range list must be sorted in ascending order")
        if len(set(v)) != len(v):
            raise ValueError("year_range list must contain unique years")

        return v

    @pydantic.field_validator("s3_uri", mode="after")
    def validate_s3_uri(cls, v: str) -> str:
        if not isinstance(v, str):
            raise TypeError("s3_uri must be a string")

        if v.startswith("s3://"):
            parsed = urllib.parse.urlparse(v)
            if not parsed.netloc:
                raise ValueError("s3_uri must include a bucket name after 's3://'")
            # TODO: remove the commented lines below. This used to be a good test, but now dataset could be a parquet hive partitioned at the root of the bucket.
            # if not parsed.path or parsed.path == "/":
            #     raise ValueError(
            #         "s3_uri must include a valid key path after the bucket"
            #     )
            try:
                pathlib.PurePosixPath(parsed.path.lstrip("/"))
            except Exception as e:
                raise ValueError(f"s3_uri key path is not a valid POSIX path: {e}")
        else:
            # Validate as a relative POSIX path (e.g. "IMOS/SRS/...")
            try:
                pathlib.PurePosixPath(v)
            except Exception as e:
                raise ValueError(f"s3_uri is not a valid relative POSIX path: {e}")

        return v

    @pydantic.field_validator("filter", mode="before")
    @classmethod
    def extract_string_from_filter(cls, v):
        # 1. Handle List input: check length and extract the string
        if isinstance(v, list):
            if len(v) > 1:
                raise ValueError(
                    f"Filter list must contain at most one element, got {len(v)}"
                )
            return v[0] if v else ""

        # 2. Handle None/Empty
        if v is None:
            return ""

        # 3. If it's already a string, just pass it through
        return v

    @pydantic.field_validator("filter", mode="after")
    @classmethod
    def validate_regex(cls, v: str) -> str:
        # Now 'v' is guaranteed to be a string
        if not v:
            return v

        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"Invalid regex: {v} ({e})")
        return v

    @pydantic.model_validator(mode="after")
    def validate_cross_fields(cls, values):
        dataset_type = values.type or "files"
        if values.type is None:
            warnings.warn(
                "No 'type' specified in PathConfig. Assuming 'files' as default.",
                UserWarning,
                stacklevel=2,
            )
            values.type = "files"
            if (
                any(".parquet" in f for f in values.filter)
                or ".parquet" in values.s3_uri
            ):
                raise ValueError(
                    "type must be defined as 'parquet' in run_settings.paths config if ingesting a parquet dataset."
                )
            elif any(".zarr" in f for f in values.filter) or ".zarr" in values.s3_uri:
                raise ValueError(
                    "type must be defined as 'zarr' in run_settings.paths config if ingesting a zarr dataset."
                )

        if dataset_type == "parquet":
            if values.filter:
                raise ValueError("filter must not be defined when type='parquet'")
            if values.year_range:
                raise ValueError("year_range must not be defined when type='parquet'")
            if values.partitioning not in (None, "hive"):
                raise ValueError(
                    f"Invalid partitioning='{values.partitioning}' for parquet dataset. Only 'hive' is supported."
                )

        elif dataset_type == "zarr":
            if values.filter:
                raise ValueError("filter must not be defined when type='zarr'")
            if values.year_range:
                raise ValueError("year_range must not be defined when type='zarr'")
            if values.partitioning:
                raise ValueError("partitioning is not applicable when type='zarr'")

        elif dataset_type == "files":
            if values.partitioning:
                raise ValueError("partitioning is not applicable when type='files'")

        else:
            raise ValueError(f"Unsupported dataset type: {dataset_type}")

        return values
