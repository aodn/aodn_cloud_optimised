import typing
import urllib.parse

import pydantic
import s3fs

import aodn_cloud_optimised.lib.clusterLib

from .cluster_config import ClusterConfig
from .coiled_cluster_options import CoiledClusterOptions
from .ec2_adapt_options import Ec2AdaptOptions
from .ec2_cluster_options import Ec2ClusterOptions
from .path_config import PathConfig


class RunSettings(pydantic.BaseModel):
    """Run configuration for processing.

    Attributes:
        paths: List of dataset path configurations.
        cluster: Cluster execution settings.
        clear_existing_data: Clear ALL existing Cloud Optimised data before run.
        force_previous_parquet_deletion: Force deletion of previous Parquet files matching an input file.
        raise_error: Raise errors and exit instead of simply logging.
        suffix: File suffix for input file matching.
        exclude: Optional regex to exclude files.
        optimised_bucket_name: Optional override for the destination S3 bucket where cloud-optimised output will be written.
        root_prefix_cloud_optimised_path: Optional override for the root key prefix inside the optimised bucket.
        bucket_raw_default_name: Optional override for the default raw data input bucket.
            If provided, it must match the bucket in any `s3_uri` that begins with 's3://'.
        batch_size: Optional maximum number of files to process in a single batch. Must be a positive integer.
        coiled_cluster_options: Optional configuration block for Coiled clusters.
            Required only when cluster mode is set to 'coiled'.

        s3_fs_common_opts: Optional shared S3 credentials/endpoint configuration.
        s3_bucket_opts: Optional dictionary specifying input/output buckets and per-bucket overrides.

    Notes:
        - If `s3_uri` starts with 's3://', and `bucket_raw_default_name` is also provided,
        the bucket in the URI must match `bucket_raw_default_name`, or validation will fail.

        - When `cluster.mode` is set to "coiled", `coiled_cluster_options` must be provided,
        otherwise a validation error will be raised.

        - s3_fs_common_opts can be overridden per bucket in s3_bucket_opts.
    """

    paths: list[PathConfig] = pydantic.Field(
        ..., description="List of input S3 path configs to process."
    )
    cluster: ClusterConfig = pydantic.Field(
        default_factory=ClusterConfig, description="Settings for cluster configuration."
    )
    clear_existing_data: bool = pydantic.Field(
        default=False,
        description="If True, clear previously optimised data before processing.",
    )
    force_previous_parquet_deletion: bool = pydantic.Field(
        default=False,
        description="If True, force deletion of previously generated Parquet files for matching input file.",
    )
    raise_error: pydantic.StrictBool = pydantic.Field(
        default=False,
        description="If True, raise errors for every logger.Error instead of continuing silently.",
    )
    suffix: str = pydantic.Field(
        default=".nc",
        description="Suffix used to identify relevant input files (e.g., '.nc').",
    )
    exclude: str | None = None
    optimised_bucket_name: str | None = pydantic.Field(
        default=None,
        description="Override the default cloud optimised S3 bucket name.",
    )
    root_prefix_cloud_optimised_path: str | None = pydantic.Field(
        default=None,
        description="Override the default root prefix path for the cloud-optimised output.",
    )
    bucket_raw_default_name: str | None = pydantic.Field(
        default=None,
        description="Override the input s3 bucket name where the input files are located.",
    )
    batch_size: int | None = pydantic.Field(
        default=None,
        description="Maximum number of files to process in a batch (must be a positive integer).",
        ge=1,
    )
    coiled_cluster_options: CoiledClusterOptions | None = pydantic.Field(
        default=None,
        description="Configuration options required when cluster.mode is 'coiled'. Ignored otherwise.",
    )
    ec2_cluster_options: Ec2ClusterOptions | None = pydantic.Field(
        default=None,
        description="Configuration options required when cluster.mode is 'ec2'. Ignored otherwise.",
    )
    ec2_adapt_options: Ec2AdaptOptions | None = pydantic.Field(
        default=None,
        description="Configuration min/max wokersoptions required when cluster.mode is 'ec2'. Ignored otherwise.",
    )
    s3_bucket_opts: dict[str, dict[str, typing.Any]] | None = pydantic.Field(
        default=None,
        description=(
            "Optional per-bucket configuration. "
            "Example: {'input_data': {'bucket': 'my-bucket', 's3_fs_opts': {...}}, "
            "'output_data': {'bucket': 'my-other-bucket', 's3_fs_opts': {...}}}"
        ),
    )
    s3_fs_common_opts: dict[str, typing.Any] | None = pydantic.Field(
        default=None,
        description=(
            "Optional arguments passed directly to s3fs.S3FileSystem for this path. "
            "Can include `key`, `secret`, `token`, `client_kwargs` (with `endpoint_url` for MinIO), etc."
        ),
    )

    @pydantic.model_validator(mode="after")
    def validate_bucket_consistency(self) -> "RunSettings":
        for path in self.paths:
            if path.s3_uri.startswith("s3://"):
                parsed = urllib.parse.urlparse(path.s3_uri)
                bucket_from_uri = parsed.netloc
                if (
                    self.bucket_raw_default_name is not None
                    and bucket_from_uri != self.bucket_raw_default_name
                ):
                    raise ValueError(
                        f"`bucket_raw_default_name` ('{self.bucket_raw_default_name}') does not match the bucket in s3_uri ('{bucket_from_uri}')."
                    )
        return self

    @pydantic.model_validator(mode="after")
    def validate_cluster_opts(self) -> "RunSettings":
        # Validate_ coiled options if mode is coiled
        if (
            self.cluster.mode
            == aodn_cloud_optimised.lib.clusterLib.ClusterMode.COILED.value
            and self.coiled_cluster_options is None
        ):
            raise ValueError(
                "coiled_cluster_options must be provided when cluster.mode is 'coiled'"
            )
        elif (
            self.cluster.mode
            == aodn_cloud_optimised.lib.clusterLib.ClusterMode.EC2.value
            and (self.ec2_cluster_options is None or self.ec2_adapt_options is None)
        ):
            raise ValueError(
                "ec2_cluster_options must be provided when cluster.mode is 'ec2'"
            )

        return self

    @pydantic.model_validator(mode="after")
    def validate_s3_fs_common_opts(self) -> "RunSettings":
        """Ensure that s3_fs_common_opts, if provided, can instantiate an s3fs filesystem."""
        if self.s3_fs_common_opts:
            try:
                s3fs.S3FileSystem(**self.s3_fs_common_opts)
            except Exception as e:
                raise ValueError(f"s3_fs_common_opts validation failed: {e}") from e
        return self

    @pydantic.model_validator(mode="after")
    def validate_s3_bucket_opts(self) -> "RunSettings":
        """Validate that per-bucket s3_fs_opts are correct and only allowed buckets exist."""
        allowed_buckets = {"input_data", "output_data"}
        if self.s3_bucket_opts:
            # Ensure only allowed keys are present
            invalid_keys = set(self.s3_bucket_opts.keys()) - allowed_buckets
            if invalid_keys:
                raise ValueError(
                    f"Invalid keys in s3_bucket_opts: {invalid_keys}. Allowed keys: {allowed_buckets}"
                )

            # Validate each bucket's s3_fs_opts
            for bucket_name, cfg in self.s3_bucket_opts.items():
                opts = cfg.get("s3_fs_opts")
                if opts:
                    try:
                        s3fs.S3FileSystem(**opts)
                    except Exception as e:
                        raise ValueError(
                            f"s3_fs_opts validation failed for '{bucket_name}': {e}"
                        ) from e
        return self
