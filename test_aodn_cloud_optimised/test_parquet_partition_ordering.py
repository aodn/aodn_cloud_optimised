import json
import pathlib

import pyarrow.dataset as pds
import pytest
import s3fs

import aodn_cloud_optimised.config.dataset
from aodn_cloud_optimised.bin.config.model.dataset_config import DatasetConfig
from aodn_cloud_optimised.lib.config import load_dataset_config
from test_aodn_cloud_optimised.conftest import PLACEHOLDER_DATASET_CONFIG_FILES


def _config_dir() -> pathlib.Path:
    return pathlib.Path(aodn_cloud_optimised.config.dataset.__path__[0]).absolute()


def _parent_config_filenames(config_dir: pathlib.Path) -> set[str]:
    parent_configs: set[str] = set()
    for config_path in config_dir.glob("*.json"):
        try:
            config_data = json.loads(config_path.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        if isinstance(config_data, dict) and "parent_config" in config_data:
            parent_configs.add(config_data["parent_config"])
    return parent_configs


def _loadable_parquet_config_filenames() -> list[str]:
    config_dir = _config_dir()
    parent_configs = _parent_config_filenames(config_dir)

    parquet_filenames: list[str] = []
    for config_path in sorted(config_dir.glob("*.json")):
        if (
            config_path.name in PLACEHOLDER_DATASET_CONFIG_FILES
            or config_path.name in parent_configs
        ):
            continue

        legacy = load_dataset_config(str(config_path))
        if legacy.get("cloud_optimised_format") == "parquet":
            parquet_filenames.append(config_path.name)

    return parquet_filenames


def _dataset_source_from_config(dataset_config: dict) -> str:
    resources = (dataset_config.get("aws_opendata_registry") or {}).get("Resources", [])
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        arn = resource.get("ARN")
        if (
            isinstance(arn, str)
            and arn.startswith("arn:aws:s3:::")
            and ".parquet" in arn
        ):
            return arn.removeprefix("arn:aws:s3:::").rstrip("/")

    return f"aodn-cloud-optimised/{dataset_config['dataset_name']}.parquet"


def _partition_order_from_s3_uri(file_uri: str, dataset_source: str) -> tuple[str, ...]:
    dataset_prefix = f"{dataset_source.rstrip('/')}/"
    if dataset_prefix not in file_uri:
        return tuple()

    relative_uri = file_uri.split(dataset_prefix, 1)[1]
    partition_segments = [
        segment for segment in relative_uri.split("/") if "=" in segment
    ]
    return tuple(segment.split("=", 1)[0] for segment in partition_segments)


@pytest.mark.s3
@pytest.mark.parametrize(
    "config_filename",
    _loadable_parquet_config_filenames(),
    ids=lambda value: value.removesuffix(".json"),
)
def test_partition_order_matches_config(config_filename):
    # Arrange
    dataset_config = DatasetConfig.load_from_cloud_optimised_directory(
        config_filename
    ).model_dump(by_alias=True)
    expected_partition_order = tuple(
        partition["source_variable"]
        for partition in dataset_config["schema_transformation"]["partitioning"]
    )
    dataset_source = _dataset_source_from_config(dataset_config)
    s3_fs = s3fs.S3FileSystem(anon=True)

    # Act
    try:
        parquet_dataset = pds.dataset(
            source=dataset_source,
            filesystem=s3_fs,
            format="parquet",
            partitioning="hive",
        )
    except (FileNotFoundError, PermissionError) as exc:
        pytest.skip(f"Live parquet dataset unavailable for {config_filename}: {exc}")
    except Exception as exc:
        pytest.fail(
            f"Failed to hydrate live parquet dataset for {config_filename} at "
            f"{dataset_source}; dataset may be corrupted ({exc})"
        )

    discovered_partition_orders = {
        _partition_order_from_s3_uri(file_uri, dataset_source)
        for file_uri in parquet_dataset.files
        if "=" in file_uri
    }

    # Assert
    assert discovered_partition_orders, (
        f"No partition folders discovered in live dataset files for "
        f"{config_filename} at {dataset_source}"
    )
    assert discovered_partition_orders == {expected_partition_order}, (
        f"Partition order mismatch for {config_filename}: expected "
        f"{expected_partition_order}, observed {sorted(discovered_partition_orders)}"
    )
