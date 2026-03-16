import pathlib

import prefect
import prefect_aws

from aodn_cloud_optimised.bin.config.model import DatasetConfig, PathConfig
from aodn_cloud_optimised.bin.orchestrate import FileCollector


@prefect.task
def load_dataset_config(dataset_config_path: pathlib.Path) -> DatasetConfig:
    """Load dataset configuration from a file path.

    :param dataset_config_path: Path to the dataset configuration file
    :type dataset_config_path: pathlib.Path
    :return: Parsed dataset configuration
    :rtype: DatasetConfig
    """
    return DatasetConfig.from_path(dataset_config_path)


@prefect.task
def collect_s3_file_list(file_collector: FileCollector) -> list[str]:
    """Collect S3 file URIs using the provided file collector.

    :param file_collector: Configured file collector instance
    :type file_collector: FileCollector
    :return: List of S3 file URIs
    :rtype: list[str]
    """
    return file_collector.collect()


@prefect.task
def get_handler(
    handler_class,
    dataset_config: DatasetConfig,
    optimised_bucket_block: prefect_aws.S3Bucket,
) -> object:

    return handler_class(
        **{
            "dataset_config": dataset_config.model_dump(by_alias=True),
            "optimised_bucket_name": optimised_bucket_block.bucket_name,
            "root_prefix_cloud_optimised_path": optimised_bucket_block.bucket_folder,
        }
    )


@prefect.task
def optimise(
    handler,
    s3_file_uri_list: list[str],
):
    # Run the cloud optimisation
    handler.to_cloud_optimised(
        s3_file_uri_list,
    )


@prefect.flow
def generate(
    dataset_config_path: pathlib.Path,
    file_collector: FileCollector,
    handler_class,
    optimised_bucket_block: prefect_aws.S3Bucket = prefect_aws.S3Bucket.load(
        "optimised-bucket"
    ),
) -> None:
    """Orchestrate the cloud-optimisation workflow.

    Coordinates the process of loading dataset configuration, initialising the handler,
    collecting S3 files, and converting them to cloud-optimised format.

    :param dataset_config_path: Path to the dataset configuration file
    :type dataset_config_path: pathlib.Path
    :param file_collector: Configured file collector for retrieving S3 file URIs
    :type file_collector: FileCollector
    :param optimised_bucket_block: Prefect S3 bucket block for cloud-optimised outputs, defaults to "optimised-bucket"
    :type optimised_bucket_block: prefect_aws.S3Bucket, optional
    :return: None
    :rtype: None
    """

    # Get the dataset_config
    dataset_config = DatasetConfig.from_path(dataset_config_path)

    # Collect the s3 file list
    s3_file_uri_list = collect_s3_file_list(file_collector=file_collector)

    # Update dataset_config paths
    dataset_config.run_settings.paths = [
        PathConfig(
            type="parquet",
            s3_uri=s3_file_uri,
        )
        for s3_file_uri in s3_file_uri_list
    ]

    # Construct the handler
    handler = get_handler(
        handler_class=handler_class,
        dataset_config=dataset_config,
        optimised_bucket_block=optimised_bucket_block,
    )

    optimise(
        handler=handler,
        s3_file_uri_list=s3_file_uri_list,
    )


if __name__ == "__main__":

    from aodn_cloud_optimised.lib.GenericParquetHandler import (
        GenericHandler as GenericParquetHandler,
    )

    s3_bucket_block = prefect_aws.S3Bucket.load("processing-stored-bucket")
    file_collector = FileCollector.from_s3_bucket_block(
        s3_bucket_block=s3_bucket_block,
        path="seabird",
        suffix="seabird_v1_2026-01-01T21:57:17.parquet",
    )
    generate(
        dataset_config_path=pathlib.Path(
            "aodn_cloud_optimised/config/dataset/aggregated_seabird_nonqc.json"
        ),
        file_collector=file_collector,
        handler_class=GenericParquetHandler,
    )
