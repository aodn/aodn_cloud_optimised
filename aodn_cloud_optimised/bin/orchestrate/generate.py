import pathlib

from aodn_cloud_optimised.bin.config.model import DatasetConfig, PathConfig
from aodn_cloud_optimised.bin.orchestrate import FileCollector


def optimise(
    handler,
    s3_file_uri_list: list[str],
):
    # Run the cloud optimisation
    handler.to_cloud_optimised(
        s3_file_uri_list,
    )


def generate(
    dataset_config: DatasetConfig,
    file_collector: FileCollector,
    optimised_bucket_name: str,
    root_prefix_cloud_optimised_path: str | None,
) -> None:
    """Orchestrate the cloud-optimisation workflow.

    Coordinates the process of loading dataset configuration, initialising the handler,
    collecting S3 files, and converting them to cloud-optimised format.

    :param dataset_config: Parsed dataset configuration
    :type dataset_config: DatasetConfig
    :param file_collector: Configured file collector for retrieving S3 file URIs
    :type file_collector: FileCollector
    :param optimised_bucket_name: Name of the S3 bucket to write cloud-optimised output to
    :type optimised_bucket_name: str
    :param root_prefix_cloud_optimised_path: Key prefix within the bucket, or None for the bucket root
    :type root_prefix_cloud_optimised_path: str | None
    :return: None
    :rtype: None
    """

    # Collect the s3 file list
    s3_file_uri_list = file_collector.collect()

    # Update dataset_config paths
    dataset_config.run_settings.paths = [
        PathConfig(
            type="parquet",
            s3_uri=s3_file_uri,
        )
        for s3_file_uri in s3_file_uri_list
    ]

    # Construct and run the handler
    handler = dataset_config.resolve_handler_class()(
        dataset_config=dataset_config.model_dump(by_alias=True),
        optimised_bucket_name=optimised_bucket_name,
        root_prefix_cloud_optimised_path=root_prefix_cloud_optimised_path,
    )

    optimise(
        handler=handler,
        s3_file_uri_list=s3_file_uri_list,
    )


if __name__ == "__main__":
    file_collector = FileCollector.from_s3_uri(
        s3_uri="s3://aodn-dataflow-dev/thomas.galindo/processing/stored/seabird/",
        suffix="seabird_v1_2026-01-01T21:57:17.parquet",
    )
    generate(
        dataset_config=DatasetConfig.from_path(
            pathlib.Path(
                "aodn_cloud_optimised/config/dataset/aggregated_seabird_nonqc.json"
            )
        ),
        file_collector=file_collector,
        optimised_bucket_name="aodn-dataflow-dev",
        root_prefix_cloud_optimised_path="thomas.galindo/processing/stored/seabird",
    )
