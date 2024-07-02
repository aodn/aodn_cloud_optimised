import os
import tempfile
import timeit
from typing import List

import boto3
import netCDF4
import xarray as xr
import yaml
from coiled import Cluster
from dask.distributed import Client
from dask.distributed import LocalCluster
from jsonschema import validate, ValidationError

from .config import load_variable_from_config, load_dataset_config
from .logging import get_logger

import s3fs
import unittest


class CommonHandler:
    def __init__(self, **kwargs):
        """
        Initialise the CommonHandler object.

        Args:
            **kwargs: Additional keyword arguments.
                optimised_bucket_name (str, optional[config]): Name of the optimised bucket.
                root_prefix_cloud_optimised_path (str, optional[config]): Root Prefix path of the location of cloud optimised files
                input_object_key (str): Key of the input object.
                force_previous_parquet_deletion (bool, optional[config]): Force the deletion of existing cloud optimised files(slow) (default=False)

        """
        self.start_time = timeit.default_timer()
        self.temp_dir = tempfile.TemporaryDirectory()

        # TODO: remove this variable, not used anymore.
        # self.raw_bucket_name = kwargs.get(
        #     "raw_bucket_name", load_variable_from_config("BUCKET_RAW_DEFAULT")
        # )
        self.optimised_bucket_name = kwargs.get(
            "optimised_bucket_name",
            load_variable_from_config("BUCKET_OPTIMISED_DEFAULT"),
        )
        self.root_prefix_cloud_optimised_path = kwargs.get(
            "root_prefix_cloud_optimised_path",
            load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH"),
        )

        # TODO: remove the following variables as not used anymore
        self.input_object_key = kwargs.get("input_object_key", None)
        self.input_object_keys = kwargs.get("input_object_keys", None)

        # Cluster options
        valid_clusters = ["remote", "local", None]
        self.cluster_mode = kwargs.get("cluster_mode", "local")

        if self.cluster_mode not in valid_clusters:
            raise ValueError(
                f"Invalid cluster value: {self.cluster_mode}. Valid values are {valid_clusters}"
            )

        self.dataset_config = kwargs.get("dataset_config")

        self.cloud_optimised_format = self.dataset_config.get("cloud_optimised_format")

        self.dataset_name = self.dataset_config["dataset_name"]

        self.schema = self.dataset_config.get("schema")

        logger_name = self.dataset_config.get("logger_name", "generic")
        self.logger = get_logger(logger_name)

        cloud_optimised_format = self.dataset_config.get("cloud_optimised_format")
        self.cloud_optimised_output_path = f"s3://{os.path.join(self.optimised_bucket_name, self.root_prefix_cloud_optimised_path, self.dataset_name + '.' + cloud_optimised_format)}/"

        self.clear_existing_data = kwargs.get(
            "clear_existing_data", None
        )  # setting to True will recreate the zarr from scratch at every run!

        self.cluster_options = self.dataset_config.get("cluster_options", None)

        # TODO: fix this ugly abomination
        if "unittest" in globals() or "unittest" in locals():
            # Check if unittest is imported
            if unittest.TestCase("__init__").__class__.__module__ == "unittest.case":
                self.s3_fs = s3fs.S3FileSystem(
                    anon=False,
                    client_kwargs={
                        "endpoint_url": "http://127.0.0.1:5555/",
                        "region_name": "us-east-1",
                    },
                )
            else:
                self.s3_fs = s3fs.S3FileSystem(anon=False)

    def __enter__(self):
        # Initialize resources if necessary
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Release any resources held by the handler_nc_anmn_file
        self.close()

    def close(self):
        # Release resources
        for name in dir():
            if not name.startswith("_"):
                # del globals()[name]
                self.logger.info(f"{name} has not been deleted")
        import gc

        gc.collect()

    def create_cluster(self):
        """
        Create a Dask cluster based on the cluster_mode.

        If the cluster_mode is "remote", this method attempts to create a remote cluster using
        the Coiled service. If creating the remote cluster fails, it falls back to creating a local
        cluster. If the cluster_mode is "local", it creates a local Dask cluster.

        Attributes:
            cluster_mode (str): Specifies the type of cluster to create ("remote" or "local").
            logger (logging.Logger): Logger for logging information, warnings, and errors.
            dataset_config (dict): Configuration dictionary containing cluster options.
            dataset_name (str): Name of the dataset used for naming the remote cluster.
            cluster (Cluster): The created Dask cluster (either remote or local).
            client (Client): Dask client connected to the created cluster.

        Raises:
            ValueError: If an invalid cluster_mode is specified.
        """

        # TODO: quite crazy, but if client and cluster become self.client and self.cluster, then they can't be used
        #       with self.client.submit as they can't be serialize ... what a bloody pain in .. seriously

        local_cluster_options = self.dataset_config.get(
            "local_cluster_options",
            {
                "n_workers": 2,
                "memory_limit": "8GB",
                "threads_per_worker": 2,
            },
        )

        if self.cluster_mode == "remote":
            try:
                self.logger.info("Creating a remote cluster")
                cluster_options = self.dataset_config.get("cluster_options", None)
                if cluster_options is None:
                    self.logger.error("No cluster options provided in dataset_config")

                cluster_options["name"] = f"Processing_{self.dataset_name}"

                cluster = Cluster(**cluster_options)
                client = Client(cluster)
                self.logger.info(
                    f"Coiled Cluster dask dashboard available at {cluster.dashboard_link}"
                )

            except Exception as e:
                self.logger.warning(
                    f"Could not create a Coiled cluster: {e}. Falling back to local cluster."
                )
                # Create a local Dask cluster as a fallback
                cluster = LocalCluster(**local_cluster_options)
                client = Client(cluster)
                self.logger.info(
                    f"Local Cluster dask dashboard available at {cluster.dashboard_link}"
                )
        elif self.cluster_mode == "local":
            self.logger.info("Creating a local cluster")

            cluster = LocalCluster(**local_cluster_options)
            client = Client(cluster)
            self.logger.info(
                f"Local Cluster dask dashboard available at {cluster.dashboard_link}"
            )

        return client, cluster

    def close_cluster(self, client, cluster):
        """
        Close the Dask cluster and client.

        This method attempts to close the Dask client and cluster if they are currently open.
        It logs successful closure operations and catches any exceptions that occur during
        the process, logging them as errors.

        Attributes:
            client (Client): The Dask client connected to the cluster.
            cluster (Cluster): The Dask cluster (either remote or local).
            logger (logging.Logger): Logger for logging information and errors.

        Logs:
            Info: Logs a message when the Dask client and cluster are closed successfully.
            Error: Logs a message if there is an error while closing the Dask client or cluster.
        """
        try:
            client.close()
            self.logger.info("Dask client closed successfully.")

            cluster.close()
            self.logger.info("Dask cluster closed successfully.")
        except Exception as e:
            self.logger.error(f"Error while closing the cluster or client: {e}")

    @staticmethod
    def batch_process_fileset(fileset, batch_size=10):
        """
        Processes a list of files in batches.

        This method yields successive batches of files from the input fileset.
        Each batch contains up to `batch_size` files. Adjusting `batch_size`
        can impact memory usage and performance and lead to out of memory errors. Be cautious

        Parameters
        ----------
        fileset : list
            A list of files to be processed in batches.
        batch_size : int, optional
            The number of files to include in each batch (default is 10).

        Yields
        ------
        list
            A sublist of `fileset` containing up to `batch_size` files.

        """
        # batch_size modification could lead to some out of mem
        num_files = len(fileset)
        for start_idx in range(0, num_files, batch_size):
            end_idx = min(start_idx + batch_size, num_files)
            yield fileset[start_idx:end_idx]

    def validate_json(self, json_validation_path):
        """
        Validate the JSON configuration of a dataset against a specified pyarrow_schema.
        This method uses a predefined pyarrow_schema loaded from a JSON file to validate the dataset configuration.

        Parameters:
            json_validation_path:
            self (object): The current instance of the class containing the dataset configuration.

        Raises:
            ValueError: If the dataset configuration fails validation against the pyarrow_schema.

        Example:
            Assuming `self.dataset_config` contains the dataset configuration JSON:
            ```
            dataset_validator = DatasetValidator()
            try:
                dataset_validator.validate_json()
            except ValueError as e:
                print(f"Validation error: {e}")
            ```

        Schema Loading:
            The pyarrow_schema is loaded from a JSON file using `importlib.resources.path`.
            Ensure the pyarrow_schema file (`schema_validation_parquet.json`) is accessible within the
            `aodn_cloud_optimised.config.dataset` package.

        Validation Process:
            - The method attempts to validate `self.dataset_config` against the loaded pyarrow_schema.
            - If validation is successful, it logs an info message indicating success.
            - If validation fails, it raises a `ValueError` with details of the validation error.
        """
        schema = load_dataset_config(json_validation_path)
        try:
            validate(instance=self.dataset_config, schema=schema)
            self.logger.info(
                f"JSON configuration for dataset {os.path.basename(json_validation_path)}: Validation successful."
            )
        except ValidationError as e:
            raise ValueError(
                f"JSON configuration for dataset {os.path.basename(json_validation_path)}: Validation failed: {e}"
            )

    # TODO: remove as not used anymore
    def is_valid_netcdf(self, nc_file_path):
        """
        Check if a file is a valid NetCDF file.

        Parameters:
        - file_path (str): The path to the NetCDF file.

        Returns:
        - bool: True if the file is a valid NetCDF file, False otherwise.
        """
        if not self.input_object_key.endswith(".nc"):
            self.logger.error(
                f"{self.filename}: Not valid NetCDF file. Not ending with .nc"
            )
            raise ValueError

        try:
            netCDF4.Dataset(nc_file_path)
            return True
        except Exception as e:
            self.logger.error(f"{self.filename}: Not valid NetCDF file: {e}.")
            raise TypeError

    @staticmethod
    def is_open_ds(ds: xr.Dataset) -> bool:
        """
        Check if an xarray Dataset is open.

        Args:
            ds (xarray.Dataset): The xarray Dataset to check.

        Returns:
            bool: True if the Dataset is open, False otherwise.
        """
        try:
            # Try to access an attribute or method of the Dataset
            ds.attrs
            return True  # If no error is raised, the Dataset is not closed
        except RuntimeError:
            return False  # If a RuntimeError is raised, the Dataset is closed

    def push_metadata_aws_registry(self) -> None:
        """
        Pushes metadata to the AWS OpenData Registry.

        If the 'aws_opendata_registry' key is missing from the dataset configuration, a warning is logged.
        Otherwise, the metadata is extracted from the 'aws_opendata_registry' key, converted to YAML format,
        and uploaded to the specified S3 bucket.

        Returns:
            None
        """
        if "aws_opendata_registry" not in self.dataset_config:
            self.logger.warning(
                "Missing dataset configuration to populate AWS OpenData Registry"
            )
        else:
            aws_registry_config = self.dataset_config["aws_opendata_registry"]
            yaml_data = yaml.dump(aws_registry_config)

            s3 = boto3.client("s3")

            key = os.path.join(
                self.root_prefix_cloud_optimised_path, self.dataset_name + ".yaml"
            )
            # Upload the YAML data to S3
            s3.put_object(
                Bucket=self.optimised_bucket_name,
                Key=key,
                Body=yaml_data.encode("utf-8"),
            )
            self.logger.info(
                f"Push AWS Registry file to: {os.path.join(self.root_prefix_cloud_optimised_path, self.dataset_name + '.yaml')}"
            )

    def postprocess(self, ds: xr.Dataset) -> None:
        """
        Clean up resources used during data processing.

        Args:
            ds (xarray.Dataset): The xarray Dataset to clean up.

        Returns:
            None
        """
        if self.is_open_ds(ds):
            ds.close()

        # if os.path.exists(self.tmp_input_file):
        #    os.remove(self.tmp_input_file)
        if os.path.exists(self.temp_dir.name):
            self.temp_dir.cleanup()

        self.logger.handlers.clear()


def _get_generic_handler_class(dataset_config):
    """
    Determine the appropriate handler_nc_anmn_file class based on the dataset configuration.

    This function selects and returns the handler_nc_anmn_file class for processing cloud-optimized
    datasets in either Zarr or Parquet format.

    Parameters
    ----------
    dataset_config : dict
        A dictionary containing the configuration of the dataset. The key
        "cloud_optimised_format" should be set to either "zarr" or "parquet"
        to specify the format.

    Returns
    -------
    handler_class : class
        The handler_nc_anmn_file class corresponding to the specified cloud-optimized format.

    Raises
    ------
    ValueError
        If the "cloud_optimised_format" is not specified or is neither "zarr"
        nor "parquet".
    """
    from .GenericParquetHandler import GenericHandler as parquet_handler
    from .GenericZarrHandler import GenericHandler as zarr_handler

    cloud_optimised_format = dataset_config.get("cloud_optimised_format", None)

    if cloud_optimised_format == "zarr":
        handler_class = zarr_handler
    elif cloud_optimised_format == "parquet":
        handler_class = parquet_handler
    else:
        return ValueError

    return handler_class


def cloud_optimised_creation(
    s3_file_uri_list: List[str], dataset_config: dict, **kwargs
) -> None:
    """
    Iterate through a list of s3 file paths and create Cloud Optimised files for each file.

    Args:
        s3_file_uri_list (List[str]): List of file paths to process.
        dataset_config (dictionary): dataset configuration. Check config/dataset_template.json for example
        **kwargs: Additional keyword arguments for customization.
            handler_class (class, optional): Handler class for cloud optimised creation.
            force_previous_parquet_deletion (bool, optional): Whether to force deletion of old Parquet files (default is False).

    Returns:
        None
    """

    handler_class = kwargs.get("handler_class", None)

    # loading the right handler_nc_anmn_file based on configuration
    if handler_class is None:
        handler_class = _get_generic_handler_class(dataset_config)

    handler_clear_existing_data_arg = kwargs.get("clear_existing_data", None)

    # Create the kwargs_handler_class dictionary, to be used as list of arguments to call cloud_optimised_creation -> handler_class
    # when values need to be overwritten
    kwargs_handler_class = {
        "optimised_bucket_name": kwargs.get(
            "optimised_bucket_name",
            load_variable_from_config("BUCKET_OPTIMISED_DEFAULT"),
        ),
        "root_prefix_cloud_optimised_path": kwargs.get(
            "root_prefix_cloud_optimised_path",
            load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH"),
        ),
        "cluster_mode": kwargs.get("cluster_mode", "local"),
    }

    # Filter out None values
    filtered_kwargs = {k: v for k, v in kwargs_handler_class.items() if v is not None}
    kwargs_handler_class = filtered_kwargs
    logger_name = dataset_config.get("logger_name", "generic")
    logger = get_logger(logger_name)

    kwargs_handler_class["dataset_config"] = dataset_config
    kwargs_handler_class["clear_existing_data"] = handler_clear_existing_data_arg

    # Creating an instance of the specified class with the provided arguments
    start_whole_processing = timeit.default_timer()
    with handler_class(**kwargs_handler_class) as handler_instance:
        handler_instance.to_cloud_optimised(s3_file_uri_list)

    time_spent_processing = timeit.default_timer() - start_whole_processing
    logger.info(f"Whole dataset completed in {time_spent_processing}s")

    # TODO: everything seems very slow using to_cloud_optimised. Maybe let's try to use to_cloud_optimised_single below?
    #       and comment above or do something. Will comment for now
    #
    # if dataset_config.get("cloud_optimised_format") == "parquet":
    #     def task(f, i, handler_clear_existing_data_arg=False):
    #         start_time = timeit.default_timer()
    #         try:
    #             # kwargs_handler_class["input_object_key"] = f
    #             kwargs_handler_class["dataset_config"] = dataset_config
    #             kwargs_handler_class[
    #                 "clear_existing_data"
    #             ] = handler_clear_existing_data_arg
    #
    #             # Creating an instance of the specified class with the provided arguments
    #             with handler_class(**kwargs_handler_class) as handler_instance:
    #                 handler_instance.to_cloud_optimised_single(f)
    #
    #                 time_spent = timeit.default_timer() - start_time
    #                 logger.info(
    #                     f"{i}/{len(s3_file_uri_list)}: {f} Cloud Optimised file completed in {time_spent}s"
    #                 )
    #
    #         except Exception as e:
    #             logger.error(f"{i}/{len(s3_file_uri_list)} issue with {f}: {e}")
    #
    #     local_cluster_options = {
    #         "n_workers": 2,
    #         "memory_limit": "8GB",
    #         "threads_per_worker": 2,
    #     }
    #
    #     cluster = LocalCluster(**local_cluster_options)
    #     client = Client(cluster)
    #
    #     client.amm.start()  # Start Active Memory Manager
    #     logger.info(
    #         f"Local Cluster dask dashboard available at {cluster.dashboard_link}"
    #     )
    #
    #     if handler_clear_existing_data_arg:
    #         # if handler_clear_existing_data_arg, better to wait for this task to complete before adding new data!!
    #         futures_init = [
    #             client.submit(task, s3_file_uri_list[0], 1, handler_clear_existing_data_arg=True)
    #         ]
    #         wait(futures_init)
    #
    #         # Parallel Execution with List Comprehension
    #         futures = [
    #             client.submit(task, f, i) for i, f in enumerate(s3_file_uri_list[1:], start=2)
    #         ]
    #         wait(futures)
    #     else:
    #         futures = [client.submit(task, f, i) for i, f in enumerate(s3_file_uri_list, start=1)]
    #         wait(futures)
    #
    #     client.close()
    #     cluster.close()
