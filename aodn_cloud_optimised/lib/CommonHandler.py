import importlib
import os
import tempfile
import timeit
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

import s3fs
import xarray as xr
import yaml
from dask.distributed import Client, LocalCluster
from jsonschema import ValidationError, validate
from s3path import PureS3Path

from .clusterLib import ClusterManager, ClusterMode
from .config import load_dataset_config, load_variable_from_config
from .logging import get_logger


class CommonHandler:
    """
    GenericHandler for managing cloud-optimised datasets.

    This class provides common methods and functionality for handling cloud-optimised datasets.

    """

    def __init__(self, **kwargs):
        """
        Initialise the CommonHandler object.

        Args:
            **kwargs: Additional keyword arguments.
                optimised_bucket_name (str, optional): Name of the optimised bucket. Defaults to the value in the configuration.
                root_prefix_cloud_optimised_path (str, optional): Root prefix path of the location of cloud optimised files. Defaults to the value in the configuration.
                force_previous_parquet_deletion (bool, optional): Force the deletion of existing cloud optimised files (slow). Defaults to False.
                cluster_mode (str, optional): Specifies the type of cluster to create ("coiled", "ec2", "local", or None). Defaults to "local".
                dataset_config (dict): Configuration dictionary for the dataset.
                clear_existing_data (bool, optional): Flag to clear existing data. Defaults to None.
                raise_error (bool, optional): raise error if logger.error
                s3_client_opts_common (dict, s3 client options, optional): specify the s3 client options if needed. Can't be an object because Dask s* and needs to serialize objects which aren't serializable (This is an abomination)

        Attributes:
            start_time (float): The start time of the handler.
            optimised_bucket_name (str): Name of the optimised bucket.
            root_prefix_cloud_optimised_path (str): Root prefix path of the location of cloud optimised files.
            cluster_mode (str): Specifies the type of cluster to create ("coiled", "local", or None).
            dataset_config (dict): Configuration dictionary for the dataset.
            cloud_optimised_format (str): Format for cloud optimised files.
            dataset_name (str): Name of the dataset.
            schema (dict): Schema of the dataset.
            logger (logging.Logger): Logger for logging information, warnings, and errors.
            cloud_optimised_output_path (str): S3 path for cloud optimised output.
            clear_existing_data (bool): Flag to clear existing data.
            coiled_cluster_options (dict): Options for the cluster configuration.
            s3_fs_common_session (s3fs.S3FileSystem): S3 file system object for accessing S3.

        Raises:
            ValueError: If an invalid cluster_mode is specified.
        """
        self.start_time = timeit.default_timer()
        self.raise_error = kwargs.get("raise_error", False)

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

        # Cluster options
        valid_clusters = [mode.value for mode in ClusterMode]
        self.cluster_mode = kwargs.get("cluster_mode", ClusterMode.NONE)
        if isinstance(self.cluster_mode, Enum):
            self.cluster_mode = self.cluster_mode.value

        if self.cluster_mode not in valid_clusters:
            raise ValueError(
                f"Invalid cluster value: {self.cluster_mode}. Valid values are {valid_clusters}"
            )

        self.dataset_config = kwargs.get("dataset_config")
        self.cloud_optimised_format = self.dataset_config.get("cloud_optimised_format")

        self.dataset_name = self.dataset_config["dataset_name"]

        self.schema = self.dataset_config.get("schema")

        logger_name = self.dataset_config.get("logger_name", "generic")
        self.logger = get_logger(logger_name, raise_error=self.raise_error)

        cloud_optimised_format = self.dataset_config.get("cloud_optimised_format")

        optimised_bucket = kwargs.get("optimised_bucket", None)
        if optimised_bucket:
            self.optimised_bucket_name = optimised_bucket.bucket_name

        self.cloud_optimised_output_path = (
            PureS3Path.from_uri(f"s3://{self.optimised_bucket_name}")
            .joinpath(
                self.root_prefix_cloud_optimised_path,
                f"{self.dataset_name}.{cloud_optimised_format}",
            )
            .as_uri()
        )

        self.clear_existing_data = kwargs.get(
            "clear_existing_data", None
        )  # setting to True will recreate the zarr from scratch at every run!

        self.coiled_cluster_options = self.dataset_config.get("run_settings", {}).get(
            "coiled_cluster_options", None
        )

        self.cluster_manager = ClusterManager(
            cluster_mode=self.cluster_mode,
            dataset_name=self.dataset_name,
            dataset_config=self.dataset_config,
            logger=self.logger,
        )

        self.s3_client_opts_common = kwargs.get("s3_client_opts_common", None)

        self.s3_fs_common_opts = self.dataset_config["run_settings"].get(
            "s3_fs_common_opts", None
        )
        self.s3_bucket_opts = self.dataset_config["run_settings"].get(
            "s3_bucket_opts", None
        )

        self.s3_fs_common_session = kwargs.get("s3_fs_common_session", None)

        # Validation: if one is defined, both must be
        if (self.s3_client_opts_common is not None) != (
            self.s3_fs_common_session is not None
        ):
            raise ValueError(
                "Both 's3_client_opts_common' and 's3_fs_common_session' must be provided together."
            )

        self.init_s3_filesystems(
            s3_bucket_opts=self.s3_bucket_opts,
            s3_fs_common_opts=self.s3_fs_common_opts,
            s3_fs_common_session=self.s3_fs_common_session,
            s3_client_opts_common=self.s3_client_opts_common,
        )

        self.uuid_log = None
        self.s3_file_uri_list = None

        self.drop_variables = self.dataset_config.get("schema_transformation", {}).get(
            "drop_variables", []
        )

    def init_s3_filesystems(
        self,
        s3_fs_common_opts=None,
        s3_bucket_opts=None,
        s3_fs_common_session=None,
        s3_client_opts_common=None,
    ):
        """
        Initialise S3FileSystem and boto3 client options for input/output buckets.

        Args:
            s3_fs_common_opts (dict, optional): Common S3FS options applied if per-bucket options are missing.
            s3_bucket_opts (dict, optional): Per-bucket options for input_data and output_data.
            s3_fs_common_session (Any, optional): Optional s3fs session object (used in tests/mocking). Overwrites any other options from config as this is comming from kwargs
            s3_client_opts_common (dict, optional): Optional boto3 s3 client dict. Overwrite any other options from config as this is coming from kwargs
        """
        from aodn_cloud_optimised.lib.s3Tools import boto3_s3_from_opts_dict

        DEFAULT_S3FS_OPTS = dict(
            anon=False,
            default_cache_type=None,
            session=None,
            default_fill_cache=False,
            config_kwargs={"max_pool_connections": 30},
        )
        # Validate s3_fs_common_session type
        if s3_fs_common_session is not None and not isinstance(
            s3_fs_common_session, s3fs.S3FileSystem
        ):
            raise TypeError("s3_fs_common_session must be an s3fs.S3FileSystem object")

        # ------------------------------------------------------------------
        # Helpers
        # ------------------------------------------------------------------
        def get_bucket_opts(bucket_name):
            """Return the dict of s3_fs_opts for a bucket, or None."""
            if not s3_bucket_opts:
                return None
            bucket_cfg = s3_bucket_opts.get(bucket_name)
            if not bucket_cfg:
                return None
            return bucket_cfg.get("s3_fs_opts")

        def build_s3fs_instance(opts):
            """Create an S3FileSystem instance with provided opts or defaults."""
            return s3fs.S3FileSystem(**(opts or DEFAULT_S3FS_OPTS))

        def resolve_s3fs(common_session, common_opts, bucket_opts):
            """
            S3FS precedence:
                1. explicit S3FS session (mocking / tests)
                2. bucket-level override
                3. common S3FS options
                4. internal defaults
            """
            if common_session is not None:
                return common_session
            if bucket_opts:
                return build_s3fs_instance(bucket_opts)
            if common_opts:
                return build_s3fs_instance(common_opts)
            return build_s3fs_instance(None)

        def resolve_boto_opts(common_client_opts, common_s3fs_opts, bucket_s3fs_opts):
            """
            Boto client precedence:
                1. explicit boto client options
                2. bucket s3_fs_opts → boto options
                3. common s3_fs_opts → boto options
                4. None
            """
            if common_client_opts is not None:
                return common_client_opts
            if bucket_s3fs_opts:
                return boto3_s3_from_opts_dict(bucket_s3fs_opts)
            if common_s3fs_opts:
                return boto3_s3_from_opts_dict(common_s3fs_opts)
            return None

        # ------------------------------------------------------------------
        # Resolve common S3FS
        # ------------------------------------------------------------------
        self.s3_fs_common_opts = s3_fs_common_opts
        self.s3_fs = resolve_s3fs(s3_fs_common_session, s3_fs_common_opts, None)

        # ------------------------------------------------------------------
        # Resolve per-bucket S3FS
        # ------------------------------------------------------------------
        input_opts = get_bucket_opts("input_data")
        output_opts = get_bucket_opts("output_data")

        self.s3_fs_input = resolve_s3fs(
            s3_fs_common_session, s3_fs_common_opts, input_opts
        )
        self.s3_fs_output = resolve_s3fs(
            s3_fs_common_session, s3_fs_common_opts, output_opts
        )

        # ------------------------------------------------------------------
        # Resolve boto3 client options
        # ------------------------------------------------------------------
        self.s3_client_common_opts = (
            s3_client_opts_common
            if s3_client_opts_common is not None
            else (
                boto3_s3_from_opts_dict(s3_fs_common_opts)
                if s3_fs_common_opts
                else None
            )
        )

        self.s3_client_opts_input = resolve_boto_opts(
            s3_client_opts_common, s3_fs_common_opts, input_opts
        )
        self.s3_client_opts_output = resolve_boto_opts(
            s3_client_opts_common, s3_fs_common_opts, output_opts
        )

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
        Create a Dask cluster based on the specified cluster_mode.

        This method creates a Dask cluster either remotely using the Coiled service or locally
        depending on the value of the cluster_mode attribute. If coiled/ec2 cluster creation fails,
        it falls back to creating a local cluster.

        Attributes:
            cluster_mode (str): Specifies the type of cluster to create ("coiled" or "local").
            logger (logging.Logger): Logger for logging information, warnings, and errors.
            dataset_config (dict): Configuration dictionary containing cluster options.
            dataset_name (str): Name of the dataset used for naming the coiled cluster.
            cluster (Cluster): The created Dask cluster (either coiled or local).
            client (Client): Dask client connected to the created cluster.

        Raises:
            ValueError: If an invalid cluster_mode is specified.

        Returns:
            Tuple[Client, Cluster]: A tuple containing the Dask client and the created cluster.

        Notes:
            - If self.client and self.cluster become instance attributes, they can't be used with
              self.client.submit as they can't be serialised.

        """

        # TODO: quite crazy, but if client and cluster become self.client and self.cluster, then they can't be used
        #       with self.client.submit as they can't be serialize ... what a bloody pain in .. seriously

        (
            client,
            cluster,
            self.cluster_mode,
        ) = (
            self.cluster_manager.create_cluster()
        )  # self.cluster_mode is overwritten if necessary

        return client, cluster

    def _reset_cluster(self):
        self.logger.warning(
            f"{self.uuid_log}: Resetting Coiled cluster after scheduler failure…"
        )

        self.cluster_manager.close_cluster(self.client, self.cluster)

        client, cluster = self.create_cluster()
        self.logger.info(f"{self.uuid_log}: New cluster initialised.")

        return client, cluster

    def get_batch_size(self, client=None):
        """
        Calculate the optimal batch size for processing files with Dask on a cluster.

        This function determines the batch size based on the number of workers and the number
        of threads per worker. It retrieves these values from the dataset configuration or, if
        a Dask client is provided, directly from the Dask client.

        Args:
            client (dask.distributed.Client, optional): A Dask client to retrieve the number of
                threads per worker. If not provided, the number of threads is retrieved from the
                dataset configuration. Defaults to None.

        Returns:
            int: The calculated batch size for processing files.

        Explanation:
            The function first checks if a specific batch size is defined in the dataset configuration.
            This value comes from trial and error.
            If not, it determines the number of workers (`n_workers`) and the number of threads per worker
            (`n_threads`) from the dataset configuration's cluster options.

            If a Dask client is provided (`client`), it retrieves the current scheduler and thread
            information to dynamically calculate the optimal `n_threads` per worker. This is particularly
            useful for adjusting to changes in cluster resources or configurations.

            The final batch size is computed as the product of `n_workers` and `n_threads`. This value
            represents the optimal number of files that can be processed simultaneously, balancing
            parallelism with resource availability.

            The function logs the computed batch size using the logger associated with the instance.


        Args:
            client (dask.distributed.Client, optional): A Dask client to retrieve the number of
                threads per worker. If not provided, the number of threads is retrieved from the
                dataset configuration. Defaults to None.

        Returns:
            int: The calculated batch size for processing files.
        """
        # retrieve info from dataset config
        if self.dataset_config.get("run_settings", {}).get("batch_size") is not None:
            batch_size = int(self.dataset_config["run_settings"]["batch_size"])
            self.logger.info(
                f"Optimal batch size taken from dataset configuration: {batch_size}"
            )

            return batch_size
        else:
            batch_size = 1
            self.logger.warning(
                f"batch size missing from dataset configuration. Defaulting to {batch_size}"
            )

        # TODO: delete the rest below. we shouldnt do this!

        # n_workers = self.dataset_config.get("coiled_cluster_options", {}).get("n_workers", [])
        # max_n_workers = max(n_workers) if n_workers else None
        # n_workers = max_n_workers  #
        #
        # # retrieve the number of threads
        # worker_options = self.dataset_config.get("coiled_cluster_options", {}).get(
        #     "worker_options", {}
        # )
        #
        # # Retrieve nthreads if it exists
        # n_threads = worker_options.get("nthreads", 1)
        #
        # # but overwrite values from above if the client exists
        # if client is not None:
        #     scheduler_info = client.scheduler_info()
        #     nthreads_info = client.nthreads()
        #
        #     # Calculate the average number of threads per worker
        #     if nthreads_info:
        #         total_threads = sum(nthreads_info.values())
        #         num_workers = len(nthreads_info)
        #         n_threads = total_threads / num_workers
        #     else:
        #         n_threads = 1
        #
        #     # local cluster
        #     if isinstance(client.cluster, LocalCluster):
        #         # Calculate the number of workers available in the local cluster. For remote we keep the dataset config max value
        #         n_workers = len(scheduler_info["workers"])
        #
        # batch_size = int(n_workers * n_threads)  # too big?
        #
        # self.logger.info(f"Computed optimal batch size:  {batch_size}")
        # return batch_size

    @staticmethod
    def batch_process_fileset(fileset, batch_size=10):
        """
        Processes a list of files in batches.

        This method yields successive batches of files from the input fileset.
        Each batch contains up to `batch_size` files. Adjusting `batch_size`
        can impact memory usage and performance, potentially leading to out-of-memory errors. Be cautious.

        Args:
            fileset (list): A list of files to be processed in batches.
            batch_size (int, optional): The number of files to include in each batch (default is 10).

        Yields:
            list: A sublist of `fileset` containing up to `batch_size` files.
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
            The pyarrow_schema is loaded from a JSON file using `importlib.resources.files`.
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
                f"Successfully validated JSON configuration for dataset {self.dataset_name} against {os.path.basename(json_validation_path)}."
            )
        except ValidationError as e:
            raise ValueError(
                f"Failed to validat JSON configuration for dataset {self.dataset_name} against {os.path.basename(json_validation_path)}: {e}"
            )

    # TODO: remove as not used anymore
    # def is_valid_netcdf(self, nc_file_path):
    #     """
    #     Check if a file is a valid NetCDF file.
    #
    #     Parameters:
    #     - file_path (str): The path to the NetCDF file.
    #
    #     Returns:
    #     - bool: True if the file is a valid NetCDF file, False otherwise.
    #     """
    #     if not self.input_object_key.endswith(".nc"):
    #         self.logger.error(
    #             f"{self.filename}: Not valid NetCDF file. Not ending with .nc"
    #         )
    #         raise ValueError
    #
    #     try:
    #         netCDF4.Dataset(nc_file_path)
    #         return True
    #     except Exception as e:
    #         self.logger.error(f"{self.filename}: Not valid NetCDF file: {e}.")
    #         raise TypeError

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

    def create_metadata_aws_registry(self, target_directory=None):
        """
        Creates a YAML file with metadata for the AWS OpenData Registry.

        If the 'aws_opendata_registry' key is missing from the dataset configuration, a warning is logged.
        Otherwise, the metadata is extracted from the 'aws_opendata_registry' key, converted to YAML format,
        and saved to the specified directory.

        Args:
            target_directory (str, optional): Directory where the YAML file should be created.
                If not provided, a temporary directory is used.

        Returns:
            None
        """
        if "aws_opendata_registry" not in self.dataset_config:
            self.logger.warning(
                "Missing dataset configuration to populate AWS OpenData Registry"
            )
            return

        aws_registry_config = self.dataset_config["aws_opendata_registry"]
        yaml_data = yaml.dump(aws_registry_config, sort_keys=False)

        if target_directory:
            file_path = os.path.join(target_directory, f"aodn_{self.dataset_name}.yaml")
        else:
            target_directory = tempfile.gettempdir()
            file_path = os.path.join(target_directory, f"aodn_{self.dataset_name}.yaml")

        # Write the YAML data to the file
        with open(file_path.lower(), "w") as file:
            file.write(yaml_data)

        self.logger.info(f"Created AWS Registry file at: {file_path}")

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


def _get_generic_handler_class(dataset_config):
    """
    Determine the appropriate handler_nc_anmn_file class based on the dataset configuration.

    Args:
        dataset_config (dict): A dictionary containing the configuration of the dataset. The key
            "cloud_optimised_format" should be set to either "zarr" or "parquet"
            to specify the format.

    Returns:
        class: The handler_nc_anmn_file class corresponding to the specified cloud-optimized format.

    Raises:
        ValueError: If the "cloud_optimised_format" is not specified or is neither "zarr"
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
    s3_file_uri_list: List[str],
    dataset_config: dict,
    **kwargs,
) -> bool:
    """
    Iterate through a list of s3 file paths and create Cloud Optimised files for each file.

    Args:
        s3_file_uri_list (List[str]): List of file paths to process.
        dataset_config (dictionary): dataset configuration. Check config/dataset_template.json for example
        **kwargs: Additional keyword arguments for customization.
            handler_class (class, optional): Handler class for cloud optimised creation.
            force_previous_parquet_deletion (bool, optional): Whether to force deletion of old Parquet files (default is False).
            s3_fs_common_session: An aiobotocore authenticated session

    Returns:
        str: cluster_id for the cluster used. This will be a cluster name for local clusters and an id for Coiled clusters.
    """

    # this is optional! Default will use generic handler
    handler_class_name = dataset_config.get("handler_class", None)

    # loading the right handler based on configuration
    if handler_class_name is None:
        handler_class = _get_generic_handler_class(dataset_config)
    else:
        module = importlib.import_module(
            f"aodn_cloud_optimised.lib.{handler_class_name}"
        )
        handler_class = getattr(module, handler_class_name)

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
        "s3_fs_common_session": kwargs.get("s3_fs_common_session", None),
        "raise_error": kwargs.get("raise_error", False),
    }

    # Filter out None values
    filtered_kwargs = {k: v for k, v in kwargs_handler_class.items() if v is not None}
    kwargs_handler_class = filtered_kwargs
    logger_name = dataset_config.get("logger_name", "generic")
    logger = get_logger(logger_name, raise_error=kwargs.get("raise_error", False))

    kwargs_handler_class["dataset_config"] = dataset_config
    kwargs_handler_class["clear_existing_data"] = handler_clear_existing_data_arg

    kwargs_handler_class["optimised_bucket"] = kwargs.get("optimised_bucket", None)
    kwargs_handler_class["source_bucket"] = kwargs.get("source_bucket", None)

    # Creating an instance of the specified class with the provided arguments
    start_whole_processing = timeit.default_timer()
    with handler_class(**kwargs_handler_class) as handler_instance:
        handler_instance.to_cloud_optimised(s3_file_uri_list)
        # if kwargs_handler_class["cluster_mode"]:
        #     cluster_id = handler_instance.cluster_id
        # else:
        #     cluster_id = None

    time_spent_processing = timeit.default_timer() - start_whole_processing
    logger.info(f"Processed entire dataset in {time_spent_processing}s")

    return True

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
    #     local_coiled_cluster_options = {
    #         "n_workers": 2,
    #         "memory_limit": "8GB",
    #         "threads_per_worker": 2,
    #     }
    #
    #     cluster = LocalCluster(**local_coiled_cluster_options)
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
