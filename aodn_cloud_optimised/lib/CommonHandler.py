import ctypes
import os
import tempfile
import time
import timeit
from typing import List

import boto3
import netCDF4
import xarray as xr
import yaml
from dask.distributed import Client, LocalCluster, wait
from jsonschema import validate, ValidationError
from urllib.parse import urlparse

from .config import load_variable_from_config, load_dataset_config
from .logging import get_logger


class CommonHandler:
    def __init__(self, **kwargs):
        """
        Initialise the CommonHandler object.

        Args:
            **kwargs: Additional keyword arguments.
                raw_bucket_name (str, optional[config]): Name of the raw bucket.
                optimised_bucket_name (str, optional[config]): Name of the optimised bucket.
                root_prefix_cloud_optimised_path (str, optional[config]): Root Prefix path of the location of cloud optimised files
                input_object_key (str): Key of the input object.
                force_old_pq_del (bool, optional[config]): Force the deletion of existing cloud optimised files(slow) (default=False)

        """
        self.start_time = timeit.default_timer()
        self.temp_dir = tempfile.TemporaryDirectory()

        self.raw_bucket_name = kwargs.get(
            "raw_bucket_name", load_variable_from_config("BUCKET_RAW_DEFAULT")
        )
        self.optimised_bucket_name = kwargs.get(
            "optimised_bucket_name",
            load_variable_from_config("BUCKET_OPTIMISED_DEFAULT"),
        )
        self.root_prefix_cloud_optimised_path = kwargs.get(
            "root_prefix_cloud_optimised_path",
            load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH"),
        )

        self.input_object_key = kwargs.get("input_object_key", None)

        self.dataset_config = kwargs.get("dataset_config")

        self.cloud_optimised_format = self.dataset_config.get("cloud_optimised_format")

        self.dataset_name = self.dataset_config["dataset_name"]

        self.schema = self.dataset_config.get("schema")

        logger_name = self.dataset_config.get("logger_name", "generic")
        self.logger = get_logger(logger_name)

        cloud_optimised_format = self.dataset_config.get("cloud_optimised_format")
        self.cloud_optimised_output_path = f"s3://{os.path.join(self.optimised_bucket_name, self.root_prefix_cloud_optimised_path, self.dataset_name + '.' + cloud_optimised_format)}/"

        if self.input_object_key is not None:
            self.filename = os.path.basename(self.input_object_key)
            self.tmp_input_file = self.get_s3_raw_obj()
        else:
            self.logger.error("No input object given")
            raise ValueError

    def __enter__(self):
        # Initialize resources if necessary
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Release any resources held by the handler
        self.close()

    def close(self):
        # Release resources
        for name in dir():
            if not name.startswith("_"):
                # del globals()[name]
                self.logger.info(f"{name} has not been deleted")
        import gc

        gc.collect()

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
            self.logger.info("JSON configuration for dataset: Validation successful.")
        except ValidationError as e:
            raise ValueError(f"JSON configuration for dataset: Validation failed: {e}")

    @staticmethod
    def prefix_exists(s3_path):
        """
        Check if a given S3 prefix exists.

        This function parses an S3 path to extract the bucket name and prefix,
        then checks if the prefix exists in the specified S3 bucket.

        Args:
            s3_path (str): The S3 path to check, in the format "s3://bucket-name/prefix".

        Returns:
            bool: True if the prefix exists, False otherwise.

        Raises:
            ValueError: If the provided path does not appear to be an S3 URL.

        """
        # Parse the S3 path
        parsed_url = urlparse(s3_path)

        if parsed_url.scheme != "s3":
            raise ValueError("The provided path does not appear to be an S3 URL.")

        bucket_name = parsed_url.netloc
        prefix = parsed_url.path.lstrip("/")

        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, MaxKeys=1
        )
        return "Contents" in response

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

    def get_s3_raw_obj(self) -> str:
        """
        Download an S3 object from the raw bucket to a temporary file.

        :return: Local filepath of the temporary file.
        :rtype: str
        """

        s3 = boto3.client("s3")

        # Construct the full path for the temporary file
        temp_file_path = os.path.join(
            self.temp_dir.name, os.path.basename(self.input_object_key)
        )

        # Download the S3 object to the temporary file
        s3.download_file(self.raw_bucket_name, self.input_object_key, temp_file_path)

        self.logger.info(
            f"{self.filename}: Downloading {self.input_object_key} object from {self.raw_bucket_name} bucket"
        )
        return temp_file_path

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

        if os.path.exists(self.tmp_input_file):
            os.remove(self.tmp_input_file)
        if os.path.exists(self.temp_dir.name):
            self.temp_dir.cleanup()

        self.logger.handlers.clear()


def _get_generic_handler_class(dataset_config):
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


def cloud_optimised_creation(obj_key: str, dataset_config, **kwargs) -> None:
    """
    Create Cloud Optimised files for a specific object key in an S3 bucket.

    Args:
        obj_key (str): The object key (file path) of the NetCDF file to process.
        dataset_config (dictionary): dataset configuration. Check config/dataset_template.json for example
        **kwargs: Additional keyword arguments for customization.
            handler_class (class, optional): Handler class for cloud optimised  creation (default is GenericHandler).
            force_old_pq_del (bool, optional): Whether to force deletion of old Parquet files (default is False).

    Returns:
        None
    """
    handler_class = kwargs.get("handler_class", None)

    # loading the right handler based on configuration
    if handler_class is None:
        handler_class = _get_generic_handler_class(dataset_config)

    handler_reprocess_arg = kwargs.get("handler_reprocess_arg", None)

    kwargs_handler_class = {
        "raw_bucket_name": kwargs.get(
            "raw_bucket_name", load_variable_from_config("BUCKET_RAW_DEFAULT")
        ),
        "optimised_bucket_name": kwargs.get(
            "optimised_bucket_name",
            load_variable_from_config("BUCKET_OPTIMISED_DEFAULT"),
        ),
        "root_prefix_cloud_optimised_path": kwargs.get(
            "root_prefix_cloud_optimised_path",
            load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH"),
        ),
        "input_object_key": obj_key,
        "dataset_config": dataset_config,
        "reprocess": handler_reprocess_arg,
    }

    # Creating an instance of the specified class with the provided arguments
    with handler_class(**kwargs_handler_class) as handler_instance:
        handler_instance.to_cloud_optimised()


def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


def cloud_optimised_creation_loop(
    obj_ls: List[str], dataset_config: dict, **kwargs
) -> None:
    """
    Iterate through a list of file paths and create Cloud Optimised files for each file.

    Args:
        obj_ls (List[str]): List of file paths to process.
        dataset_config (dictionary): dataset configuration. Check config/dataset_template.json for example
        **kwargs: Additional keyword arguments for customization.
            handler_class (class, optional): Handler class for cloud optimised creation.
            force_old_pq_del (bool, optional): Whether to force deletion of old Parquet files (default is False).

    Returns:
        None
    """

    handler_class = kwargs.get("handler_class", None)

    # loading the right handler based on configuration
    if handler_class is None:
        handler_class = _get_generic_handler_class(dataset_config)

    handler_reprocess_arg = kwargs.get("reprocess", None)

    # Create the kwargs_handler_class dictionary, to be used as list of arguments to call cloud_optimised_creation -> handler_class
    # when values need to be overwritten
    kwargs_handler_class = {
        "raw_bucket_name": kwargs.get(
            "raw_bucket_name", load_variable_from_config("BUCKET_RAW_DEFAULT")
        ),
        "optimised_bucket_name": kwargs.get(
            "optimised_bucket_name",
            load_variable_from_config("BUCKET_OPTIMISED_DEFAULT"),
        ),
        "root_prefix_cloud_optimised_path": kwargs.get(
            "root_prefix_cloud_optimised_path",
            load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH"),
        ),
    }

    # Filter out None values
    filtered_kwargs = {k: v for k, v in kwargs_handler_class.items() if v is not None}

    logger_name = dataset_config.get("logger_name", "generic")
    logger = get_logger(logger_name)

    # Attempt to create a Coiled cluster
    try:
        from coiled import Cluster

        if dataset_config.get("cloud_optimised_format") == "parquet":
            cluster = Cluster(
                n_workers=[1, 20],
                scheduler_vm_types="t3.small",
                worker_vm_types="t3.medium",
                allow_ingress_from="me",
                compute_purchase_option="spot_with_fallback",
            )
        elif dataset_config.get("cloud_optimised_format") == "zarr":
            # cluster = Cluster(
            #     n_workers=[1,5],
            #     scheduler_vm_types="c6gn.medium",  # t3.small",
            #     worker_vm_types="c6gn.4xlarge",
            #     allow_ingress_from="me",
            #     compute_purchase_option="spot_with_fallback",
            # )
            cluster = Cluster(
                n_workers=1,  # havent managed to use more than one worker successfully without corrupting the zarr dataset, even by using the dask distributed lock
                scheduler_vm_types="c6gn.medium",  # t3.small",
                worker_vm_types="c6gn.2xlarge",
                allow_ingress_from="me",
                compute_purchase_option="spot_with_fallback",
            )

        client = Client(cluster)

    except Exception as e:
        logger.warning(
            f"Could not create Coiled cluster: {e}. Falling back to local cluster."
        )
        # Create a local Dask cluster as a fallback
        cluster = LocalCluster()
        client = Client(cluster)

    start_whole_processing = timeit.default_timer()

    client.amm.start()  # Start Active Memory Manager

    # Define the task function to be executed in parallel
    def task(f, i):
        start_time = timeit.default_timer()
        try:
            cloud_optimised_creation(
                f,
                dataset_config,
                handler_class=handler_class,
                handler_reprocess_arg=handler_reprocess_arg,
                **filtered_kwargs,
            )
            time_spent = timeit.default_timer() - start_time
            logger.info(
                f"{i}/{len(obj_ls)}: {f} Cloud Optimised file completed in {time_spent}s"
            )
        except Exception as e:
            logger.error(f"{i}/{len(obj_ls)} issue with {f}: {e}")

    # Number of tasks to submit at once
    n_tasks = 4

    # Function to submit tasks in batches
    def submit_tasks_in_batches(client, task, obj_ls, n_tasks):
        results = []
        for i in range(0, len(obj_ls), n_tasks):
            batch = obj_ls[i : i + n_tasks]
            futures = [client.submit(task, f, i + j) for j, f in enumerate(batch)]
            wait(futures)  # Wait for the batch to complete
            results.extend(client.gather(futures))
        return results

    client.amm.start()

    def wait_for_no_workers(client):
        while len(client.scheduler_info()["workers"]) > 0:
            time.sleep(1)

    # Submit tasks to the Dask cluster
    if dataset_config.get("cloud_optimised_format") == "parquet":

        # Parallel Execution with List Comprehension
        futures = [client.submit(task, f, i) for i, f in enumerate(obj_ls, start=1)]

        # Wait for all futures to complete
        wait(futures)

    elif dataset_config.get("cloud_optimised_format") == "zarr":

        # TODO: because of memory leaks growing over time, it could make sense to define the cluster in this if elif
        #       section and recreate it every 50-100 files?
        # TODO: we need to get the parallelisation work like this for now, but eventually, the handler class should take
        #       many NetCDF files as a list, and then do the dask processing of mfdataset and to_zarr. but how to deal
        #       the download of the input data without saturating the disk, especially if to_zarr(compute=False)
        # TODO: I tried verious thing to have multiple workers for zarr. the main thing is to have a proper lock on the
        #       zarr dataset to avoid corruption and having multiple threads writing at the same time. I tried using a lock
        #       which seems to work for one worker, but doesn't get shared amongts workers as claimed by the doc.
        #       I tried retrieving the scheduler worker, and have it as an argument of task function. However, its not
        #       possible to serialise a client() object with pickle or dill, and have it as a parameter... Nor was it
        #       possible to have it as a global variable.
        #       I then tried to create a custom lock by creating a zarr lock file on s3. Realistically, that should have
        #       worked. Not sure why it didnt? maybe I should try again, I may have done to many changes as the same time.
        #       would have to make sure that the first NetCDF is properly converted outside of the loop to make sure that
        #       the consecutive parallel task don't think it's an empty dataset.
        # TODO: my code seems to work fine in parallel instead of being sequential, however if too many tasks are put at once,
        #       , even like 20, everything seems to be very slow, hangs. I never have the patience to wait

        submit_tasks_in_batches(client, task, obj_ls, n_tasks)

        # Parallel Execution with List Comprehension
        # futures = [client.submit(task, f, i) for i, f in enumerate(obj_ls, start=1)]

        # Wait for all futures to complete
        # wait(futures)

        # Submit tasks to the Dask cluster sequentially
        # for i, f in enumerate(obj_ls, start=1):
        #     client.amm.start()
        #     future = client.submit(task, f, i)
        #     result = future.result()  # Sequential Execution with future.result()
        #     wait(future)
        #
        #     trim_future = client.submit(trim_memory)
        #     wait(trim_future)
        #
        #     restart_cluster_every_n = 100
        #     if i % 10 == restart_cluster_every_n:
        #         # Scale down to zero workers
        #         logger.info(
        #             f"Restarting workers after {i} iterations to avoid memory leaks"
        #         )
        #         cluster.scale(0)
        #
        #         # Wait for the workers to be removed
        #         wait_for_no_workers(client)
        #
        #         desired_n_workers = 1
        #         # Scale back up to the desired number of workers
        #         cluster.scale(desired_n_workers)
        #
        #         # Wait for the workers to be ready
        #         client.wait_for_workers(desired_n_workers)

    time_spent_processing = timeit.default_timer() - start_whole_processing
    logger.info(f"Whole dataset completed in {time_spent_processing}s")

    client.close()
    cluster.close()
