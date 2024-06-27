import importlib.resources
import os
import timeit
import traceback
import warnings

import boto3
import dask
import fsspec
import numpy as np
import s3fs
import xarray as xr
import zarr
from coiled import Cluster
from dask.diagnostics import ProgressBar
from dask.distributed import Client, Lock, get_client, wait
from dask.distributed import LocalCluster
from rechunker import rechunk
from xarray.core.merge import MergeError

from functools import partial, partialmethod

from ..lib.s3Tools import delete_objects_in_prefix, split_s3_path
from .CommonHandler import CommonHandler
from .logging import get_logger


def preprocess_xarray_no_class(ds, dataset_config):  # , filename):
    logger_name = dataset_config.get("logger_name", "generic")
    dimensions = dataset_config.get("dimensions")
    schema = dataset_config.get("schema")

    logger = get_logger(logger_name)

    # TODO: get filename; Should be from https://github.com/pydata/xarray/issues/9142

    # ds = ds.assign(
    #     filename=((dimensions["time"]["name"],), [filename])
    # )  # add new filename variable with time dimension

    vars_to_drop = set(ds.data_vars) - set(schema)
    ds_filtered = ds.drop_vars(vars_to_drop)
    ds = ds_filtered

    var_required = schema.copy()
    var_required.pop(dimensions["time"]["name"])
    var_required.pop(dimensions["latitude"]["name"])
    var_required.pop(dimensions["longitude"]["name"])

    # TODO: make the variable below something more generic? a parameter?
    var_template_shape = dataset_config.get("var_template_shape")

    try:
        warnings.filterwarnings("error", category=RuntimeWarning)
        nan_array = np.full(ds[var_template_shape].shape, np.nan, dtype=np.float64)
        # the following commented line returned some RuntimeWarnings every now and then.
        # nan_array = np.empty(
        #    ds[var_template_shape].shape) * np.nan  # np.full_like( (1, 4500, 6000), np.nan, dtype=object)
    except RuntimeWarning as rw:
        raise TypeError

    for variable_name in var_required:
        datatype = var_required[variable_name].get("type")

        # if variable doesn't exist
        if variable_name not in ds:
            # logger.warning(
            #     f"{filename}: add missing {variable_name} to xarray dataset"
            # )
            # logger.warning(
            #     f"add missing {variable_name} to xarray dataset"
            # )

            # check the type of the variable (numerical of string)
            if np.issubdtype(datatype, np.number):
                # Add the missing variable  to the dataset
                ds[variable_name] = (
                    (
                        dimensions["time"]["name"],
                        dimensions["latitude"]["name"],
                        dimensions["longitude"]["name"],
                    ),
                    nan_array,
                )

            else:
                # for strings variables, it's quite likely that the variables don't have any dimensions associated.
                # This can be an issue to know which datapoint is associated to which string value.
                # In this case we might have to repeat the string the times of ('TIME')
                ds[variable_name] = (
                    (dimensions["time"]["name"],),
                    empty_string_array,
                )

            ds[variable_name] = ds[variable_name].astype(datatype)

        # if variable already exists
        else:

            if (
                datatype == "timestamp[ns]"
            ):  # Timestamps do not have an astype method. But numpy.datetime64 do.
                datatype = "datetime64[ns]"

            elif not np.issubdtype(datatype, np.number):
                # we repeat the string variable to match the size of the TIME dimension
                ds[variable_name] = (
                    (dimensions["time"]["name"],),
                    np.full_like(
                        ds[dimensions["time"]["name"]],
                        ds[variable_name],
                        dtype="<S1",
                    ),
                )

            ds[variable_name] = ds[variable_name].astype(datatype)

    return ds


class GenericHandler(CommonHandler):
    def __init__(self, **kwargs):
        """
        Initialise the GenericHandler object.

        Args:
            **kwargs: Additional keyword arguments.
                raw_bucket_name (str, optional[config]): Name of the raw bucket.
                optimised_bucket_name (str, optional[config]): Name of the optimised bucket.
                root_prefix_cloud_optimised_path (str, optional[config]): Root Prefix path of the location of cloud optimised files
                input_object_key (str): Key of the input object.
        """
        super().__init__(**kwargs)

        json_validation_path = str(
            importlib.resources.path(
                "aodn_cloud_optimised.config", "schema_validation_zarr.json"
            )
        )
        self.validate_json(
            json_validation_path
        )  # we cannot validate the json config until self.dataset_config and self.logger are set

        self.dimensions = self.dataset_config.get("dimensions")
        self.rechunk_drop_vars = kwargs.get("rechunk_drop_vars", None)
        self.vars_to_drop_no_common_dimension = self.dataset_config.get(
            "vars_to_drop_no_common_dimension", None
        )

        self.chunks = {
            self.dimensions["time"]["name"]: self.dimensions["time"]["chunk"],
            self.dimensions["latitude"]["name"]: self.dimensions["latitude"]["chunk"],
            self.dimensions["longitude"]["name"]: self.dimensions["longitude"]["chunk"],
        }

        self.compute = bool(True)

        self.s3 = s3fs.S3FileSystem(anon=False)

        self.store = s3fs.S3Map(
            root=f"{self.cloud_optimised_output_path}", s3=self.s3, check=False
        )

    def acquire_dask_lock(self):
        """
        Acquire a Dask distributed lock to ensure exclusive access to a shared resource.

        This method attempts to acquire a named Dask lock to prevent concurrent access to a
        shared resource, such as writing to a Zarr dataset. If a Dask client is available,
        it will obtain the lock, blocking if necessary until the lock becomes available.
        If no Dask client is available, it will set the lock attribute to None and log a warning.
        """
        lock_name = "zarr_write_lock"

        # Attempt to get the Dask client
        try:
            self.dask_client = get_client()  # Get the Dask client
            self.logger.info(f"Acquired Dask Client: {self.dask_client}")
        except ValueError:
            self.dask_client = None  # Set to None if no Dask client is found
            self.lock = None  # Set to None if no Dask cluster is found
            self.logger.warning("Dask Client not found. No cluster lock to setup")

        if self.dask_client:
            lock = Lock(name=lock_name, client=self.dask_client)
            lock.acquire(
                blocking=False
            )  #  https://docs.python.org/3/library/threading.html#threading.Lock.acquire
            # When invoked with the blocking argument set to True (the default), block until the lock is unlocked, then set it to locked and return True.

            self.logger.info(f"Lock '{lock_name}' acquired successfully.")
            self.lock = lock

    def release_lock(self):
        """
        Release the currently held Dask lock.

        This method releases the Dask lock previously acquired by the `acquire_dask_lock` method.
        If the lock is held, it will be released and an info message will be logged. If the lock
        is not held, it will log an info message indicating that no lock is held.
        """
        if self.lock:
            # if self.lock.locked():
            self.lock.release()
            self.logger.info("Lock released.")
            self.lock = None
        else:
            self.logger.info("No lock is held.")

    def check_file_already_processed(self) -> bool:
        """
        Check whether a NetCDF file has been previously processed and integrated into an existing Zarr dataset.
        This check is performed by examining the filename variable added to the Zarr dataset.

        If the file has been processed previously, a self.reprocessed_time_idx variable will be created
        to determine the index value of the time variable region for potential overwriting.

        :returns:
            - True if the filename has already been integrated.
            - False if the filename has not been integrated yet.
        """
        self.logger.info(
            f"{self.filename}: Checking if input NetCDF has already been ingested into Zarr dataset"
        )

        # Load existing zarr dataset
        try:
            ds = xr.open_zarr(
                fsspec.get_mapper(self.cloud_optimised_output_path, anon=True),
                consolidated=True,
            )
        except Exception as e:
            self.logger.warning(f"Zarr dataset does not exist")
            return False

        # TODO: Fix this as filename might not be a variable anymore. Solution is to load the org zarr dataset and look
        #       for time index where the values are similar
        # Locate values of time indexes where new filename has possibly been already downloaded
        idx = ds.indexes[self.dimensions["time"]["name"]].where(
            ds.filename == self.filename
        )
        not_nan_mask = ~idx.isna()  # ~np.isnan(idx)

        # Use numpy.where to get the indices where the values are not NaN
        indices_not_nan = np.where(not_nan_mask)[0]
        if indices_not_nan.size == 1:  # filename exists, file part of existing zarr
            self.reprocessed_time_idx = indices_not_nan[0]
            return True

        elif indices_not_nan.size == 0:
            return False

    def preprocess_xarray(self, ds) -> xr.Dataset:
        """
        Perform preprocessing on the input dataset (`ds`) and return an xarray Dataset.

        :param ds: Input xarray Dataset.
        :param filename: Name of the file being processed.

        :return:
            Preprocessed xarray Dataset.
        """

        # Drop variables not in the list
        # TODO: add a warning message
        vars_to_drop = set(ds.data_vars) - set(self.schema)
        ds_filtered = ds.drop_vars(vars_to_drop)
        ds = ds_filtered

        # https://github.com/pydata/xarray/issues/2313
        # filename = ds.encoding["source"]

        # self.logger.info(f"{filename}: xarray preprocessing")

        # Add a new dimension 'filename' with a filename value
        filename = None
        if filename is not None:
            ds = ds.assign(
                filename=((self.dimensions["time"]["name"],), [filename])
            )  # add new filename variable with time dimension

        var_required = self.schema.copy()
        var_required.pop(self.dimensions["time"]["name"])
        var_required.pop(self.dimensions["latitude"]["name"])
        var_required.pop(self.dimensions["longitude"]["name"])

        # TODO: make the variable below something more generic? a parameter?
        var_template_shape = self.dataset_config.get("var_template_shape")

        try:
            warnings.filterwarnings("error", category=RuntimeWarning)
            nan_array = np.full(ds[var_template_shape].shape, np.nan, dtype=np.float64)
            # the following commented line returned some RuntimeWarnings every now and then.
            # nan_array = np.empty(
            #    ds[var_template_shape].shape) * np.nan  # np.full_like( (1, 4500, 6000), np.nan, dtype=object)
        except RuntimeWarning as rw:
            raise TypeError

        for variable_name in var_required:
            datatype = var_required[variable_name].get("type")

            # if variable doesn't exist
            if variable_name not in ds:
                # self.logger.warning(
                #     f"{filename}: add missing {variable_name} to xarray dataset"
                # )
                self.logger.warning(f"add missing {variable_name} to xarray dataset")

                # check the type of the variable (numerical of string)
                if np.issubdtype(datatype, np.number):
                    # Add the missing variable  to the dataset
                    ds[variable_name] = (
                        (
                            self.dimensions["time"]["name"],
                            self.dimensions["latitude"]["name"],
                            self.dimensions["longitude"]["name"],
                        ),
                        nan_array,
                    )

                else:
                    # for strings variables, it's quite likely that the variables don't have any dimensions associated.
                    # This can be an issue to know which datapoint is associated to which string value.
                    # In this case we might have to repeat the string the times of ('TIME')
                    ds[variable_name] = (
                        (self.dimensions["time"]["name"],),
                        empty_string_array,
                    )

                ds[variable_name] = ds[variable_name].astype(datatype)

            # if variable already exists
            else:

                if (
                    datatype == "timestamp[ns]"
                ):  # Timestamps do not have an astype method. But numpy.datetime64 do.
                    datatype = "datetime64[ns]"

                elif not np.issubdtype(datatype, np.number):
                    # we repeat the string variable to match the size of the TIME dimension
                    ds[variable_name] = (
                        (self.dimensions["time"]["name"],),
                        np.full_like(
                            ds[self.dimensions["time"]["name"]],
                            ds[variable_name],
                            dtype="<S1",
                        ),
                    )

                ds[variable_name] = ds[variable_name].astype(datatype)

        return ds

    def batch_process_fileset(self, fileset, batch_size=10):
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

    def publish_cloud_optimised_fileset_batch(self):
        """

        Returns:

        """
        # Iterate over fileset in batches
        if self.input_object_keys is None:
            raise ValueError("input_object_keys is not defined")

        fileset = self.create_fileset(self.raw_bucket_name, self.input_object_keys)

        for idx, batch_files in enumerate(self.batch_process_fileset(fileset)):
            self.logger.info(f"Processing batch {idx + 1}...")
            self.logger.info(batch_files)

            batch_filenames = [os.path.basename(f.full_name) for f in batch_files]

            partial_reprocess = partial(
                preprocess_xarray_no_class, dataset_config=self.dataset_config
            )  # , filename=batch_filenames)

            drop_vars_list = [
                var_name
                for var_name, attrs in self.schema.items()
                if attrs.get("drop_vars", False)
            ]
            self.logger.warning(f"Dropping variables {drop_vars_list} from dataset")

            with dask.config.set(
                **{
                    "array.slicing.split_large_chunks": False,
                    "distributed.scheduler.worker-saturation": "inf",
                }
            ):
                try:
                    # TODO: if using preprocess function within mfdataset (has to be outside the class otherwise parallelizing issues), the
                    #       local ram is being used! and not the cluster one! even if the function only does return ds
                    #       solution, open at the end with ds = preprocess(ds) afterwards
                    #
                    ds = xr.open_mfdataset(
                        batch_files,
                        engine="h5netcdf",
                        parallel=True,
                        # preprocess=partial_reprocess, # this sometimes hangs the process
                        concat_characters=True,
                        mask_and_scale=True,
                        decode_cf=True,
                        decode_times=True,
                        use_cftime=True,
                        decode_coords=True,
                        compat="override",
                        coords="minimal",
                        data_vars="minimal",
                        drop_variables=drop_vars_list,
                    )

                    # TODO: create a simple jupyter notebook 2 show 2 different problems:
                    #       1) serialization issue if preprocess is within a class
                    #       2) blowing of memory if preprocess function is outside of a class and only does return ds

                    ds = preprocess_xarray_no_class_wip(ds, self.dataset_config)

                    # NOTE: if I comment the next line, i get some errors with the latest chunk for some variables
                    ds = ds.chunk(
                        chunks=self.chunks
                    )  # careful with chunk size, had an issue

                    # Write the dataset to Zarr
                    if self.prefix_exists(self.cloud_optimised_output_path):
                        self.logger.info(f"append data to existing Zarr")

                        # NOTE: In the next section, we need to figure out if we're reprocessing existing data.
                        #       For this, the logic is open the original zarr store and compare with the new ds from
                        #       this batch if they have time values in common.
                        #       If this is the case, we need then to find the CONTIGUOUS regions as we can't assume that
                        #       the data is well ordered. The logic below is looking for the matching regions and indexes

                        ds_org = xr.open_zarr(
                            self.store,
                            consolidated=True,
                            decode_cf=True,
                            decode_times=True,
                            use_cftime=True,
                            decode_coords=True,
                        )

                        time_values_org = ds_org[self.dimensions["time"]["name"]].values
                        time_values_new = ds[self.dimensions["time"]["name"]].values

                        # Find common time values
                        common_time_values = np.intersect1d(
                            time_values_org, time_values_new
                        )

                        # Handle the 2 scenarios, reprocessing of a batch, or append new data
                        if len(common_time_values) > 0:
                            self.logger.info(
                                f"Duplicate values of {self.dimensions['time']['name']}"
                            )
                            # Get indices of common time values in the original dataset
                            common_indices = np.nonzero(
                                np.isin(time_values_org, common_time_values)
                            )[0]

                            # regions must be CONTIGIOUS!! very important. so looking for different regions
                            # Define regions as slices for the common time values
                            regions = []
                            matching_indexes = []

                            start = common_indices[0]
                            for i in range(1, len(common_indices)):
                                if common_indices[i] != common_indices[i - 1] + 1:
                                    end = common_indices[i - 1]
                                    regions.append(
                                        {
                                            self.dimensions["time"]["name"]: slice(
                                                start, end + 1
                                            )
                                        }
                                    )
                                    matching_indexes.append(
                                        np.where(
                                            np.isin(
                                                time_values_new,
                                                time_values_org[start : end + 1],
                                            )
                                        )[0]
                                    )
                                    start = common_indices[i]

                            # Append the last region
                            end = common_indices[-1]
                            regions.append(
                                {self.dimensions["time"]["name"]: slice(start, end + 1)}
                            )
                            matching_indexes.append(
                                np.where(
                                    np.isin(
                                        time_values_new,
                                        time_values_org[start : end + 1],
                                    )
                                )[0]
                            )

                            # Process region by region if necessary
                            for region, indexes in zip(regions, matching_indexes):
                                self.logger.info(
                                    f"Overwriting zarr in Region: {region}, Matching Indexes in new ds: {indexes}"
                                )
                                ds.isel(time=indexes).drop_vars(
                                    self.vars_to_drop_no_common_dimension
                                ).to_zarr(
                                    self.store,
                                    write_empty_chunks=False,
                                    region=region,
                                    compute=True,
                                    consolidated=True,
                                )

                        # No reprocessing needed
                        else:
                            ds.to_zarr(
                                self.store,
                                mode="a",  # append mode for the next batches
                                write_empty_chunks=False,  # TODO: could True fix the issue when some variables dont exists? I doubt
                                compute=True,  # Compute the result immediately
                                consolidated=True,
                                append_dim=self.dimensions["time"]["name"],
                            )

                    # First time writing the dataset
                    else:
                        self.logger.info(f"Writing data to new Zarr dataset")

                        ds.to_zarr(
                            self.store,
                            mode="w",  # Overwrite mode for the first batch
                            write_empty_chunks=False,
                            compute=True,  # Compute the result immediately
                            consolidated=True,
                        )

                    self.logger.info(
                        f"Batch {idx + 1} processed and written to {self.store}"
                    )

                except MergeError as e:
                    self.logger.error(f"Failed to merge datasets: {e}")

                except Exception as e:
                    self.logger.error(f"An unexpected error occurred: {e}")

    def preprocess_single_file(self, fileset) -> xr.Dataset:
        """
        Create a dataframe and xarray data from a NetCDF file. Loaded in memory.

        :return:
            ds: xarray Dataset.
        """

        # TODO:  be careful here. needs to be rewritten to have the 2 cases, single file, or list of files
        # in order to allow paraellel=True, all the workers need to have access to the file. The file can't be local
        # as not all the workers will have it.
        # to fix this, we're opening the file with fsspec

        # if self.input_object_key is None:
        #     raise ValueError("input_object_key is not defined")

        # fileset = self.create_fileset(self.raw_bucket_name, [self.input_object_key])

        ds = xr.open_dataset(
            fileset,
            engine="h5netcdf",
            concat_characters=True,
            mask_and_scale=True,
            decode_cf=True,
            decode_times=True,
            use_cftime=True,
            # autoclose=True,
            decode_coords=True,
        )

        filename = os.path.basename(fileset.full_name)
        ds = self.preprocess_xarray(ds, filename)

        return ds

    def publish_cloud_optimised_single_file(self, ds):
        """
        Create or update a Zarr dataset in the specified S3 bucket.

        :param ds: The xarray dataset to be stored in Zarr format.
        :type ds: xr.Dataset

        :return: None
        """

        ds = ds.chunk(chunks=self.chunks)

        if self.prefix_exists(self.cloud_optimised_output_path):

            self.logger.info(f"{self.filename}: append data to existing Zarr")

            # case when a file should be reprocessed and writen to a specific region
            if self.check_file_already_processed():
                self.logger.info(
                    f"{self.filename}: update time region at slice({self.reprocessed_time_idx} , {self.reprocessed_time_idx + 1}) with new NetCDF data"
                )
                # when setting `region` explicitly in to_zarr(), all variables in the dataset to write
                # must have at least one dimension in common with the region's dimensions ['TIME'],
                # but that is not the case for some variables here. To drop these variables
                # from this dataset before exporting to zarr, write:
                # .drop_vars(['LATITUDE', 'LONGITUDE', 'GDOP'])

                write_job = ds.drop_vars(self.vars_to_drop_no_common_dimension).to_zarr(
                    self.store,
                    write_empty_chunks=False,
                    region={
                        self.dimensions["time"]["name"]: slice(
                            self.reprocessed_time_idx, self.reprocessed_time_idx + 1
                        )
                    },
                    compute=self.compute,
                    consolidated=True,
                )
            else:
                write_job = ds.to_zarr(
                    self.store,
                    write_empty_chunks=False,
                    mode="a",
                    compute=self.compute,
                    append_dim=self.dimensions["time"]["name"],
                    consolidated=True,
                )
        else:
            self.logger.info(f"{self.filename}: Write data to new Zarr dataset")

            write_job = ds.to_zarr(
                self.store,
                write_empty_chunks=False,
                mode="w",
                compute=self.compute,
                consolidated=True,
            )
            self.logger.info(f"{self.filename}: Writen data to new Zarr dataset")

        self.logger.info(
            f"{self.filename}: Zarr created and pushed to {self.cloud_optimised_output_path} successfully"
        )

    def to_cloud_optimised(self):
        """
        Create a Zarr dataset from NetCDF data.

        Returns:
        None

        This method creates a Zarr dataset from NetCDF data. It logs the process,
        creates a dataset using the 'preprocess' method, and populates the Zarr dataset
        using the 'publish_cloud_optimised' method. After completion, the temporary NetCDF file
        is removed. The total time taken for the operation is logged.

        Note: The 'preprocess' and 'publish_cloud_optimised' methods are assumed to be defined within the class.
        """
        if self.clear_existing_data:
            self.logger.warning(
                f"Creating new Zarr dataset - DELETING existing all Zarr objects if exist"
            )
            # TODO: delete all objects
            if self.prefix_exists(self.cloud_optimised_output_path):
                bucket_name, prefix = split_s3_path(self.cloud_optimised_output_path)
                self.logger.info(
                    f"Deleting existing Zarr objects from {self.cloud_optimised_output_path}"
                )

                delete_objects_in_prefix(bucket_name, prefix)

        # Multiple file processing with cluster
        if self.input_object_keys is not None:
            # creating a cluster to process multiple files at once
            self.create_cluster()
            self.publish_cloud_optimised_fileset_batch()
            self.close_cluster()

        elif self.input_object_key is not None:

            try:
                fileset = self.create_fileset(
                    self.raw_bucket_name, [self.input_object_key]
                )
                ds = self.preprocess_single_file(fileset)

                try:
                    lock = Lock(name="zarr_lock", client=get_client())
                    self.logger.info(f"Get lock from Client {lock}")
                    with lock:
                        self.publish_cloud_optimised_single_file(ds)
                except:
                    self.logger.info("No existing Dask client to set up a lock")
                    self.publish_cloud_optimised_single_file(ds)

                self.push_metadata_aws_registry()

                time_spent = timeit.default_timer() - self.start_time
                self.logger.info(
                    f"Cloud Optimised file of {self.input_object_key} completed in {time_spent}s"
                )

                self.postprocess(ds)

            except Exception as e:
                self.logger.error(
                    f"Issue while creating Cloud Optimised file: {type(e).__name__}: {e} \n {traceback.print_exc()}"
                )

                if "ds" in locals():
                    self.postprocess(ds)

    @staticmethod
    def filter_rechunk_dimensions(dimensions):
        """
        Filter dimensions dictionary based on the 'rechunk' key.

        Parameters:
        - dimensions (dict): A dictionary containing dimensions information.

        Returns:
        - rechunk_dimensions (dict): A filtered dictionary containing keys where 'rechunk' is True,
                                    along with their corresponding 'chunk' values.
        """
        rechunk_dimensions = {}

        for key, value in dimensions.items():
            if "rechunk" in value and value["rechunk"]:
                rechunk_dimensions[value["name"]] = value["chunk"]

        return rechunk_dimensions

    def rechunk(self, max_mem="8.0GB"):
        """
        Rechunk a Zarr dataset stored on S3.

        Parameters:
        - max_mem (str, optional): Maximum memory to use during rechunking (default is '8.0GB').

        Returns:
        None

        Example:
        >>> gridded_handler_instance.rechunk(max_mem='12.0GB')

        This method rechunks a Zarr dataset stored on S3 using Dask and the specified target chunks.
        The rechunked data is stored in a new Zarr dataset on S3.
        The intermediate and previous rechunked data are deleted before the rechunking process.
        The 'max_mem' parameter controls the maximum memory to use during rechunking.

        Note: The target chunks for rechunking are determined based on the dimensions and chunking information.
        """
        # with worker_client():
        with Client() as client:

            target_chunks = self.filter_rechunk_dimensions(
                self.dimensions
            )  # only return a dict with the dimensions to rechunk

            s3 = s3fs.S3FileSystem(anon=False)

            org_url = (
                self.cloud_optimised_output_path
            )  # f's3://{self.optimised_bucket_name}/zarr/{self.dataset_name}.zarr'
            # org_store = s3fs.S3Map(root=f'{org_url}', s3=s3, check=False)

            target_url = org_url.replace(
                f"{self.dataset_name}", f"{self.dataset_name}_rechunked"
            )
            target_store = s3fs.S3Map(root=f"{target_url}", s3=s3, check=False)
            # zarr.consolidate_metadata(org_store)

            ds = xr.open_zarr(fsspec.get_mapper(org_url, anon=True), consolidated=True)

            temp_url = org_url.replace(
                f"{self.dataset_name}", f"{self.dataset_name}_intermediate"
            )

            temp_store = s3fs.S3Map(root=f"{temp_url}", s3=s3, check=False)

            # delete previous version of intermediate and rechunked data
            s3_client = boto3.resource("s3")
            bucket = s3_client.Bucket(f"{self.optimised_bucket_name}")
            self.logger.info("Delete previous rechunked version in progress")

            bucket.objects.filter(
                Prefix=temp_url.replace(f"s3://{self.optimised_bucket_name}/", "")
            ).delete()
            bucket.objects.filter(
                Prefix=target_url.replace(f"s3://{self.optimised_bucket_name}/", "")
            ).delete()
            self.logger.info(f"Rechunking in progress with chunks: {target_chunks}")

            options = dict(overwrite=True)
            array_plan = rechunk(
                ds,
                target_chunks,
                max_mem,
                target_store,
                temp_store=temp_store,
                temp_options=options,
                target_options=options,
            )
            with ProgressBar():
                self.logger.info(f"{array_plan.execute()}")

            zarr.consolidate_metadata(target_store)
