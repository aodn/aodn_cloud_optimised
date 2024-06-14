import importlib.resources
import timeit
import traceback
from functools import partial

import boto3
import fsspec
import numpy as np
import s3fs
import xarray as xr
import zarr
import time

from dask.diagnostics import ProgressBar
from dask.distributed import Client, Lock, wait, get_client, worker_client

from rechunker import rechunk

from .CommonHandler import CommonHandler


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

        self.reprocess = kwargs.get(
            "reprocess", None
        )  # setting to True will recreate the zarr from scratch at every run!

        self.dimensions = self.dataset_config.get("dimensions")
        self.rechunk_drop_vars = kwargs.get("rechunk_drop_vars", None)
        self.vars_to_drop_no_common_dimension = self.dataset_config.get(
            "vars_to_drop_no_common_dimension", None
        )

        self.chunks = {
            self.dimensions["latitude"]["name"]: self.dimensions["latitude"]["chunk"],
            self.dimensions["longitude"]["name"]: self.dimensions["longitude"]["chunk"],
        }

        self.compute = bool(True)

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
                blocking=True
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
            if self.lock.locked():
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

    def preprocess_xarray(self, ds, filename) -> xr.Dataset:
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

        # add a new filename variable
        filename = self.filename

        self.logger.info(f"{self.filename}: xarray preprocessing")

        # Add a new dimension 'filename' with a filename value
        ds = ds.assign(
            filename=((self.dimensions["time"]["name"],), [filename])
        )  # add new filename variable with time dimension

        var_required = self.schema.copy()
        var_required.pop(self.dimensions["time"]["name"])
        var_required.pop(self.dimensions["latitude"]["name"])
        var_required.pop(self.dimensions["longitude"]["name"])

        # TODO: make the variable below something more generic? a parameter?
        var_template_shape = self.dataset_config.get("var_template_shape")

        import warnings

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
                self.logger.warning(
                    f"{self.filename}: add missing {variable_name} to xarray dataset"
                )

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

    def preprocess(self) -> xr.Dataset:
        """
        Create a dataframe and xarray data from a NetCDF file. Loaded in memory.

        :return:
            ds: xarray Dataset.
        """

        preproc = partial(self.preprocess_xarray, filename=self.filename)
        ds = xr.open_mfdataset(
            self.tmp_input_file,
            preprocess=preproc,
            engine="h5netcdf",
            concat_characters=True,
            mask_and_scale=True,
            decode_cf=True,
            decode_times=True,
            use_cftime=True,
            parallel=True,
            # autoclose=True,
            decode_coords=True,
        )

        return ds

    def publish_cloud_optimised(self, ds):
        """
        Create or update a Zarr dataset in the specified S3 bucket.

        :param ds: The xarray dataset to be stored in Zarr format.
        :type ds: xr.Dataset

        :return: None
        """
        s3 = s3fs.S3FileSystem(anon=False)

        store = s3fs.S3Map(
            root=f"{self.cloud_optimised_output_path}", s3=s3, check=False
        )

        ds = ds.chunk(chunks=self.chunks)

        # first file of the dataset (overwrite)
        if self.reprocess:
            self.logger.warning(
                f"{self.filename}: Creating new Zarr dataset - OVERWRITTING existing all Zarr objects if exist"
            )

            write_job = ds.to_zarr(
                store,
                write_empty_chunks=False,
                mode="w",
                compute=self.compute,
                consolidated=True,
            )

        # append new files to the dataset
        else:
            if self.prefix_exists(self.cloud_optimised_output_path):

                self.logger.info(f"{self.filename}: append data to existing Zarr")
                if (
                    self.check_file_already_processed()
                ):  # case when a file should be reprocessed and write to a specific region
                    self.logger.info(
                        f"{self.filename}: update time region at slice({self.reprocessed_time_idx} , {self.reprocessed_time_idx + 1}) with new NetCDF data"
                    )
                    # when setting `region` explicitly in to_zarr(), all variables in the dataset to write
                    # must have at least one dimension in common with the region's dimensions ['TIME'],
                    # but that is not the case for some variables here. To drop these variables
                    # from this dataset before exporting to zarr, write:
                    # .drop_vars(['LATITUDE', 'LONGITUDE', 'GDOP'])

                    write_job = ds.drop_vars(
                        self.vars_to_drop_no_common_dimension
                    ).to_zarr(
                        store,
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
                        store,
                        write_empty_chunks=False,
                        mode="a",
                        compute=self.compute,
                        append_dim=self.dimensions["time"]["name"],
                        consolidated=True,
                    )
            else:
                self.logger.info(f"{self.filename}: Write data to new Zarr dataset")

                write_job = ds.to_zarr(
                    store,
                    write_empty_chunks=False,
                    mode="w",
                    compute=self.compute,
                    consolidated=True,
                )
        # write_job = write_job.persist()
        # distributed.progress(write_job, notebook=False)
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

        if self.tmp_input_file.endswith(".nc"):
            self.is_valid_netcdf(
                self.tmp_input_file
            )  # check file validity before doing anything else

        try:
            ds = self.preprocess()

            # Attempt to acquire the zarr lock
            self.acquire_dask_lock()
            # Critical section - perform operations protected by the lock
            self.publish_cloud_optimised(ds)
            # Release the lock
            self.release_lock()

            self.push_metadata_aws_registry()

            time_spent = timeit.default_timer() - self.start_time
            self.logger.info(f"Cloud Optimised file completed in {time_spent}s")

            self.postprocess(ds)

        except Exception as e:
            self.logger.error(
                f"Issue while creating Cloud Optimised file: {type(e).__name__}: {e} \n {traceback.print_exc()}"
            )

            if "ds" in locals():
                self.postprocess(ds)
        finally:
            self.release_lock()

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
