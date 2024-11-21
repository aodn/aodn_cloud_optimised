import importlib.resources
import os
import traceback
import uuid
import warnings
from functools import partial

import boto3
import fsspec
import numpy as np
import s3fs
import xarray as xr
import zarr
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from rechunker import rechunk
from xarray.core.merge import MergeError
from dask import array as da

from aodn_cloud_optimised.lib.CommonHandler import CommonHandler
from aodn_cloud_optimised.lib.logging import get_logger
from aodn_cloud_optimised.lib.s3Tools import (
    delete_objects_in_prefix,
    split_s3_path,
    prefix_exists,
    create_fileset,
)


def preprocess_xarray(ds, dataset_config):
    """
    Perform preprocessing on the input dataset (`ds`) and return an xarray Dataset.

    :param ds: Input xarray Dataset.
    :param dataset_config: Configuration dictionary for the dataset.

    :return:
        Preprocessed xarray Dataset.
    """
    # TODO: this is part a rewritten function available in the GenericHandler class below.
    #       running the class method with xarray as preprocess=self.preprocess_xarray lead to many issues
    #       1) serialization of the arguments with pickle.
    #       2) Running in a dask remote cluster, for some netcdf files, xarray would send the data back to the local machine
    #          and using ALL of the local ram. Complete nonsense. s3fs bug
    # https://github.com/fsspec/filesystem_spec/issues/1747
    # https://discourse.pangeo.io/t/remote-cluster-with-dask-distributed-uses-the-deployment-machines-memory-and-internet-bandwitch/4637
    # https://github.com/dask/distributed/discussions/8913
    logger_name = dataset_config.get("logger_name", "generic")
    dimensions = dataset_config.get("dimensions")
    schema = dataset_config.get("schema")

    logger = get_logger(logger_name)
    # TODO: get filename; Should be from https://github.com/pydata/xarray/issues/9142

    # ds = ds.assign(
    #     filename=((dimensions["time"]["name"],), [filename])
    # )  # add new filename variable with time dimension

    # Drop variables not in the list
    vars_to_drop = set(ds.data_vars) - set(schema)

    # Add variables with "drop_vars": true in the schema to vars_to_drop
    for var_name, var_details in schema.items():
        if var_details.get("drop_vars", False):
            vars_to_drop.add(var_name)

    ds_filtered = ds.drop_vars(vars_to_drop, errors="ignore")
    ds = ds_filtered

    if not ds.data_vars:
        logger.error(
            f"The dataset has no data variable left. Check the configuration dataset"
        )
        raise ValueError(
            f"The dataset has no data variable left. Check the configuration dataset"
        )

    ##########
    var_required = schema.copy()
    var_required.pop(dimensions["time"]["name"])
    var_required.pop(dimensions["latitude"]["name"])
    var_required.pop(dimensions["longitude"]["name"])

    # Remove variables with "drop_vars": true from the var_required list
    for var_name, var_details in schema.items():
        if var_details.get("drop_vars", False):
            var_required.pop(var_name, None)

    # TODO: make the variable below something more generic? a parameter?
    var_template_shape = dataset_config.get("var_template_shape")

    # retrieve filename from ds
    var = next(var for var in ds)
    try:
        filename = os.path.basename(ds[var].encoding["source"])
    except KeyError as e:
        logger.debug(f"Original filename not available in xarray dataset.\n {e}")
        filename = "UNKOWN_FILENAME.nc"

    logger.info(f"Applying preprocessing on dataset from {filename}")
    try:
        warnings.filterwarnings("error", category=RuntimeWarning)
        nan_array = da.full(ds[var_template_shape].shape, da.nan, dtype=da.float64)
        # the following commented line returned some RuntimeWarnings every now and then.
        # nan_array = np.empty(
        #    ds[var_template_shape].shape) * np.nan  # np.full_like( (1, 4500, 6000), np.nan, dtype=object)
    except RuntimeWarning as rw:
        raise TypeError

    for variable_name in var_required:
        datatype = var_required[variable_name].get("type")

        # if variable doesn't exist
        if variable_name not in ds:

            logger.warning(
                f"Add missing {variable_name} to xarray dataset with NaN values"
            )

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
                # ds[variable_name] = (
                #    (dimensions["time"]["name"],),
                #    empty_string_array,
                # )
                logger.warning(f"{variable_name} is not np.number")

            ds[variable_name] = ds[variable_name].astype(datatype)

        # if variable already exists
        else:

            if (datatype == "timestamp[ns]") or (
                datatype == "datetime64[ns]"
            ):  # Timestamps do not have an astype method. But numpy.datetime64 do.
                datatype = "datetime64[ns]"

            elif not np.issubdtype(datatype, np.number):
                # we repeat the string variable to match the size of the TIME dimension
                ds[variable_name] = (
                    (dimensions["time"]["name"],),
                    da.full_like(
                        ds[dimensions["time"]["name"]],
                        ds[variable_name],
                        dtype="<S1",
                    ),
                )

            ds[variable_name] = ds[variable_name].astype(datatype)

    logger.info(f"Successfully applied preprocessing to dataset from {filename}")
    return ds


class GenericHandler(CommonHandler):
    """
    GenericHandler to create cloud-optimised datasets in Zarr format.

    Inherits:
        CommonHandler: Provides common functionality for handling cloud-optimised datasets.
    """

    def __init__(self, **kwargs):
        """
        Initialise the GenericHandler object.

        Args:
            **kwargs: Additional keyword arguments.
                optimised_bucket_name (str, optional[config]): Name of the optimised bucket.
                root_prefix_cloud_optimised_path (str, optional[config]): Root Prefix path of the location of cloud optimised files

        Inherits:
            CommonHandler: Provides common functionality for handling cloud-optimised datasets.

        """
        super().__init__(**kwargs)

        json_validation_path = str(
            importlib.resources.files("aodn_cloud_optimised")
            .joinpath("config")
            .joinpath("schema_validation_zarr.json")
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

        # TODO: fix this ugly abomination. Unfortunately, patching the s3_fs value in the unittest is not enough for
        #       zarr! why? it works fine for parquet, but if I remove this if condition, my unittest breaks! maybe
        #       self.s3_fs is overwritten somewhere?? need to check
        if os.getenv("RUNNING_UNDER_UNITTEST") == "true":
            self.s3_fs = s3fs.S3FileSystem(
                anon=False,
                client_kwargs={
                    "endpoint_url": "http://127.0.0.1:5555/",
                    "region_name": "us-east-1",
                },
            )

        self.store = s3fs.S3Map(
            root=f"{self.cloud_optimised_output_path}", s3=self.s3_fs, check=False
        )

        # to_zarr common options
        self.safe_chunks = True
        self.write_empty_chunks = False
        self.consolidated = True

    def publish_cloud_optimised_fileset_batch(self, s3_file_uri_list):
        """
        Process and publish a batch of NetCDF files stored in S3 to a Zarr dataset.

        This method iterates over a list of S3 file URIs, processes them in batches, and publishes
        the resulting datasets to a Zarr store on S3. It performs the following steps:

        1. Validate input parameters and initialise logging.
        2. Create a list of file handles from S3 file URIs.
        3. Iterate through batches of file handles.
        4. Perform preprocessing on each batched dataset.
        5. Drop specified variables from the dataset based on schema settings.
        6. Open and preprocess each dataset using Dask for parallel processing.
        7. Chunk the dataset according to predefined dimensions.
        8. Write the processed dataset to an existing or new Zarr store on S3.
        9. Handle merging datasets and logging errors if encountered.

        Parameters:
        - s3_file_uri_list (list): List of S3 file URIs to process and publish.

        Raises:
        - ValueError: If input_objects (`s3_file_uri_list`) is not defined.

        Returns:
        None
        """
        # Iterate over s3_file_handle_list in batches
        if s3_file_uri_list is None:
            raise ValueError("input_objects is not defined")

        time_dimension_name = self.dimensions["time"]["name"]
        drop_vars_list = [
            var_name
            for var_name, attrs in self.schema.items()
            if attrs.get("drop_vars", False)
        ]

        partial_preprocess = partial(
            preprocess_xarray, dataset_config=self.dataset_config
        )

        if self.cluster_mode:
            batch_size = self.get_batch_size(client=self.client)
        else:
            batch_size = 1

        for idx, batch_uri_list in enumerate(
            self.batch_process_fileset(s3_file_uri_list, batch_size=batch_size)
        ):

            self.uuid_log = str(uuid.uuid4())  # value per batch

            self.logger.info(
                f"{self.uuid_log}: Listing all objects to process and creating a s3_file_handle_list"
            )

            batch_files = create_fileset(batch_uri_list, self.s3_fs)

            self.logger.info(f"{self.uuid_log}: Processing batch {idx + 1}...")
            self.logger.info(batch_files)

            if drop_vars_list:
                self.logger.warning(
                    f"{self.uuid_log}: Dropping variables: {drop_vars_list} from the dataset"
                )

            partial_preprocess_already_run = False

            try:
                # TODO(DONE): if using preprocess function within mfdataset (has to be outside the class otherwise
                #  parallelizing issues)

                try:
                    ds = self._open_mfds(
                        partial_preprocess,
                        drop_vars_list,
                        batch_files,
                    )
                    partial_preprocess_already_run = True

                except (ValueError, TypeError) as e:
                    self.logger.debug(
                        f'{self.uuid_log}: The default engine "h5netcdf" could not be used. Falling back '
                        f'to using "scipy" engine. This is an issue with old NetCDF files'
                    )

                    try:
                        ds = self._open_mfds(
                            partial_preprocess,
                            drop_vars_list,
                            batch_files,
                            engine="scipy",
                        )
                        partial_preprocess_already_run = True

                    except Exception as e:
                        self.logger.warning(
                            f'{self.uuid_log}: The engine "scipy" could not be used to concatenate the dataset together. '
                            f"likely because the files to concatenate are NetCDF3 and NetCDF4 and should use different engines. Falling back "
                            f"to opening the files individually with different engines"
                        )

                        # TODO: once xarray issue is fixed https://github.com/pydata/xarray/issues/8909,
                        #  the follwing code could probably be removed. Only scipy and h5netcdf can be used for remote access
                        try:
                            ds = self._concatenate_files_different_engines(
                                batch_files, partial_preprocess, drop_vars_list
                            )
                            self.logger.info(
                                f"{self.uuid_log}: Successfully Concatenating files together"
                            )
                            partial_preprocess_already_run = True
                            # if this works, we get out of this try except, and keep the processing as usual
                        except Exception as e:
                            # in this case, none of the above worked!! we try then to process each file one by one
                            # including writing to zarr, hence we do continue afterward to go to the next batch
                            self.logger.warning(
                                f"{self.uuid_log}: {e}.\n {traceback.format_exc()}"
                            )
                            self.logger.info(
                                f"{self.uuid_log}: None of the mfdataset with different engines worked, including concatenation. Falling back to processing files individually"
                            )

                            self._process_individual_file_fallback(
                                batch_files, partial_preprocess, drop_vars_list, idx
                            )
                            continue

                # If ds open with open_mfdataset with the partial preprocess_xarray, no need to re-run it again!
                if not partial_preprocess_already_run:
                    # TODO: can probably be removed as partial_preprocess_already_run should always be True now
                    self.logger.debug(
                        f"{self.uuid_log}: partial_preprocess_already_run is False"
                    )
                    ds = preprocess_xarray(ds, self.dataset_config)

                # Write the dataset to Zarr
                self._write_ds(ds, idx)

                self.logger.info(
                    f"{self.uuid_log}: Batch {idx + 1} successfully published to Zarr store: {self.store}"
                )

            except MergeError as e:
                self.logger.error(f"{self.uuid_log}: Failed to merge datasets: {e}")
                if "ds" in locals():
                    self.postprocess(ds)

            except Exception as e:
                self.logger.error(
                    f"{self.uuid_log}: An unexpected error occurred: {e}.\n {traceback.format_exc()}"
                )
                try:
                    self.logger.info(
                        f"{self.uuid_log}: None of the methods to process files as a batch worked. Falling back to "
                        f"processing files individually"
                    )
                    # in this case, none of the above worked!! we try then to process each file one by one
                    # including writing to zarr
                    self._process_individual_file_fallback(
                        batch_files, partial_preprocess, drop_vars_list, idx
                    )

                except Exception as e:
                    # Nothing could work. Workers failed ... Desperate times
                    self.logger.error(
                        f"{self.uuid_log}: An unexpected error occurred: {e}.\n {traceback.format_exc()}"
                    )

                if "ds" in locals():
                    self.postprocess(ds)

    def _handle_duplicate_regions(
        self,
        ds,
        idx,
        common_time_values,
        time_values_org,
        time_values_new,
    ):
        time_dimension_name = self.dimensions["time"]["name"]

        self.logger.info(
            f"{self.uuid_log}: Duplicate values of {self.dimensions['time']['name']} already existing in dataset. Overwriting"
        )
        # Get indices of common time values in the original dataset
        common_indices = np.nonzero(np.isin(time_values_org, common_time_values))[0]

        # regions must be CONTIGIOUS!! very important. so looking for different regions
        # Define regions as slices for the common time values
        regions = []
        matching_indexes = []

        start = common_indices[0]
        for i in range(1, len(common_indices)):
            if common_indices[i] != common_indices[i - 1] + 1:
                end = common_indices[i - 1]
                regions.append({time_dimension_name: slice(start, end + 1)})
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
        regions.append({time_dimension_name: slice(start, end + 1)})
        matching_indexes.append(
            np.where(
                np.isin(
                    time_values_new,
                    time_values_org[start : end + 1],
                )
            )[0]
        )

        n_region = 0
        for region, indexes in zip(regions, matching_indexes):
            self.logger.info(
                f"{self.uuid_log}: Region {n_region + 1} from Batch {idx + 1} - Overwriting Zarr dataset in Region: {region}, Matching Indexes in new ds: {indexes}"
            )

            ##########################################
            # extremely rare!! only happened once, and why??
            s = regions[0][time_dimension_name]
            region_num_elements = s.stop - s.start
            matching_indexes_array_size = np.size(matching_indexes[0])

            if matching_indexes_array_size != region_num_elements:
                amount_to_pad = region_num_elements - matching_indexes_array_size

                self.logger.error(
                    f"{self.uuid_log}: Duplicate values of {time_dimension_name} dimension "
                    f"found in original Zarr dataset. writing {matching_indexes_array_size} index value in a bigger region {region}. Trying to pad the dataset with"
                    f"an extra {amount_to_pad} index of NaN"
                )
            else:
                amount_to_pad = 0
                ds = ds.pad(time=(0, amount_to_pad))
            ##########################################
            for var in ds:
                if "chunks" in ds[var].encoding:
                    del ds[var].encoding["chunks"]

            # TODO:
            # compute() was added as unittests failed on github, but not locally. related to
            # https://github.com/pydata/xarray/issues/5219
            ds.isel(**{time_dimension_name: indexes}).drop_vars(
                self.vars_to_drop_no_common_dimension, errors="ignore"
            ).pad(**{time_dimension_name: (0, amount_to_pad)}).compute().to_zarr(
                self.store,
                write_empty_chunks=self.write_empty_chunks,
                region=region,
                compute=True,
                consolidated=self.consolidated,
                safe_chunks=self.safe_chunks,
                mode="r+",
            )
            self.logger.info(
                f"{self.uuid_log}: Region {n_region + 1} from Batch {idx + 1} - successfully published to {self.store}"
            )
            n_region += 1

        self.logger.info(
            f"{self.uuid_log}: All existing Regions from Batch {idx + 1} were successfully published to {self.store}"
        )
        self.logger.info(
            f"{self.uuid_log}: Looking for dataset indexes left to be published from Batch {idx + 1} to {self.store}"
        )
        # Now find the time values in ds that were NOT reprocessed
        # These are the time values not found in any of the common regions
        unprocessed_time_values = np.setdiff1d(time_values_new, common_time_values)

        # Return a dataset with the unprocessed time values
        if len(unprocessed_time_values) > 0:
            self.logger.info(
                f"{self.uuid_log}: Found indexes left to be published from Batch {idx + 1} to {self.store}"
            )
            ds_unprocessed = ds.sel({time_dimension_name: unprocessed_time_values})
            self._write_ds(ds_unprocessed, idx)
            return ds_unprocessed
        else:
            return None

    def _open_file_with_fallback(self, file, partial_preprocess, drop_vars_list):
        """Attempts to open a file using the specified engines.

        Tries to open the given file with the 'scipy' engine. If an exception occurs,
        it falls back to using the 'h5netcdf' engine. Logs success or failure messages
        accordingly.

        Args:
            file (str): The file path to be opened.
            partial_preprocess (bool): Flag indicating whether to apply partial preprocessing.
            drop_vars_list (list): List of variables to drop from the dataset.

        Returns:
            xr.Dataset: The opened dataset.

        Raises:
            Exception: Propagates any exceptions raised by the dataset opening operations.
        """
        try:
            with self.s3_fs.open(file, "rb") as f:  # Open the file-like object
                ds = self._open_ds(
                    f, partial_preprocess, drop_vars_list, engine="scipy"
                )
            self.logger.info(
                f"{self.uuid_log}: Success opening {file} with scipy engine."
            )
            return ds
        except (ValueError, TypeError) as e:
            self.logger.debug(
                f"{self.uuid_log}: Error opening {file}: {e} with scipy engine. Defaulting to h5netcdf"
            )
            ds = self._open_ds(
                file, partial_preprocess, drop_vars_list, engine="h5netcdf"
            )
            self.logger.info(
                f"{self.uuid_log}: Success opening {file} with h5netcdf engine."
            )
            return ds

    def _process_individual_file_fallback(
        self, batch_files, partial_preprocess, drop_vars_list, idx
    ):
        """Processes individual files from a batch, applying fallback mechanisms.

        Iterates over a batch of files, attempts to open each file using the
        '_open_file_with_fallback' method, and writes the resulting dataset to storage
        using the '_write_ds' method.
        """
        for file in batch_files:
            ds = self._open_file_with_fallback(file, partial_preprocess, drop_vars_list)
            self._write_ds(ds, idx)

    def _concatenate_files_different_engines(
        self, batch_files, partial_preprocess, drop_vars_list
    ):
        """Concatenates datasets opened from a batch of files using different engines.

        Attempts to open each file in the provided batch with the 'scipy' engine, falling
        back to the 'h5netcdf' engine if needed. Collects all datasets and concatenates them
        into a single dataset, logging progress throughout the process.

        Args:
            batch_files (list): A list of file paths to be concatenated.
            partial_preprocess (bool): Flag indicating whether to apply partial preprocessing.
            drop_vars_list (list): List of variables to drop from each dataset.

        Returns:
            xr.Dataset: A single concatenated dataset containing all successfully opened files.
        """
        datasets = []
        for file in batch_files:
            ds = self._open_file_with_fallback(file, partial_preprocess, drop_vars_list)
            datasets.append(ds)

        # Concatenate the datasets
        self.logger.info(
            f"{self.uuid_log}: Successfully read all files with different engines. Concatenating them together"
        )

        ds = xr.concat(
            datasets,
            compat="override",
            coords="minimal",
            data_vars="all",
            dim=self.dimensions["time"]["name"],
        )
        ds = ds.unify_chunks()
        # ds = ds.persist()
        self.logger.info(f"{self.uuid_log}: Successfully Concatenating files together")
        return ds

    def _open_mfds(
        self, partial_preprocess, drop_vars_list, batch_files, engine="h5netcdf"
    ):

        # Note:
        # if using preprocess within open_mfdataset, data_vars should probably be set to "minimal" as the preprocess
        # function will create the missing variables anyway. However I noticed that in some cases, some variables gets
        # "emptied" such as 20190205111000-ABOM-L3S_GHRSST-SSTfnd-MultiSensor-1d_dn_Southern.nc when processed within a
        # batch. Need to be careful
        # if preprocess is called after the open_mfdataset, then data_vars should probably be set to "all" as some variables
        # might be changed to NaN for a specific batch, if some variables aren't common to all NetCDF

        open_mfdataset_params = {
            "engine": engine,
            "parallel": True,
            "preprocess": partial_preprocess,  # this sometimes hangs the process. to monitor
            "data_vars": "all",
            # EXTREMELY IMPORTANT! Could lead to some variables being empty silently when writing to zarr
            "concat_characters": True,
            "mask_and_scale": True,
            "decode_cf": True,
            "decode_times": True,
            "use_cftime": True,
            "decode_coords": True,
            "compat": "override",
            "coords": "minimal",
            "drop_variables": drop_vars_list,
        }

        ds = xr.open_mfdataset(batch_files, **open_mfdataset_params)
        try:
            ds.chunk(chunks="auto")
        except Exception as err:
            self.logger.warning(
                f"{self.uuid_log}:{err}\n Defaulting to open files without auto chunks option"
            )
            ds.chunk(chunks=self.chunks)

        ds = ds.unify_chunks()
        # ds = ds.map_blocks(partial_preprocess) ## EXTREMELY DANGEROUS TO USE. CORRUPTS SOME DATA CHUNKS SILENTLY while it's working fine with preprocess
        # ds = ds.persist()

        return ds

    # @delayed  # to open and chunk each dataset lazily. cant work with xarray concatenation
    def _open_ds(self, file, partial_preprocess, drop_vars_list, engine="h5netcdf"):
        """Open and preprocess a single file as a xarray dataset.

        This method opens a dataset from a specified file using xarray and preprocesses it
        according to the provided configuration. It supports various engines for decoding
        and handling variables.

        Args:
            file (str): The file path or URI of the dataset to open.
            partial_preprocess (function): A function to preprocess the dataset, applied to
                the dataset after it is opened.
            drop_vars_list (list of str): A list of variable names to drop from the dataset.
            engine (str, optional): The engine to use for reading the dataset. Defaults to
                "h5netcdf".

        Returns:
            xr.Dataset: The opened and preprocessed xarray dataset.

        Note:
        This function can't have the @delayed decorator.
        The reason is that this function is used when we have to use xr.concatenate, which can't handle
        <class 'dask.delayed.Delayed'>.
        """

        open_dataset_params = {
            "engine": engine,
            "mask_and_scale": True,
            "decode_cf": True,
            "decode_times": True,
            "use_cftime": True,
            "decode_coords": True,
            "drop_variables": drop_vars_list,
        }

        ds = xr.open_dataset(file, **open_dataset_params)

        # got to do ds.chunks here as we can get a
        # *** NotImplementedError: Can not use auto rechunking with object dtype. We are unable to estimate the size in bytes of object data
        # for GSLA files
        # https://github.com/pydata/xarray/issues/4055
        try:
            ds.chunk(chunks="auto")
        except Exception as err:
            self.logger.warning(
                f"{self.uuid_log}:{err}\n Defaulting to open files without auto chunks option"
            )
            ds.chunk(chunks=self.chunks)

        # ds = ds.map_blocks(partial_preprocess)
        ds = ds.unify_chunks()

        ds = preprocess_xarray(ds, self.dataset_config)
        # ds = ds.persist()
        return ds

    def _find_duplicated_values(self, ds_org):
        # Find duplicates
        time_dimension_name = self.dimensions["time"]["name"]
        time_values_org = ds_org[time_dimension_name].values

        unique, counts = np.unique(time_values_org, return_counts=True)
        duplicates = unique[counts > 1]

        # Raise error if duplicates are found
        if len(duplicates) > 0:
            self.logger.error(
                f"{self.uuid_log}: Duplicate values of {time_dimension_name} dimension "
                f"found in original Zarr dataset. Could lead to a corrupted dataset: {duplicates}"
            )

        return True

    def _write_ds(self, ds, idx):
        time_dimension_name = self.dimensions["time"]["name"]
        ds = ds.sortby(time_dimension_name)
        ds = ds.chunk(chunks=self.chunks)

        # TODO: see https://github.com/pydata/xarray/issues/5219  https://github.com/pydata/xarray/issues/5286
        for var in ds:
            if "chunks" in ds[var].encoding:
                del ds[var].encoding["chunks"]

        # Write the dataset to Zarr
        if prefix_exists(self.cloud_optimised_output_path):
            self.logger.info(f"{self.uuid_log}: Appending data to existing Zarr")

            # NOTE: In the next section, we need to figure out if we're reprocessing existing data.
            #       For this, the logic is to open the original zarr store and compare with the new ds from
            #       this batch if they have time values in common.
            #       If this is the case, we need then to find the CONTIGUOUS regions as we can't assume that
            #       the data is well-ordered. The logic below is looking for the matching regions and indexes

            with xr.open_zarr(
                self.store,
                consolidated=True,
                decode_cf=True,
                decode_times=True,
                use_cftime=True,
                decode_coords=True,
            ) as ds_org:

                self._find_duplicated_values(ds_org)

                time_values_org = ds_org[time_dimension_name].values
                time_values_new = ds[time_dimension_name].values

                # Find common time values
                common_time_values = np.intersect1d(time_values_org, time_values_new)

                # Handle the 2 scenarios, reprocessing of a batch, or append new data
                if len(common_time_values) > 0:
                    self._handle_duplicate_regions(
                        ds, idx, common_time_values, time_values_org, time_values_new
                    )

                # No reprocessing needed
                else:
                    self._append_zarr_store(ds)

                    self.logger.info(
                        f"Batch {idx + 1} successfully published to {self.store}"
                    )

        # First time writing the dataset
        else:
            self._write_new_zarr_store(ds)

    def _write_new_zarr_store(self, ds):
        """
        Writes the dataset to a new Zarr store.
        """
        self.logger.info(f"{self.uuid_log}: Writing data to a new Zarr dataset.")
        ds.to_zarr(
            self.store,
            mode="w",  # Overwrite mode for the first batch
            write_empty_chunks=self.write_empty_chunks,
            compute=True,  # Compute the result immediately
            consolidated=self.consolidated,
            safe_chunks=self.safe_chunks,
        )

    def _append_zarr_store(self, ds):
        time_dimension_name = self.dimensions["time"]["name"]
        """
        Append the dataset to an existing Zarr store.
        """
        self.logger.info(f"{self.uuid_log}: Appending data to Zarr dataset")

        ds.to_zarr(
            self.store,
            mode="a",
            write_empty_chunks=self.write_empty_chunks,
            compute=True,  # Compute the result immediately
            consolidated=self.consolidated,
            append_dim=time_dimension_name,
            safe_chunks=self.safe_chunks,
        )

    def to_cloud_optimised(self, s3_file_uri_list=None):
        """
        Create a Zarr dataset from NetCDF data.

        This method creates a Zarr dataset from NetCDF data stored in S3. It logs the process,
        deletes existing Zarr objects if specified, processes multiple files concurrently using a cluster,
        and publishes the resulting datasets using the 'publish_cloud_optimised_fileset_batch' method.

        Note:

        Args:
        - s3_file_uri_list (list, optional): List of S3 file URIs to process and create the Zarr dataset.
                                             If not provided, no processing is performed.

        Returns:
        None
        """
        if self.clear_existing_data:
            self.logger.warning(
                f"Creating new Zarr dataset - DELETING existing all Zarr objects if they exist"
            )
            # TODO: delete all objects
            if prefix_exists(self.cloud_optimised_output_path):
                bucket_name, prefix = split_s3_path(self.cloud_optimised_output_path)
                self.logger.info(
                    f"Deleting existing Zarr objects from path: {self.cloud_optimised_output_path}"
                )

                delete_objects_in_prefix(bucket_name, prefix)

        # Multiple file processing with cluster
        if s3_file_uri_list is not None:
            s3_file_uri_list = list(
                dict.fromkeys(s3_file_uri_list)
            )  # ensure the list is unique!

            self.s3_file_uri_list = s3_file_uri_list

            if self.cluster_mode:
                # creating a cluster to process multiple files at once
                self.client, self.cluster = self.create_cluster()
                if self.cluster_mode == "coiled":
                    self.cluster_id = self.cluster.cluster_id
                else:
                    self.cluster_id = self.cluster.name
            else:
                self.cluster_id = "local_execution"

            self.publish_cloud_optimised_fileset_batch(s3_file_uri_list)

            if self.cluster_mode:
                self.cluster_manager.close_cluster(self.client, self.cluster)

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

            # s3 = s3fs.S3FileSystem(anon=False)

            org_url = (
                self.cloud_optimised_output_path
            )  # f's3://{self.optimised_bucket_name}/zarr/{self.dataset_name}.zarr'
            # org_store = s3fs.S3Map(root=f'{org_url}', s3=s3, check=False)

            target_url = org_url.replace(
                f"{self.dataset_name}", f"{self.dataset_name}_rechunked"
            )
            target_store = s3fs.S3Map(root=f"{target_url}", s3=self.s3_fs, check=False)
            # zarr.consolidate_metadata(org_store)

            ds = xr.open_zarr(fsspec.get_mapper(org_url, anon=True), consolidated=True)

            temp_url = org_url.replace(
                f"{self.dataset_name}", f"{self.dataset_name}_intermediate"
            )

            temp_store = s3fs.S3Map(root=f"{temp_url}", s3=self.s3_fs, check=False)

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
            self.logger.info(
                f"Rechunking in progress with target chunks: {target_chunks}"
            )

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
