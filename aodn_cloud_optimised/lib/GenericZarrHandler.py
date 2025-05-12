import importlib.resources
import os
import re
import traceback
import uuid
import warnings
from functools import partial

import numpy as np
import s3fs
import xarray as xr
from dask import array as da
from xarray.core.merge import MergeError

from aodn_cloud_optimised.lib.CommonHandler import CommonHandler
from aodn_cloud_optimised.lib.logging import get_logger
from aodn_cloud_optimised.lib.s3Tools import (
    delete_objects_in_prefix,
    split_s3_path,
    prefix_exists,
    create_fileset,
)


def check_variable_values_dask(
    file_path, reference_values, variable_name, dataset_config, uuid_log
):
    """
    Check if the values of a specified variable in a single file are consistent with the reference.

    Args:
        file_path (str): File path to check.
        reference_values (np.ndarray): Reference values for the variable.
        variable_name (str): Name of the variable to check.

    Returns:
        tuple: (file_path, bool) where bool indicates if the file is problematic.

    Comment:
        this variable cannot be in the class below. Otherwise, the self cannot be serialized when calling future
    """
    logger_name = dataset_config.get("logger_name", "generic")
    logger = get_logger(logger_name)
    try:
        ds = xr.open_dataset(file_path)
        variable_values = ds[variable_name].values
        ds.close()

        logger.debug(
            f"{uuid_log}: Checking if {variable_name} in {file_path} is consistent with reference values."
        )

        logger.debug(f"{uuid_log}: Reference values:\n{reference_values}")

        res = not np.array_equal(variable_values, reference_values)

        if res is True:
            logger.error(
                f"{uuid_log}: {variable_name} in {file_path} is NOT consistent with reference values."
            )
            logger.error(f"{uuid_log}: Variable values:\n{variable_values}")
        else:
            logger.debug(f"{uuid_log}: Variable values:\n{variable_values}")

        # Check if the values are identical
        return file_path, res
    except Exception as e:
        logger.error(f"{uuid_log}: Failed to open {file_path}: {e}")
        return file_path, True


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

    # Drop variables not in the list
    vars_to_drop = set(ds.data_vars) - set(schema)

    # Add variables with "drop_var": true in the schema to vars_to_drop
    for var_name, var_details in schema.items():
        if var_details.get("drop_var", False):
            vars_to_drop.add(var_name)

    ds_filtered = ds.drop_vars(vars_to_drop, errors="ignore")
    ds = ds_filtered

    if not ds.data_vars:
        logger.error(
            "The dataset has no data variables left. Check the dataset configuration."
        )
        raise ValueError(
            "The dataset has no data variables left. Check the dataset configuration."
        )

    ##########
    var_required = schema.copy()
    for dim_info in dimensions.values():
        var_required.pop(dim_info["name"], None)

    # Remove variables with "drop_var": true from the var_required list
    for var_name, var_details in schema.items():
        if var_details.get("drop_var", False):
            var_required.pop(var_name, None)

    # create variables from gatts
    gatts_to_variable = dataset_config.get("gatts_to_variable", None)
    if gatts_to_variable:
        for gatts_var in gatts_to_variable:
            dest_name = gatts_to_variable[gatts_var]["destination_name"]
            dim_name = gatts_to_variable[gatts_var]["dimensions"]

            # Get the string length from the config, defaulting to 61 if not specified
            length = gatts_to_variable[gatts_var].get("length", 255)

            # Define the string dtype dynamically with the given length
            string_dtype = f"<U{length}"

            # Create padded string to have consistent variable size accross the whole dataset
            gatt_var_value = getattr(ds, gatts_var, None)
            if gatt_var_value is None:
                gatt_var_value_padded = "".ljust(
                    length
                )  # Ensures dtype becomes <U{length}
            else:
                gatt_var_value_padded = str(gatt_var_value).ljust(length)

            ds[dest_name] = (
                dim_name,
                np.full(ds.dims[dim_name], gatt_var_value_padded, dtype=string_dtype),
            )

    # TODO: make the variable below something more generic? a parameter?
    var_template_shape = dataset_config.get("var_template_shape")

    # retrieve filename from ds
    var = next(var for var in ds)
    try:
        filename = os.path.basename(ds[var].encoding["source"])

    except KeyError as e:
        logger.debug(f"Original filename not available in xarray dataset.\n {e}")
        filename = "UNKOWN_FILENAME.nc"

    # TODO: get filename; Should be from https://github.com/pydata/xarray/issues/9142
    ds["filename"] = (
        dimensions["time"]["name"],
        np.full(ds.dims[dimensions["time"]["name"]], filename),
    )
    # ds = ds.assign(
    # filename=((dimensions["time"]["name"],), [filename])
    # )  # add new filename variable with time dimension

    # TODO: when filename is added, this can be used to find the equivalent data already stored as CO, and create a NAN
    # version to push back as CO in order to "delete". If UNKOWN_FILENAME.nc, either run an error, or have another approach,
    # Maybe the file to delete, would be a filename, OR the physical og NetCDF, which means then that we have all of the info in it, and simply making it NAN

    logger.info(f"Applying preprocessing on dataset from {filename}")
    for variable_name in var_required:
        datatype = var_required[variable_name].get("type")

        # if variable doesn't exist
        if variable_name not in ds:

            logger.warning(
                f"Adding missing variable '{variable_name}' to xarray dataset with NaN values."
            )

            var_dims = schema[variable_name].get("dims", None)
            # check the type of the variable (numerical of string)
            if np.issubdtype(datatype, np.number):
                # Add the missing variable  to the dataset
                if var_dims:
                    dim_names = tuple(var_dims)
                    missing_coords = [dim for dim in dim_names if dim not in ds.coords]

                    # create missing dimension for missing variable
                    # TODO: this is dangerous in case the first file to process of a dataset doesnt have ALL the dimensions. If this is the case, the dimension will be full of NaNs.
                    # How to avoid this? create a dummy NetCDF with epoq date, NaN variables but ALL valid dimensions?
                    if missing_coords:
                        for dim in missing_coords:

                            logger.warning(
                                f"Adding missing coordinate '{dim}' to xarray dataset with NaN values."
                            )
                            size = dimensions[dim][
                                "size"
                            ]  # get size from dataset's .dims
                            nan_array = da.full(size, da.nan, dtype=da.float64)
                            ds.coords[dim] = (dim, nan_array)

                    shape = tuple([ds[dim].shape[0] for dim in dim_names])
                    nan_array = da.full(shape, da.nan, dtype=da.float64)

                else:
                    logger.warning(
                        f"The 'dims' key is missing for '{variable_name}' in the schema. Defaulting to creating '{variable_name}' with all available dimensions: {dimensions}"
                    )
                    dim_names = tuple(
                        dim_info["name"] for dim_info in dimensions.values()
                    )
                    # TODO: to clean and remove this abomination. nan_array should always be created based on the dims set for each variable!
                    nan_array = da.full(
                        ds[var_template_shape].shape, da.nan, dtype=da.float64
                    )

                ds[variable_name] = (
                    dim_names,
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
                logger.warning(f"Variable '{variable_name}' is not of a numeric type (np.number).")

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
        self.vars_incompatible_with_region = self.dataset_config.get(
            "vars_incompatible_with_region", None
        )

        # Generic chunks structure
        self.chunks = {
            dim_info["name"]: dim_info["chunk"]
            for dim_info in self.dimensions.values()
            if "chunk" in dim_info
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

        drop_vars_list = [
            var_name
            for var_name, attrs in self.schema.items()
            if attrs.get("drop_var", False)
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
                f"{self.uuid_log}: Listing objects to process and creating S3 file handle list for batch {idx + 1}."
            )

            batch_files = create_fileset(batch_uri_list, self.s3_fs)

            self.logger.info(f"{self.uuid_log}: Processing batch {idx + 1} with files: {batch_files}")

            if drop_vars_list:
                self.logger.warning(
                    f"{self.uuid_log}: Dropping variables from the dataset: {drop_vars_list}"
                )

            partial_preprocess_already_run = False

            try:
                ds = self.try_open_dataset(
                    batch_files,
                    partial_preprocess,
                    drop_vars_list,
                )
                partial_preprocess_already_run = True

                if not partial_preprocess_already_run:
                    # Likely redundant now, but retained for safety
                    self.logger.debug(
                        f"{self.uuid_log}: 'partial_preprocess_already_run' is False. Applying preprocess_xarray."
                    )
                    ds = preprocess_xarray(ds, self.dataset_config)

                self._write_ds(ds, idx)
                self.logger.info(
                    f"{self.uuid_log}: Batch {idx + 1} successfully published to Zarr store: {self.store}"
                )

            except MergeError as e:
                self.logger.error(f"{self.uuid_log}: Failed to merge datasets for batch {idx + 1}: {e}")
                if "ds" in locals():
                    self.postprocess(ds)

            except Exception as e:
                self.logger.error(
                    f"{self.uuid_log}: An unexpected error occurred during batch {idx + 1} processing: {e}.\n {traceback.format_exc()}"
                )
                self.fallback_to_individual_processing(
                    batch_files, partial_preprocess, drop_vars_list, idx
                )
                if "ds" in locals():
                    self.postprocess(ds)

    def try_open_dataset(
        self,
        batch_files,
        partial_preprocess,
        drop_vars_list,
        primary_engine="h5netcdf",
        fallback_engine="scipy",
    ):
        def handle_engine(engine):
            try:
                return self._open_mfds(
                    partial_preprocess, drop_vars_list, batch_files, engine=engine
                )
            except (ValueError, TypeError):
                # Capture and inspect the traceback; 3 known scenarios
                tb = traceback.format_exc()
                match_grid_not_consistent = re.search(
                    r"Coordinate variable (\w+) is neither monotonically increasing nor monotonically decreasing on all datasets",
                    tb,
                )
                match_not_netcdf4_signature = re.search(
                    r"is not the signature of a valid netCDF4 file", tb
                )
                match_not_netcdf3_signature = re.search(
                    r"is not a valid NetCDF 3 file", tb
                )

                if match_grid_not_consistent:
                    variable_name = match_grid_not_consistent.group(1)
                    return self.handle_coordinate_variable_issue(
                        batch_files,
                        variable_name,
                        partial_preprocess,
                        drop_vars_list,
                        engine,
                    )
                elif engine == primary_engine and match_not_netcdf4_signature:
                    # Indicates a possible format issue; Tried to open a NetCDF3 files with h5netcdf; fallback to the next engine
                    raise ValueError("Switch to fallback engine")
                elif engine == fallback_engine and match_not_netcdf3_signature:
                    # Should be a NetCDF3, but didn't work with scipy!
                    # Final failure point for fallback engine
                    raise ValueError(f"Invalid NetCDF file format in {batch_files}")
                else:
                    # Unknown or unhandled error
                    raise

        try:
            # First attempt with the primary engine
            return handle_engine(primary_engine)
        except ValueError as e:

            if str(e) == "Switch to fallback engine":
                try:
                    # Second attempt with the fallback engine
                    return handle_engine(fallback_engine)
                except Exception:
                    # If fallback also fails, handle multi-engine fallback
                    # Log full traceback before continuing
                    traceback.print_exc()
                    return self.handle_multi_engine_fallback(
                        batch_files, partial_preprocess, drop_vars_list
                    )

    def handle_coordinate_variable_issue(
        self, batch_files, variable_name, partial_preprocess, drop_vars_list, engine
    ):

        self.logger.error(
            f"{self.uuid_log}: Detected issue with variable '{variable_name}': Inconsistent grid."
        )
        self.logger.warning(
            f"{self.uuid_log}: Running variable consistency check across files in the batch."
        )
        problematic_files = self.check_variable_values_parallel(
            batch_files, variable_name
        )
        clean_batch_files = [
            file for file in batch_files if file not in problematic_files
        ]
        self.logger.warning(
            f"{self.uuid_log}: Processing batch without problematic files: {problematic_files}"
        )
        self.logger.info(
            f"{self.uuid_log}: Processing the following clean files:\n{clean_batch_files}"
        )

        return self._open_mfds(
            partial_preprocess, drop_vars_list, clean_batch_files, engine
        )

    def handle_multi_engine_fallback(
        self, batch_files, partial_preprocess, drop_vars_list
    ):
        self.logger.warning(
            f"{self.uuid_log}: Neither 'h5netcdf' nor 'scipy' engine could concatenate the dataset. "
            f"Falling back to opening files individually with different engines."
        )
        try:
            ds = self._concatenate_files_different_engines(
                batch_files, partial_preprocess, drop_vars_list
            )
            self.logger.info(
                f"{self.uuid_log}: Successfully concatenated files using different engines."
            )
            return ds
        except Exception as e:
            self.logger.warning(f"{self.uuid_log}: Error during multi-engine fallback: {e}.\n {traceback.format_exc()}")
            raise RuntimeError("Fallback to individual processing needed.")

    def fallback_to_individual_processing(
        self, batch_files, partial_preprocess, drop_vars_list, idx
    ):
        self.logger.info(
            f"{self.uuid_log}: Batch processing methods failed for batch {idx +1}. Falling back to processing files individually."
        )
        try:
            self._process_individual_file_fallback(
                batch_files, partial_preprocess, drop_vars_list, idx
            )
        except Exception as e:
            self.logger.error(
                f"{self.uuid_log}: An unexpected error occurred during individual file fallback processing for batch {idx + 1}: {e}.\n {traceback.format_exc()}"
            )

    def check_variable_values_parallel(self, file_paths, variable_name):
        """
        Check the values of a specified variable in all files using a Coiled cluster.

        Args:
            file_paths (list): List of file paths to check.
            variable_name (str): Name of the variable to check.

        Returns:
            list: List of file paths with inconsistent variable values.
        """
        # Open the first file and store its variable values as the reference
        try:
            reference_ds = xr.open_dataset(file_paths[0])
            reference_values = reference_ds[variable_name].values
            reference_ds.close()
        except Exception as e:
            self.logger.error(
                f"{self.uuid_log}: Failed to open the first file {file_paths[0]}: {e}"
            )
            return file_paths  # If the first file fails, consider all files problematic

        if self.cluster_mode:
            # Use Dask to process files in parallel
            futures = self.client.map(
                check_variable_values_dask,
                file_paths[1:],
                reference_values=reference_values,
                variable_name=variable_name,
                dataset_config=self.dataset_config,
                uuid_log=self.uuid_log,
            )
            results = self.client.gather(futures)
        else:
            # TODO: not running on a cluster. But if that's the case, most likely this will run a file one by one,
            #       so it will break anyway as no reference values. Hopefully this won't happen. alternative would be
            #       to have a ref value directly taken from the original zarr? but that would work only if the zarr
            #       already exist... to annoying/complicated to implement as it shouldn't happen. easier to debug if it
            #       does
            results = [
                check_variable_values_dask(
                    file_path,
                    reference_values,
                    variable_name,
                    self.dataset_config,
                    self.uuid_log,
                )
                for file_path in file_paths[1:]
            ]

        # Collect problematic files
        problematic_files = [
            file_path for file_path, is_problematic in results if is_problematic
        ]
        if problematic_files:
            self.logger.error(
                f"{self.uuid_log}: Contact the data provider. The following files have an inconsistent grid with the rest of the dataset for variable '{variable_name}':\n{problematic_files}"
            )
        return problematic_files

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
            f"{self.uuid_log}: Duplicate values of '{time_dimension_name}' found in existing dataset. Overwriting."
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
                f"{self.uuid_log}: Batch {idx + 1}, Region {n_region + 1} - Overwriting Zarr dataset in Region: {region}, with matching indexes in new dataset: {indexes}"
            )

            ##########################################
            # extremely rare!! only happened once, and why??
            s = regions[0][time_dimension_name]
            region_num_elements = s.stop - s.start
            matching_indexes_array_size = np.size(matching_indexes[0])

            if matching_indexes_array_size != region_num_elements:
                amount_to_pad = region_num_elements - matching_indexes_array_size

                self.logger.error(
                    f"{self.uuid_log}: Mismatch in size for region {region}. Original region size: {region_num_elements}, new data size: {matching_indexes_array_size}. "
                    f"Attempting to pad the new dataset with {amount_to_pad} NaN index(es)."
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
                self.vars_incompatible_with_region, errors="ignore"
            ).pad(**{time_dimension_name: (0, amount_to_pad)}).to_zarr(
                self.store,
                write_empty_chunks=self.write_empty_chunks,
                region=region,
                compute=True,
                consolidated=self.consolidated,
                safe_chunks=self.safe_chunks,
                mode="r+",
            )
            self.logger.info(
                f"{self.uuid_log}: Batch {idx + 1}, Region {n_region + 1} - Successfully published to {self.store}"
            )
            n_region += 1

        self.logger.info(
            f"{self.uuid_log}: All overlapping regions from Batch {idx + 1} were successfully published to {self.store}"
        )
        self.logger.info(
            f"{self.uuid_log}: Checking for non-overlapping data from Batch {idx + 1} to append to {self.store}"
        )
        # Now find the time values in ds that were NOT reprocessed
        # These are the time values not found in any of the common regions
        unprocessed_time_values = np.setdiff1d(time_values_new, common_time_values)

        # Return a dataset with the unprocessed time values
        if len(unprocessed_time_values) > 0:
            self.logger.info(
                f"{self.uuid_log}: Found {len(unprocessed_time_values)} non-overlapping data points from Batch {idx + 1} to append to {self.store}"
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
            engine = "scipy"
            with self.s3_fs.open(file, "rb") as f:  # Open the file-like object
                ds = self._open_ds(f, partial_preprocess, drop_vars_list, engine=engine)
            self.logger.info(
                f"{self.uuid_log}: Successfully opened {file} with '{engine}' engine."
            )
            return ds
        except (ValueError, TypeError) as e:
            self.logger.info(
                f"{self.uuid_log}: Error opening {file} with 'scipy' engine: {e}. Defaulting to 'h5netcdf'."
            )
            engine = "h5netcdf"

            ds = self._open_ds(file, partial_preprocess, drop_vars_list, engine=engine)
            self.logger.info(
                f"{self.uuid_log}: Successfully opened {file} with '{engine}' engine."
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
            f"{self.uuid_log}: Successfully read all files in batch with different engines. Concatenating them."
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
        self.logger.info(f"{self.uuid_log}: Successfully concatenated files from batch.")
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
            ds = ds.chunk(chunks="auto")
        except Exception as err:
            self.logger.warning(
                f"{self.uuid_log}: Error auto-chunking dataset: {err}. Defaulting to specified chunks: {self.chunks}"
            )
            ds = ds.chunk(chunks=self.chunks)

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
            ds = ds.chunk(chunks="auto")
        except Exception as err:
            self.logger.warning(
                f"{self.uuid_log}: Error auto-chunking dataset from file '{file}': {err}. Defaulting to specified chunks: {self.chunks}"
            )
            ds = ds.chunk(chunks=self.chunks)

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

        # TODO: Raise error if duplicates are found? Not necessarily an issue. For example, some SOOP dataset, same TIME, 2 different NetCDF files, 2 different vessel and location.
        if len(duplicates) > 0:
            self.logger.warning(
                f"{self.uuid_log}: Duplicate values of '{time_dimension_name}' dimension "
                f"found in original Zarr dataset. This could lead to a corrupted dataset. Duplicates: {duplicates}"
            )

        return True

    def _write_ds(self, ds, idx):
        time_dimension_name = self.dimensions["time"]["name"]
        ds = ds.sortby(time_dimension_name)
        if any(chunk == 0 for chunk in self.chunks.values()):
            self.logger.warning(
                f"{self.uuid_log}: One or more dimensions in the dataset configuration have a chunk size of 0: {self.chunks}. "
                f"Please modify the configuration file. Defaulting to 'chunks=auto'."
            )
            ds = ds.chunk(chunks="auto")
        else:
            ds = ds.chunk(chunks=self.chunks)

        # TODO: see https://github.com/pydata/xarray/issues/5219  https://github.com/pydata/xarray/issues/5286
        for var in ds:
            if "chunks" in ds[var].encoding:
                del ds[var].encoding["chunks"]

        # Write the dataset to Zarr
        if prefix_exists(self.cloud_optimised_output_path):
            self.logger.info(f"{self.uuid_log}: Existing Zarr store found at {self.cloud_optimised_output_path}. Appending data.")

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
                        f"{self.uuid_log}: Batch {idx + 1} successfully appended to {self.store}"
                    )

        # First time writing the dataset
        else:
            self._write_new_zarr_store(ds)

    def _write_new_zarr_store(self, ds):
        """
        Writes the dataset to a new Zarr store.
        """
        self.logger.info(f"{self.uuid_log}: Writing data to a new Zarr dataset at {self.store}.")
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
        self.logger.info(f"{self.uuid_log}: Appending data to existing Zarr dataset at {self.store}.")
        # import ipdb; ipdb.set_trace()
        ds.to_zarr(
            self.store,
            mode="a-",
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
                f"Option 'clear_existing_data' is True. DELETING all existing Zarr objects at {self.cloud_optimised_output_path} if they exist."
            )
            # TODO: delete all objects
            if prefix_exists(self.cloud_optimised_output_path):
                bucket_name, prefix = split_s3_path(self.cloud_optimised_output_path)
                self.logger.info(
                    f"Deleting existing Zarr objects from bucket '{bucket_name}' with prefix '{prefix}'."
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

    # TODO:
    # The following function is not quite ready.
    # Also to be defined if this is needed.
    # def rechunk(self, max_mem="8.0GB"):
    #     """
    #     Rechunk a Zarr dataset stored on S3.
    #
    #     Parameters:
    #     - max_mem (str, optional): Maximum memory to use during rechunking (default is '8.0GB').
    #
    #     Returns:
    #     None
    #
    #     Example:
    #     >>> gridded_handler_instance.rechunk(max_mem='12.0GB')
    #
    #     This method rechunks a Zarr dataset stored on S3 using Dask and the specified target chunks.
    #     The rechunked data is stored in a new Zarr dataset on S3.
    #     The intermediate and previous rechunked data are deleted before the rechunking process.
    #     The 'max_mem' parameter controls the maximum memory to use during rechunking.
    #
    #     Note: The target chunks for rechunking are determined based on the dimensions and chunking information.
    #     """
    #     # with worker_client():
    #     with Client() as client:
    #
    #         target_chunks = self.filter_rechunk_dimensions(
    #             self.dimensions
    #         )  # only return a dict with the dimensions to rechunk
    #
    #         # s3 = s3fs.S3FileSystem(anon=False)
    #
    #         org_url = (
    #             self.cloud_optimised_output_path
    #         )  # f's3://{self.optimised_bucket_name}/zarr/{self.dataset_name}.zarr'
    #         # org_store = s3fs.S3Map(root=f'{org_url}', s3=s3, check=False)
    #
    #         target_url = org_url.replace(
    #             f"{self.dataset_name}", f"{self.dataset_name}_rechunked"
    #         )
    #         target_store = s3fs.S3Map(root=f"{target_url}", s3=self.s3_fs, check=False)
    #         # zarr.consolidate_metadata(org_store)
    #
    #         ds = xr.open_zarr(fsspec.get_mapper(org_url, anon=True), consolidated=True)
    #
    #         temp_url = org_url.replace(
    #             f"{self.dataset_name}", f"{self.dataset_name}_intermediate"
    #         )
    #
    #         temp_store = s3fs.S3Map(root=f"{temp_url}", s3=self.s3_fs, check=False)
    #
    #         # delete previous version of intermediate and rechunked data
    #         s3_client = boto3.resource("s3")
    #         bucket = s3_client.Bucket(f"{self.optimised_bucket_name}")
    #         self.logger.info("Delete previous rechunked version in progress")
    #
    #         bucket.objects.filter(
    #             Prefix=temp_url.replace(f"s3://{self.optimised_bucket_name}/", "")
    #         ).delete()
    #         bucket.objects.filter(
    #             Prefix=target_url.replace(f"s3://{self.optimised_bucket_name}/", "")
    #         ).delete()
    #         self.logger.info(
    #             f"Rechunking in progress with target chunks: {target_chunks}"
    #         )
    #
    #         options = dict(overwrite=True)
    #         array_plan = rechunk(
    #             ds,
    #             target_chunks,
    #             max_mem,
    #             target_store,
    #             temp_store=temp_store,
    #             temp_options=options,
    #             target_options=options,
    #         )
    #         with ProgressBar():
    #             self.logger.info(f"{array_plan.execute()}")
    #
    #         zarr.consolidate_metadata(target_store)
