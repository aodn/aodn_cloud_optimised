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
    """Checks if variable values in a file match reference values.

    Designed to be run in parallel (e.g., with Dask), hence it's a top-level
    function to avoid serialization issues with class methods.

    Args:
        file_path (str): Path to the NetCDF file to check.
        reference_values (np.ndarray): The reference NumPy array to compare against.
        variable_name (str): The name of the variable within the NetCDF file
            to compare.
        dataset_config (dict): The dataset configuration dictionary, used to
            get the logger name.
        uuid_log (str): A unique identifier for logging purposes within a batch.

    Returns:
        tuple[str, bool]: A tuple containing the file path and a boolean
            indicating if the file is problematic (True) or consistent (False).
            Returns (file_path, True) if any exception occurs during processing.
    """
    logger_name = dataset_config.get("logger_name", "generic")
    logger = get_logger(logger_name)
    try:
        ds = xr.open_dataset(file_path)
        variable_values = ds[variable_name].values
        ds.close()

        logger.debug(
            f"{uuid_log}: {file_path} checking {variable_name} is consistent with the reference values."
        )

        logger.debug(f"{uuid_log}: reference values\n{reference_values}.")

        res = not np.array_equal(variable_values, reference_values)

        if res is True:
            logger.error(
                f"{uuid_log}: {file_path} - {variable_name} is NOT consistent with the reference values."
            )
            logger.error(f"{uuid_log}: variable values\n{variable_values}.")
        else:
            logger.debug(f"{uuid_log}: variable values\n{variable_values}.")

        # Check if the values are identical
        return file_path, res
    except Exception as e:
        logger.error(f"{uuid_log}: Failed to open {file_path}: {e}")
        return file_path, True


def preprocess_xarray(ds, dataset_config):
    """Performs preprocessing steps on an xarray Dataset.

    This function applies preprocessing logic defined in the dataset
    configuration, such as dropping variables, adding missing variables with
    NaN values, ensuring correct data types, and adding a 'filename' variable.

    Note:
        This function is designed to be used as a preprocessing step, potentially
        within `xr.open_mfdataset` or applied manually after opening datasets.
        It handles potential `RuntimeWarning` during NaN array creation.

    Args:
        ds (xr.Dataset): The input xarray Dataset to preprocess.
        dataset_config (dict): Configuration dictionary containing schema,
            dimensions, and other preprocessing parameters. Expected keys include
            'logger_name', 'dimensions', 'schema', 'var_template_shape'.

    Returns:
        xr.Dataset: The preprocessed xarray Dataset.

    Raises:
        ValueError: If the dataset has no data variables left after filtering.
        TypeError: If a `RuntimeWarning` occurs during NaN array creation,
                   indicating potential issues.
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

    # Remove variables with "drop_var": true from the var_required list
    for var_name, var_details in schema.items():
        if var_details.get("drop_var", False):
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

    # TODO: get filename; Should be from https://github.com/pydata/xarray/issues/9142
    ds = ds.assign(
        filename=((dimensions["time"]["name"],), [filename])
    )  # add new filename variable with time dimension

    # TODO: when filename is added, this can be used to find the equivalent data already stored as CO, and create a NAN
    # version to push back as CO in order to "delete". If UNKOWN_FILENAME.nc, either run an error, or have another approach,
    # Maybe the file to delete, would be a filename, OR the physical og NetCDF, which means then that we have all of the info in it, and simply making it NAN

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
    """Handles the creation of cloud-optimised datasets in Zarr format.

    Provides methods to process NetCDF files (potentially in batches),
    apply preprocessing, handle inconsistencies, and write the data to
    an S3 Zarr store, managing chunking, consolidation, and appending.

    Inherits:
        CommonHandler: Provides common functionality like cluster management,
                       logging, and configuration loading.
    """

    def __init__(self, **kwargs):
        """Initialises the GenericZarrHandler.

        Sets up configuration, validates schema, defines chunking strategy,
        initialises S3 filesystem and Zarr store mapper, and sets default
        Zarr writing options.

        Args:
            **kwargs: Keyword arguments passed to the parent `CommonHandler`
                and used for specific Zarr handling configuration. Expected
                kwargs include those needed by `CommonHandler` and potentially
                `rechunk_drop_vars`. Configuration is loaded from the dataset
                config file specified in kwargs or defaults.
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
        """Processes and publishes batches of NetCDF files to the Zarr store.

        Iterates through the input list of S3 URIs, processing them in batches
        determined by cluster resources or a default size. For each batch, it
        attempts to open the files as a single dataset using various strategies
        (engines, handling inconsistencies), preprocesses the data, and writes
        it to the target Zarr store using `_write_ds`. Includes fallback logic
        for individual file processing if batch methods fail.

        Args:
            s3_file_uri_list (list[str]): A list of S3 URIs pointing to the
                NetCDF files to be processed.

        Raises:
            ValueError: If `s3_file_uri_list` is None.
        """
        # Iterate over s3_file_handle_list in batches
        if s3_file_uri_list is None:
            raise ValueError("input_objects is not defined")

        time_dimension_name = self.dimensions["time"]["name"]
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
                ds = self.try_open_dataset(
                    batch_files,
                    partial_preprocess,
                    drop_vars_list,
                )
                partial_preprocess_already_run = True

                if not partial_preprocess_already_run:
                    # Likely redundant now, but retained for safety
                    self.logger.debug(
                        f"{self.uuid_log}: partial_preprocess_already_run is False"
                    )
                    ds = preprocess_xarray(ds, self.dataset_config)

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
        """Attempts to open a batch of files using multiple strategies.

        Tries opening the batch with the primary engine (`h5netcdf`). If that
        fails due to specific known errors (e.g., inconsistent coordinates,
        wrong NetCDF format signature), it attempts specific handling or tries
        the fallback engine (`scipy`). If all standard `open_mfdataset` attempts
        fail, it resorts to opening files individually with mixed engines.

        Args:
            batch_files (list[str]): List of S3 file paths in the current batch.
            partial_preprocess (callable): The pre-configured preprocessing function.
            drop_vars_list (list[str]): List of variable names to drop.
            primary_engine (str): The preferred engine for `xr.open_mfdataset`.
                Defaults to "h5netcdf".
            fallback_engine (str): The engine to try if the primary one fails
                due to specific format errors. Defaults to "scipy".

        Returns:
            xr.Dataset: The opened and potentially preprocessed dataset for the batch.

        Raises:
            RuntimeError: If all attempts, including individual file opening, fail.
            ValueError: If specific unrecoverable errors occur (e.g., invalid
                NetCDF format after trying both engines).
        """

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
                    return self.handle_multi_engine_fallback(
                        batch_files, partial_preprocess, drop_vars_list
                    )

    def handle_coordinate_variable_issue(
        self, batch_files, variable_name, partial_preprocess, drop_vars_list, engine
    ):
        """Handles errors caused by inconsistent coordinate variables in a batch.

        When `xr.open_mfdataset` fails due to a non-monotonic coordinate, this
        method identifies the problematic files by comparing the coordinate
        variable values against a reference (the first file). It then attempts
        to re-open the dataset excluding the problematic files.

        Args:
            batch_files (list[str]): List of S3 file paths in the batch.
            variable_name (str): The name of the inconsistent coordinate variable.
            partial_preprocess (callable): The pre-configured preprocessing function.
            drop_vars_list (list[str]): List of variable names to drop.
            engine (str): The engine ('h5netcdf' or 'scipy') being used when the
                error occurred.

        Returns:
            xr.Dataset: The dataset opened from the cleaned batch of files.
        """

        self.logger.error(
            f"{self.uuid_log}: Detected issue with variable: {variable_name}. Inconsistent grid"
        )
        self.logger.warning(
            f"{self.uuid_log}: Running variable consistency check across files in batch"
        )
        problematic_files = self.check_variable_values_parallel(
            batch_files, variable_name
        )
        clean_batch_files = [
            file for file in batch_files if file not in problematic_files
        ]
        self.logger.warning(
            f"{self.uuid_log}: Processing batch without problematic files"
        )
        self.logger.info(
            f"{self.uuid_log}: Processing the following files:\n{clean_batch_files}"
        )

        return self._open_mfds(
            partial_preprocess, drop_vars_list, clean_batch_files, engine
        )

    def handle_multi_engine_fallback(
        self, batch_files, partial_preprocess, drop_vars_list
    ):
        """Handles fallback scenario where files need different engines.

        If `xr.open_mfdataset` fails with both 'h5netcdf' and 'scipy' engines
        (likely due to a mix of NetCDF3 and NetCDF4 files in the batch), this
        method attempts to open each file individually, trying 'scipy' first
        and falling back to 'h5netcdf', then concatenates the resulting datasets.

        Args:
            batch_files (list[str]): List of S3 file paths in the batch.
            partial_preprocess (callable): The pre-configured preprocessing function.
            drop_vars_list (list[str]): List of variable names to drop.

        Returns:
            xr.Dataset: The concatenated dataset from individually opened files.

        Raises:
            RuntimeError: If concatenation fails after individual opening attempts.
        """
        self.logger.warning(
            f'{self.uuid_log}: Neither "h5netcdf" or "scipy" engine could not be used to concatenate the dataset together. '
            f"Falling back to opening files individually with different engines."
        )
        try:
            ds = self._concatenate_files_different_engines(
                batch_files, partial_preprocess, drop_vars_list
            )
            self.logger.info(
                f"{self.uuid_log}: Successfully concatenated files together."
            )
            return ds
        except Exception as e:
            self.logger.warning(f"{self.uuid_log}: {e}.\n {traceback.format_exc()}")
            raise RuntimeError("Fallback to individual processing needed.")

    def fallback_to_individual_processing(
        self, batch_files, partial_preprocess, drop_vars_list, idx
    ):
        """Handles the ultimate fallback: processing files one by one.

        If all batch processing attempts (`try_open_dataset`) fail for a batch,
        this method iterates through the files in the batch, opens each one
        individually (using `_open_file_with_fallback`), and writes it to the
        Zarr store immediately using `_write_ds`.

        Args:
            batch_files (list[str]): List of S3 file paths in the failed batch.
            partial_preprocess (callable): The pre-configured preprocessing function.
            drop_vars_list (list[str]): List of variable names to drop.
            idx (int): The index of the current batch (for logging).
        """
        self.logger.info(
            f"{self.uuid_log}: None of the batch processing methods worked. Falling back to processing files individually."
        )
        try:
            self._process_individual_file_fallback(
                batch_files, partial_preprocess, drop_vars_list, idx
            )
        except Exception as e:
            self.logger.error(
                f"{self.uuid_log}: An unexpected error occurred during fallback processing: {e}.\n {traceback.format_exc()}"
            )

    def check_variable_values_parallel(self, file_paths, variable_name):
        """Checks variable consistency across files in parallel.

        Compares the values of a specified variable in multiple NetCDF files
        against the values in the first file of the list. Uses the configured
        Dask cluster (if available) for parallel execution via the top-level
        `check_variable_values_dask` function.

        Args:
            file_paths (list[str]): List of S3 file paths to check. The first
                file is used as the reference.
            variable_name (str): The name of the variable to compare across files.

        Returns:
            list[str]: A list of file paths identified as having values for the
                specified variable that are inconsistent with the first file.
                Returns all file paths if the reference file cannot be opened.
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
        self.logger.error(
            f"{self.uuid_log}: Contact the data provider. The following files don't have a consistent grid with the rest of the dataset:\n{problematic_files}"
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
        """Handles writing data when time values overlap with existing Zarr store.

        Identifies contiguous regions in the existing Zarr store that overlap
        in time with the new dataset (`ds`). Writes the corresponding slices
        from `ds` into these regions using `mode='r+'` and `region=...`.
        Also handles potential size mismatches (padding) and ensures variables
        incompatible with region writing are dropped. Finally, identifies and
        writes any non-overlapping data from `ds` using `_write_ds`.

        Args:
            ds (xr.Dataset): The new dataset batch to write.
            idx (int): The index of the current batch (for logging).
            common_time_values (np.ndarray): Array of time values present in
                both the existing store and the new dataset.
            time_values_org (np.ndarray): Array of time values from the
                existing Zarr store.
            time_values_new (np.ndarray): Array of time values from the new
                dataset (`ds`).

        Returns:
            xr.Dataset | None: The dataset containing only the non-overlapping
                (unprocessed) time values, or None if all data overlapped.
        """
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
        """Opens a single file, trying 'scipy' then 'h5netcdf' engine.

        Used as part of the fallback strategy when `open_mfdataset` fails.

        Args:
            file (str): The S3 path of the file to open.
            partial_preprocess (callable): The pre-configured preprocessing function.
            drop_vars_list (list[str]): List of variable names to drop.

        Returns:
            xr.Dataset: The opened and preprocessed dataset.

        Raises:
            Exception: If the file cannot be opened with either engine.
        """
        try:
            engine = "scipy"
            with self.s3_fs.open(file, "rb") as f:  # Open the file-like object
                ds = self._open_ds(f, partial_preprocess, drop_vars_list, engine=engine)
            self.logger.info(
                f"{self.uuid_log}: Success opening {file} with {engine} engine."
            )
            return ds
        except (ValueError, TypeError) as e:
            self.logger.info(
                f"{self.uuid_log}: Error opening {file}: {e} with scipy engine. Defaulting to h5netcdf"
            )
            engine = "h5netcdf"

            ds = self._open_ds(file, partial_preprocess, drop_vars_list, engine=engine)
            self.logger.info(
                f"{self.uuid_log}: Success opening {file} with {engine} engine."
            )
            return ds

    def _process_individual_file_fallback(
        self, batch_files, partial_preprocess, drop_vars_list, idx
    ):
        """Processes files individually as a fallback.

        Iterates through `batch_files`, opens each using `_open_file_with_fallback`,
        and writes it immediately using `_write_ds`.

        Args:
            batch_files (list[str]): List of S3 file paths in the batch.
            partial_preprocess (callable): The pre-configured preprocessing function.
            drop_vars_list (list[str]): List of variable names to drop.
            idx (int): The index of the current batch (for logging).
        """
        for file in batch_files:
            ds = self._open_file_with_fallback(file, partial_preprocess, drop_vars_list)
            self._write_ds(ds, idx)

    def _concatenate_files_different_engines(
        self, batch_files, partial_preprocess, drop_vars_list
    ):
        """Opens files individually and concatenates them.

        Used when `open_mfdataset` fails even with fallback engines. Opens each
        file using `_open_file_with_fallback` and then concatenates the list
        of resulting datasets along the time dimension.

        Args:
            batch_files (list[str]): List of S3 file paths in the batch.
            partial_preprocess (callable): The pre-configured preprocessing function.
            drop_vars_list (list[str]): List of variable names to drop.

        Returns:
            xr.Dataset: The concatenated dataset.
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
        """Opens multiple files as a single dataset using xr.open_mfdataset.

        Configures and calls `xr.open_mfdataset` with appropriate parameters
        for parallel processing, preprocessing, chunking, and decoding.
        Includes fallback for chunking if 'auto' fails.

        Args:
            partial_preprocess (callable): The pre-configured preprocessing function
                to be passed to `open_mfdataset`.
            drop_vars_list (list[str]): List of variable names to drop.
            batch_files (list[str]): List of S3 file paths to open.
            engine (str): The engine to use ('h5netcdf' or 'scipy').
                Defaults to "h5netcdf".

        Returns:
            xr.Dataset: The multi-file dataset opened by xarray.
        """

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
                f"{self.uuid_log}:{err}\n Defaulting to open files without auto chunks option"
            )
            ds = ds.chunk(chunks=self.chunks)

        ds = ds.unify_chunks()
        # ds = ds.map_blocks(partial_preprocess) ## EXTREMELY DANGEROUS TO USE. CORRUPTS SOME DATA CHUNKS SILENTLY while it's working fine with preprocess
        # ds = ds.persist()

        return ds

    # @delayed # Cannot use delayed with xr.concat
    def _open_ds(self, file, partial_preprocess, drop_vars_list, engine="h5netcdf"):
        """Opens and preprocesses a single file.

        Uses `xr.open_dataset` with specified engine and parameters. Applies
        chunking (with fallback from 'auto') and the `preprocess_xarray`
        function.

        Args:
            file (str | object): The file path or file-like object to open.
            partial_preprocess (callable): The pre-configured preprocessing function
                (unused here, `preprocess_xarray` is called directly).
            drop_vars_list (list[str]): List of variable names to drop during opening.
            engine (str): The engine to use ('h5netcdf' or 'scipy').
                Defaults to "h5netcdf".

        Returns:
            xr.Dataset: The opened, chunked, and preprocessed dataset.
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
                f"{self.uuid_log}:{err}\n Defaulting to open files without auto chunks option"
            )
            ds = ds.chunk(chunks=self.chunks)

        # ds = ds.map_blocks(partial_preprocess)
        ds = ds.unify_chunks()

        ds = preprocess_xarray(ds, self.dataset_config)
        # ds = ds.persist()
        return ds

    def _find_duplicated_values(self, ds_org):
        """Checks for duplicate time values in the existing Zarr store.

        Logs an error if duplicate values are found in the time dimension of
        the provided dataset (assumed to be the existing Zarr store).

        Args:
            ds_org (xr.Dataset): The dataset representing the existing Zarr store.

        Returns:
            bool: Always returns True. The main purpose is logging the error.
        """
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
        """Writes a dataset batch to the Zarr store.

        Sorts the dataset by time, ensures correct chunking, removes incompatible
        encoding information, and determines whether to write to a new store,
        append to an existing store, or handle overlapping regions based on
        time values.

        Args:
            ds (xr.Dataset): The preprocessed dataset batch to write.
            idx (int): The index of the current batch (used for determining
                if it's the first write and for logging).
        """
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
        """Writes the dataset to a new Zarr store (mode='w').

        Used for the very first batch when the Zarr store doesn't exist yet.

        Args:
            ds (xr.Dataset): The dataset to write.
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
        """Appends the dataset to an existing Zarr store (mode='a-').

        Used when writing subsequent batches that do not overlap in time with
        the existing data. Appends along the time dimension.

        Args:
            ds (xr.Dataset): The dataset to append.
        """
        time_dimension_name = self.dimensions["time"]["name"]
        self.logger.info(f"{self.uuid_log}: Appending data to Zarr dataset")
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
        """Main entry point to convert NetCDF files to a Zarr dataset.

        Handles optional clearing of existing data, sets up the Dask cluster
        if configured, ensures the input file list is unique, and calls
        `publish_cloud_optimised_fileset_batch` to process the files.
        Closes the cluster afterwards if one was created.

        Args:
            s3_file_uri_list (list[str], optional): List of S3 URIs of NetCDF
                files to process. If None or empty, the method will exit early.
                Defaults to None.
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
        """Filters dimensions based on the 'rechunk' flag in the config.

        Static method used potentially by the (currently commented out) `rechunk`
        method to determine which dimensions and their target chunk sizes should
        be used for rechunking.

        Args:
            dimensions (dict): The 'dimensions' section of the dataset configuration.

        Returns:
            dict: A dictionary where keys are dimension names and values are
                  target chunk sizes for dimensions marked with 'rechunk: true'.
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
