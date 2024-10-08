import importlib.resources
import os
import uuid
import warnings
from functools import partial

import boto3
import dask
import fsspec
import numpy as np
import s3fs
import xarray as xr
import zarr
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from rechunker import rechunk
from xarray.core.merge import MergeError

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
    #       2) Running in a dask remote cluster, it seemed like the preprocess function (if donne within mfdataset)
    #          was actually running locally and using ALL of the local ram. Complete nonsense. So this function was made
    #          as a test. It should be run after the xarray dataset is opened. More testing required as
    #          self.preprocess_xarray() was pretty complete function.

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
    ds_filtered = ds.drop_vars(vars_to_drop)
    ds = ds_filtered

    ##########
    var_required = schema.copy()
    var_required.pop(dimensions["time"]["name"])
    var_required.pop(dimensions["latitude"]["name"])
    var_required.pop(dimensions["longitude"]["name"])

    # TODO: make the variable below something more generic? a parameter?
    var_template_shape = dataset_config.get("var_template_shape")

    # retrieve filename from ds
    var = next(var for var in ds)
    filename = os.path.basename(ds[var].encoding["source"])
    logger.info(f"Applying preprocessing on dataset from {filename}")
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
        # else:
        #
        #     if (
        #         datatype == "timestamp[ns]"
        #     ):  # Timestamps do not have an astype method. But numpy.datetime64 do.
        #         datatype = "datetime64[ns]"
        #
        #     elif not np.issubdtype(datatype, np.number):
        #         # we repeat the string variable to match the size of the TIME dimension
        #         ds[variable_name] = (
        #             (dimensions["time"]["name"],),
        #             np.full_like(
        #                 ds[dimensions["time"]["name"]],
        #                 ds[variable_name],
        #                 dtype="<S1",
        #             ),
        #         )
        #
        #     ds[variable_name] = ds[variable_name].astype(datatype)

    logger.info(f"Succesfully applied preprocessing to dataset from {filename}")
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

    # TODO: Unused at the moment
    # def preprocess_xarray(self, ds) -> xr.Dataset:
    #     """
    #     Perform preprocessing on the input dataset (`ds`) and return an xarray Dataset.
    #
    #     :param ds: Input xarray Dataset.
    #
    #     :return:
    #         Preprocessed xarray Dataset.
    #     """
    #
    #     # Drop variables not in the list
    #     # TODO: add a warning message
    #     vars_to_drop = set(ds.data_vars) - set(self.schema)
    #     ds_filtered = ds.drop_vars(vars_to_drop)
    #     ds = ds_filtered
    #
    #     # https://github.com/pydata/xarray/issues/2313
    #     # filename = ds.encoding["source"]
    #
    #     # self.logger.info(f"{filename}: xarray preprocessing")
    #
    #     # Add a new dimension 'filename' with a filename value
    #     filename = None
    #     if filename is not None:
    #         ds = ds.assign(
    #             filename=((self.dimensions["time"]["name"],), [filename])
    #         )  # add new filename variable with time dimension
    #
    #     var_required = self.schema.copy()
    #     var_required.pop(self.dimensions["time"]["name"])
    #     var_required.pop(self.dimensions["latitude"]["name"])
    #     var_required.pop(self.dimensions["longitude"]["name"])
    #
    #     # TODO: make the variable below something more generic? a parameter?
    #     var_template_shape = self.dataset_config.get("var_template_shape")
    #
    #     try:
    #         warnings.filterwarnings("error", category=RuntimeWarning)
    #         nan_array = np.full(ds[var_template_shape].shape, np.nan, dtype=np.float64)
    #         # the following commented line returned some RuntimeWarnings every now and then.
    #         # nan_array = np.empty(
    #         #    ds[var_template_shape].shape) * np.nan  # np.full_like( (1, 4500, 6000), np.nan, dtype=object)
    #     except RuntimeWarning as rw:
    #         raise TypeError
    #
    #     for variable_name in var_required:
    #         datatype = var_required[variable_name].get("type")
    #
    #         # if variable doesn't exist
    #         if variable_name not in ds:
    #             # self.logger.warning(
    #             #     f"{filename}: add missing {variable_name} to xarray dataset"
    #             # )
    #             self.logger.warning(f"add missing {variable_name} to xarray dataset")
    #
    #             # check the type of the variable (numerical of string)
    #             if np.issubdtype(datatype, np.number):
    #                 # Add the missing variable  to the dataset
    #                 ds[variable_name] = (
    #                     (
    #                         self.dimensions["time"]["name"],
    #                         self.dimensions["latitude"]["name"],
    #                         self.dimensions["longitude"]["name"],
    #                     ),
    #                     nan_array,
    #                 )
    #
    #             else:
    #                 # for strings variables, it's quite likely that the variables don't have any dimensions associated.
    #                 # This can be an issue to know which datapoint is associated to which string value.
    #                 # In this case we might have to repeat the string the times of ('TIME')
    #                 ds[variable_name] = (
    #                     (self.dimensions["time"]["name"],),
    #                     empty_string_array,
    #                 )
    #
    #             ds[variable_name] = ds[variable_name].astype(datatype)
    #
    #         # if variable already exists
    #         else:
    #
    #             if (
    #                 datatype == "timestamp[ns]"
    #             ):  # Timestamps do not have an astype method. But numpy.datetime64 do.
    #                 datatype = "datetime64[ns]"
    #
    #             elif not np.issubdtype(datatype, np.number):
    #                 # we repeat the string variable to match the size of the TIME dimension
    #                 ds[variable_name] = (
    #                     (self.dimensions["time"]["name"],),
    #                     np.full_like(
    #                         ds[self.dimensions["time"]["name"]],
    #                         ds[variable_name],
    #                         dtype="<S1",
    #                     ),
    #                 )
    #
    #             ds[variable_name] = ds[variable_name].astype(datatype)
    #
    #     return ds

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

        self.logger.info(
            "Listing all objects to process and creating a s3_file_handle_list"
        )
        s3_file_handle_list = create_fileset(s3_file_uri_list, self.s3_fs)

        time_dimension_name = self.dimensions["time"]["name"]

        batch_size = self.get_batch_size(client=self.client)

        for idx, batch_files in enumerate(
            self.batch_process_fileset(s3_file_handle_list, batch_size=batch_size)
        ):
            self.uuid_log = str(uuid.uuid4())  # value per batch

            self.logger.info(f"{self.uuid_log}: Processing batch {idx + 1}...")
            self.logger.info(batch_files)

            # batch_filenames = [os.path.basename(f.full_name) for f in batch_files]

            partial_preprocess = partial(
                preprocess_xarray, dataset_config=self.dataset_config
            )
            partial_preprocess_already_run = False

            drop_vars_list = [
                var_name
                for var_name, attrs in self.schema.items()
                if attrs.get("drop_vars", False)
            ]
            if drop_vars_list:
                self.logger.warning(
                    f"{self.uuid_log}: Dropping variables: {drop_vars_list} from the dataset"
                )

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
                    try:
                        ds = xr.open_mfdataset(
                            batch_files,
                            engine="h5netcdf",
                            parallel=True,
                            preprocess=partial_preprocess,  # this sometimes hangs the process
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

                        partial_preprocess_already_run = True
                    except:
                        self.logger.warning(
                            f'{self.uuid_log}: The default engine "h5netcdf" could not be used. Falling back '
                            f'to using "scipy" engine. This is an issue with old NetCDF files'
                        )
                        try:
                            ds = xr.open_mfdataset(
                                batch_files,
                                engine="scipy",
                                parallel=True,
                                preprocess=partial_preprocess,  # this sometimes hangs the process
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

                            partial_preprocess_already_run = True

                        except:
                            self.logger.warning(
                                f'{self.uuid_log}: The engine "scipy" could not be used to concatenate the dataset together. '
                                f"likely because the files to concatenate are NetCDF3 and NetCDF4 and should use different engines. Falling back "
                                f"to opening the files individually with different engines"
                            )

                        # TODO: once xarray issue is fixed https://github.com/pydata/xarray/issues/8909,
                        #  the follwing code could probably be removed. Only scipy and h5netcdf can be used for remote access
                        # Open each file individually
                        datasets = []
                        for file in batch_files:
                            try:
                                # fs = fsspec.filesystem('s3')
                                with self.s3_fs.open(
                                    file, "rb"
                                ) as f:  # Open the file-like object
                                    ds = xr.open_dataset(
                                        f,
                                        engine="scipy",
                                        mask_and_scale=True,
                                        decode_cf=True,
                                        decode_times=True,
                                        use_cftime=True,
                                        decode_coords=True,
                                        drop_variables=drop_vars_list,
                                    )

                                self.logger.info(
                                    f"{self.uuid_log}: Success opening {file} with scipy engine."
                                )
                            except Exception as e:
                                self.logger.error(
                                    f"{self.uuid_log}: Error opening {file}: {e} with scipy engine. Defaulting to h5netcdf"
                                )
                                ds = xr.open_dataset(
                                    file,
                                    engine="h5netcdf",
                                    mask_and_scale=True,
                                    decode_cf=True,
                                    decode_times=True,
                                    use_cftime=True,
                                    decode_coords=True,
                                    drop_variables=drop_vars_list,
                                )
                                self.logger.info(
                                    f"{self.uuid_log}: Success opening {file} with h5netcdf engine."
                                )

                                # Apply preprocessing if needed
                                # ds = partial_preprocess(ds)

                            datasets.append(ds)

                        # Concatenate the datasets
                        self.logger.info(
                            f"{self.uuid_log}: Successfully read all files with different engines. Concatenating them together"
                        )
                        ds = xr.concat(
                            datasets,
                            compat="override",
                            coords="minimal",
                            data_vars="minimal",
                            dim=self.dimensions["time"]["name"],
                        )
                        self.logger.info(
                            f"{self.uuid_log}: Successfully Concatenating files together"
                        )

                    # TODO: create a simple jupyter notebook 2 show 2 different problems:
                    #       1) serialization issue if preprocess is within a class
                    #       2) blowing of memory if preprocess function is outside of a class and only does return ds

                    # If ds open with mf_dataset with the partial preprocess_xarray, no need to re-run it again!
                    if partial_preprocess_already_run == False:
                        self.logger.warning(
                            f"{self.uuid_log}: partial_preprocess_already_run is False"
                        )
                        ds = preprocess_xarray(ds, self.dataset_config)

                    # NOTE: if I comment the next line, i get some errors with the latest chunk for some variables
                    ds = ds.chunk(
                        chunks=self.chunks
                    )  # careful with chunk size, had an issue

                    # Write the dataset to Zarr
                    if prefix_exists(self.cloud_optimised_output_path):
                        self.logger.info(
                            f"{self.uuid_log}: Appending data to existing Zarr"
                        )

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

                        time_values_org = ds_org[time_dimension_name].values
                        time_values_new = ds[time_dimension_name].values

                        # Find common time values
                        common_time_values = np.intersect1d(
                            time_values_org, time_values_new
                        )

                        # Handle the 2 scenarios, reprocessing of a batch, or append new data
                        if len(common_time_values) > 0:
                            self.logger.info(
                                f"{self.uuid_log}: Duplicate values of {self.dimensions['time']['name']}"
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
                                        {time_dimension_name: slice(start, end + 1)}
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
                            regions.append({time_dimension_name: slice(start, end + 1)})
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
                                    f"{self.uuid_log}: Overwriting Zarr dataset in Region: {region}, Matching Indexes in new ds: {indexes}"
                                )
                                ds.isel(**{time_dimension_name: indexes}).drop_vars(
                                    self.vars_to_drop_no_common_dimension
                                ).to_zarr(
                                    self.store,
                                    write_empty_chunks=False,
                                    region=region,
                                    compute=True,
                                    consolidated=True,
                                )

                                self.logger.info(
                                    f"{self.uuid_log}: Batch {idx + 1} successfully published to {self.store}"
                                )

                        # No reprocessing needed
                        else:
                            self.logger.info(
                                f"{self.uuid_log}: Appending data to Zarr dataset"
                            )

                            ds.to_zarr(
                                self.store,
                                mode="a",  # append mode for the next batches
                                write_empty_chunks=False,  # TODO: could True fix the issue when some variables dont exists? I doubt
                                compute=True,  # Compute the result immediately
                                consolidated=True,
                                append_dim=time_dimension_name,
                            )

                            self.logger.info(
                                f"Batch {idx + 1} successfully published to {self.store}"
                            )

                    # First time writing the dataset
                    else:
                        self.logger.info(
                            f"{self.uuid_log}: Writing data to a new Zarr dataset"
                        )

                        ds.to_zarr(
                            self.store,
                            mode="w",  # Overwrite mode for the first batch
                            write_empty_chunks=False,
                            compute=True,  # Compute the result immediately
                            consolidated=True,
                        )

                    self.logger.info(
                        f"{self.uuid_log}: Batch {idx + 1} successfully published to Zarr store: {self.store}"
                    )

                except MergeError as e:
                    self.logger.error(f"{self.uuid_log}: Failed to merge datasets: {e}")
                    if "ds" in locals():
                        self.postprocess(ds)

                except Exception as e:
                    self.logger.error(
                        f"{self.uuid_log}: An unexpected error occurred: {e}"
                    )
                    if "ds" in locals():
                        self.postprocess(ds)

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
            self.s3_file_uri_list = s3_file_uri_list
            # creating a cluster to process multiple files at once
            self.client, self.cluster = self.create_cluster()
            if self.cluster_mode == "remote":
                self.cluster_id = self.cluster.cluster_id
            else:
                self.cluster_id = self.cluster.name
            self.publish_cloud_optimised_fileset_batch(s3_file_uri_list)
            self.close_cluster(self.client, self.cluster)

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
