import gc
import importlib.resources
import os
import re
import timeit
import traceback
import uuid
from typing import Tuple, Generator

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from dask.distributed import wait
from shapely.geometry import Point, Polygon

from aodn_cloud_optimised.lib.logging import get_logger
from aodn_cloud_optimised.lib.s3Tools import (
    delete_objects_in_prefix,
    split_s3_path,
    prefix_exists,
    create_fileset,
)
from .CommonHandler import CommonHandler
from .schema import create_pyarrow_schema, generate_json_schema_var_from_netcdf


# TODO: improve log for parallism by adding a uuid for each task


class GenericHandler(CommonHandler):
    def __init__(self, **kwargs):
        """
        Initialise the GenericHandler object.

        Args:
            **kwargs: Additional keyword arguments.
                optimised_bucket_name (str, optional[config]): Name of the optimised bucket.
                root_prefix_cloud_optimised_path (str, optional[config]): Root Prefix path of the location of cloud optimised files
                force_previous_parquet_deletion (bool, optional[config]): Force the deletion of existing cloud optimised files(slow) (default=False)

        Inherits:
            CommonHandler: Provides common functionality for handling cloud-optimised datasets.

        """
        super().__init__(**kwargs)

        self.delete_pq_unmatch_enable = kwargs.get(
            "force_previous_parquet_deletion",
            self.dataset_config.get("force_previous_parquet_deletion", False),
        )

        json_validation_path = str(
            importlib.resources.files("aodn_cloud_optimised")
            .joinpath("config")
            .joinpath("schema_validation_parquet.json")
        )
        self.validate_json(
            json_validation_path
        )  # we cannot validate the json config until self.dataset_config and self.logger are set

        self.partition_period = self.dataset_config["time_extent"].get(
            "partition_timestamp_period", "M"
        )

        self.pyarrow_schema = create_pyarrow_schema(self.dataset_config["schema"])

        self.attributes_list_to_check = ["units", "standard_name", "reference_datum"]

    def preprocess_data_csv(
        self, csv_fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """
        Preprocesses a CSV file using pandas and converts it into an xarray Dataset based on dataset configuration.

        Args:
            csv_fp (str or s3fs.core.S3File): File path or s3fs object of the CSV file to be processed.

        Yields:
            Tuple[pd.DataFrame, xr.Dataset]: A generator yielding a tuple containing the processed pandas DataFrame
                and its corresponding xarray Dataset.

        This method reads a CSV file (`csv_fp`) using pandas' `read_csv` function with configuration options
        specified in the dataset configuration (`pandas_read_csv_config` key of `self.dataset_config`, expected
        to be a JSON-like dictionary). The resulting DataFrame (`df`) is then converted into an xarray Dataset using
        `xr.Dataset.from_dataframe()`.

        Example of `pandas_read_csv_config` in dataset configuration:
        ```json
        "pandas_read_csv_config": {
            "delimiter": ";",
            "header": 0,
            "index_col": "detection_timestamp",
            "parse_dates": ["detection_timestamp"],
            "na_values": ["N/A", "NaN"],
            "encoding": "utf-8"
        }
        ```

        The method also uses the 'schema' from the dataset configuration to assign attributes to variables in the
        xarray Dataset. Each variable's attributes are extracted from the 'schema' and assigned to the Dataset variable's
        attributes. The 'type' attribute from the `pyarrow_schema` is removed from the Dataset variables' attributes since it
        is considered unnecessary.

        If a variable in the Dataset is not found in the schema, an error is logged.
        """
        if "pandas_read_csv_config" in self.dataset_config:
            config_from_json = self.dataset_config["pandas_read_csv_config"]
            df = pd.read_csv(csv_fp, **config_from_json)
        else:
            self.logger.warning(
                f"{self.uuid_log}: No options provided for processing CSV file with pandas. Using default pandas.read_csv configuration."
            )
            df = pd.read_csv(csv_fp)

        ds = xr.Dataset.from_dataframe(df)

        for var in ds.variables:
            if var not in self.schema:
                self.logger.error(
                    f"{self.uuid_log}: Missing variable: {var} from dataset config"
                )
            else:
                ds[var].attrs = self.schema.get(var)
                del ds[var].attrs[
                    "type"
                ]  # remove the type attribute which is not necessary at all

        yield df, ds

    def preprocess_data_netcdf(
        self, netcdf_fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """
        Generate DataFrame and Dataset from a NetCDF file.
        If the dataset is more complicated, this method could be rewritten in a custom class inheriting
        the GenericHandler class with super() for method delegation.

        Args:
            netcdf_fp (str or s3fs.core.S3File): Input NetCDF filepath or s3fs object.

        Yields:
            tuple: A tuple containing DataFrame and Dataset.

        This method reads a NetCDF file (`netcdf_fp`) using xarray's `open_dataset` function with configuration options
        specified in the dataset configuration (`netcdf_read_config` key of `self.dataset_config`, expected
        to be a JSON-like dictionary). The resulting Dataset (`ds`) is converted into a pandas DataFrame (`df`) using
        `ds.to_dataframe()`.

        The method also verifies variable attributes against the 'schema' from the dataset configuration.
        If the attributes do not match the schema, an error is logged.

        Example of `netcdf_read_config` in dataset configuration:
        ```json
        "netcdf_read_config": {
            "engine": "h5netcdf",
            "decode_times": False
        }
        ```
        """
        with xr.open_dataset(netcdf_fp, engine="h5netcdf") as ds:
            # Convert xarray to pandas DataFrame
            df = ds.to_dataframe()
            # TODO: call check function on variable attributes
            if self.check_var_attributes(ds):
                yield df, ds
            else:
                self.logger.error(
                    f"{self.uuid_log}: The NetCDF file does not conform to the pre-defined schema."
                )

    def preprocess_data(
        self, fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """
        Overwrites the preprocess_data method from CommonHandler.

        Args:
            fp (str or s3fs.core.S3File): File path or S3 file object.

        Yields:
            tuple: A tuple containing DataFrame and Dataset.

        If `fp` ends with ".nc", it delegates to `self.preprocess_data_netcdf(fp)`.
        If `fp` ends with ".csv", it delegates to `self.preprocess_data_csv(fp)`.
        """
        if fp.path.endswith(".nc"):
            return self.preprocess_data_netcdf(fp)
        if fp.path.endswith(".csv"):
            return self.preprocess_data_csv(fp)

    @staticmethod
    def cast_table_by_schema(table, schema) -> pa.Table:
        """
        Cast each column of a PyArrow table individually according to a provided schema.

        Args:
            table (pyarrow.Table): The PyArrow table to be casted.
            schema (pyarrow.Schema): The schema to cast the table to.

        Returns:
            pyarrow.Table: The casted PyArrow table.

        """
        field_names = [field.name for field in schema]

        # Cast each column of the table individually according to the schema
        casted_arrays = []
        for name in field_names:
            # Get the data type of the field in the schema
            data_type = schema.field(name).type

            # Cast the column to the desired data type
            casted_array = table.column(name).cast(data_type)

            # Append the casted column to the list of casted arrays
            casted_arrays.append(casted_array)

        # Construct a new table with casted columns
        casted_table = pa.Table.from_arrays(casted_arrays, schema=schema)

        return casted_table

    @staticmethod
    def convert_df_bytes_to_str(df: pd.DataFrame):
        """
        Athena does not support byte object. Converting bytes variables into string
        """
        str_df = df.select_dtypes([object])
        str_df = str_df.stack().str.decode("utf-8").unstack()
        for col in str_df:
            df[col] = str_df[col]

        return df

    @staticmethod
    def create_polygon(point: Point, delta: float) -> str:
        """
        Create a polygon around a given point with rounded longitude and latitude
        to the nearest multiple of the specified delta, and return its Well-Known Binary (WKB)
        representation in hexadecimal format.

        Parameters:
            point (shapely.geometry.Point): The point around which the polygon will be created.
            delta (float): The distance from the point to each side of the polygon, in degrees.

        Returns:
            str: The WKB hexadecimal representation of the created polygon.
        """
        lon, lat = point.x, point.y

        # Round the longitude down to the nearest multiple of delta
        rounded_lon = int(lon / delta) * delta

        # Round the latitude down to the nearest multiple of delta
        rounded_lat = int(lat / delta) * delta

        # Define the coordinates of the polygon based on rounded longitude and latitude
        polygon_coords = [
            (rounded_lon - delta, rounded_lat - delta),
            (rounded_lon + delta, rounded_lat - delta),
            (rounded_lon + delta, rounded_lat + delta),
            (rounded_lon - delta, rounded_lat + delta),
        ]

        # Create the polygon object
        polygon = Polygon(polygon_coords)

        # Return the WKB hexadecimal representation of the polygon
        return polygon.wkb_hex

    def _add_polygon(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a polygon column to the DataFrame based on latitude and longitude data.

        This method creates Point objects from latitude and longitude coordinates in the DataFrame,
        then defines a polygon around each point with a specified delta. The polygon is represented
        as a Well-Known Binary (WKB) hexadecimal value. The polygon column is added to the DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame containing latitude and longitude data.

        Returns:
            pd.DataFrame: The DataFrame with the added polygon column.

        Note:
            The DataFrame is assumed to contain 'LONGITUDE' and 'LATITUDE' columns representing
            longitude and latitude coordinates respectively.
        """
        # Create Point objects from latitude and longitude
        if not "spatial_extent" in self.dataset_config:
            self.logger.error(
                f"{self.uuid_log}: Spatial_extent configuration is missing from dataset configuration."
            )
            raise ValueError

        # load default values if not available in config
        lat_varname = self.dataset_config["spatial_extent"].get("lat", "LATITUDE")
        lon_varname = self.dataset_config["spatial_extent"].get("lon", "LONGITUDE")
        spatial_res = self.dataset_config["spatial_extent"].get(
            "spatial_resolution", 5
        )  # Define delta for the polygon (in degrees)

        # Clean dataset from NaN values of LAT and LON; for ex 'IMOS/Argo/dac/csiro/5905017/5905017_prof.nc'
        for geo_var in [lat_varname, lon_varname]:
            geo_var_has_nan = df[geo_var].isna().any().any()
            if geo_var_has_nan:
                self.logger.warning(
                    f"{self.uuid_log}: The NetCDF contains NaN values of {geo_var}. Removing corresponding data"
                )
                df = df.dropna(
                    subset=[geo_var]
                ).reset_index()  # .reset_index(drop=True)

        point_geometry = [
            Point(lon, lat) for lon, lat in zip(df[lon_varname], df[lat_varname])
        ]

        # Create Polygon objects around each Point

        df["polygon"] = [
            self.create_polygon(point, spatial_res) for point in point_geometry
        ]

        return df

    def _add_timestamp_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds timestamp to the DataFrame.

        Parameters:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with added columns.
        """
        time_varname = self.dataset_config["time_extent"].get("time", "TIME")
        # look for the variable or column with datetime64 type
        if isinstance(df.index, pd.MultiIndex) and (time_varname in df.index.names):
            # for example, files with timeSeries and TIME dimensions such as
            # Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/WAVE-PARAMETERS/ALBANY/2022/DOT-WA_20221106_ALBANY_RT_WAVE-PARAMETERS_monthly.nc

            datetime_var = df.index.get_level_values(time_varname)
        elif (
            isinstance(df.index, pd.Index)
            and df.index.name is not None
            and (time_varname in df.index.name)
        ):
            datetime_var = df.index
        else:
            # for example, soop xbt nrt profiles where the index is the pressure and TIME is a variable
            for column in df.columns:
                if (df[column].dtype == "datetime64[ns]") and column == time_varname:
                    datetime_var = df[column].values

            if "datetime_var" not in locals():
                if pd.api.types.is_datetime64_any_dtype(df.index):
                    datetime_var = df.index
        df["timestamp"] = (
            np.int64(
                pd.to_datetime(datetime_var)
                .to_period(self.partition_period)
                .to_timestamp()
            )
            / 10**9
        )  # for partitions with the date as the 1st of the month

        return df

    def _add_columns_df(self, df: pd.DataFrame, ds: xr.Dataset, f) -> pd.DataFrame:
        """
        Adds filename column to the DataFrame as well as variables defined in the json config.

        Parameters:
            df (pd.DataFrame): Input DataFrame.
            ds (YourDataSetClass): Dataset object containing site_code information.

        Returns:
            pd.DataFrame: DataFrame with added columns.
        """
        gattrs_to_variables = self.dataset_config["gattrs_to_variables"]
        for attr in gattrs_to_variables:
            if attr in ds.attrs:
                df[attr] = getattr(ds, attr)
            else:
                self.logger.warning(
                    f"{self.uuid_log}: The global attribute '{attr}' does not exist in the original NetCDF. The corresponding variable won't be created."
                )

        df["filename"] = os.path.basename(f.path)

        return df

    def _rm_bad_timestamp_df(self, df: pd.DataFrame, f) -> pd.DataFrame:
        """
        Remove rows with bad timestamps from the DataFrame.

        This method handles issues found in files when the 'timestamp' column is not CF-compliant
        and has NaN values, for example.

        :param df: Input DataFrame.
        :type df: pd.DataFrame
        :return: DataFrame with rows containing bad timestamps removed.
        :rtype: pd.DataFrame
        """
        # Flatten multiindex. For example when there is a timeseries variable with all values == 1
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()

        time_varname = self.dataset_config["time_extent"].get("time", "TIME")

        if any(df["timestamp"] < 0):
            self.logger.warning(
                f"{self.uuid_log}: {f.path}: NaN values detected in {time_varname} time variable. Trimming corresponding data."
            )
            df2 = df[df["timestamp"] > 0].copy()
            df = df2
            df = df.reset_index()

        return df

    # def set_metadata(self, tbl, col_meta={}, tbl_meta={}):
    #     """Store table- and column-level metadata as json-encoded byte strings.
    #     function taken from https://stackoverflow.com/questions/55546027/how-to-assign-arbitrary-metadata-to-pyarrow-table-parquet-columns
    #     Table-level metadata is stored in the table's schema.
    #     Column-level metadata is stored in the table columns' fields.
    #
    #     To update the metadata, first new fields are created for all columns.
    #     Next a schema is created using the new fields and updated table metadata.
    #     Finally a new table is created by replacing the old one's schema, but
    #     without copying any data.
    #
    #     Args:
    #         tbl (pyarrow.Table): The table to store metadata in
    #         col_meta: A json-serializable dictionary with column metadata in the form
    #             {
    #                 'column_1': {'some': 'data', 'value': 1},
    #                 'column_2': {'more': 'stuff', 'values': [1,2,3]}
    #             }
    #         tbl_meta: A json-serializable dictionary with table-level metadata.
    #     """
    #     # Create updated column fields with new metadata
    #     if col_meta or tbl_meta:
    #         fields = []
    #         for col in tbl.schema.names:
    #             if col in col_meta:
    #                 # Get updated column metadata
    #                 metadata = tbl.field(col).metadata or {}
    #                 for k, v in col_meta[col].items():
    #                     metadata[k] = json.dumps(v).encode('utf-8')
    #                 # Update field with updated metadata
    #                 fields.append(tbl.field(col).with_metadata(metadata))
    #             else:
    #                 fields.append(tbl.field(col))
    #
    #         # Get updated table metadata
    #         tbl_metadata = tbl.schema.metadata or {}
    #         for k, v in tbl_meta.items():
    #             if type(v) == bytes:
    #                 tbl_metadata[k] = v
    #             else:
    #                 tbl_metadata[k] = json.dumps(v).encode('utf-8')
    #
    #         # Create new schema with updated field metadata and updated table metadata
    #         schema = pa.schema(fields, metadata=tbl_metadata)
    #
    #         # With updated schema build new table (shouldn't copy data)
    #         # tbl = pa.Table.from_batches(tbl.to_batches(), schema)
    #         tbl = tbl.cast(schema)
    #
    #     return tbl

    def check_var_attributes(self, ds):
        """
        Validate the attributes of each variable in an xarray Dataset against a predefined schema.

        This method checks if each variable in the provided xarray Dataset `ds` contains a specific set of attributes
        and verifies that the values of these attributes match the expected values defined in the `dataset_config` schema.
        If any attribute does not match the expected value, a ValueError is raised. If a variable is missing from the
        `dataset_config`, a warning is logged.

        Parameters:
        ds (xarray.Dataset): The dataset to be validated.

        Raises:
        ValueError: If an attribute value does not match the expected value as defined in the schema.
        KeyError: If an expected attribute is missing from a variable.

        Returns:
        bool: True if all attributes are validated successfully.

        Notes:
        - The method uses a predefined list of mandatory attributes (`self.attributes_list_to_check`) that are expected
          to be present and consistent across the dataset.
        - The schema containing the expected attribute values for each variable is provided via `self.dataset_config`.
        - If a variable is missing from the `dataset_config`, a warning is logged.
        """

        errors = 0
        for var_name in ds.variables:
            # Iterate over each attribute in the list of mandatory attributes which should never change across a dataset
            for attr in self.attributes_list_to_check:
                # Iterate over the var_name attributes
                if attr in ds[var_name].attrs:
                    if var_name in self.dataset_config.get("schema"):
                        # check if an attribute exist in the dataset_config for a specific variable, and compare their similarity
                        if attr in self.dataset_config.get("schema")[var_name]:
                            expected_attr = self.dataset_config.get("schema")[var_name][
                                attr
                            ]
                            file_attr = getattr(ds[var_name], attr)

                            if expected_attr != file_attr:
                                # TODO: Do we really want to do this? I've rejected too some files with a valid attribute
                                #       degree different from Degrees. Should maybe do some fuzzy and have 90% of
                                #       similarity? Maybe dangerous. In the meantime, waiting to take a decision with
                                #       rest of the team, I prefer to set errors to 0
                                self.logger.error(
                                    f"{self.uuid_log}: Attribute '{attr}' for variable '{var_name}' does not match: expected '{expected_attr}', found '{file_attr}'"
                                )
                                # TODO: Uncomment below once found a good system
                                # errors += 1
                    else:
                        self.logger.warning(
                            f"{self.uuid_log}: {var_name} is missing from the dataset configuration. Please update the configuration."
                        )

        if errors > 0:
            return False
        else:
            return True

    def publish_cloud_optimised(
        self, df: pd.DataFrame, ds: xr.Dataset, s3_file_handle
    ) -> None:
        """
        Create a parquet file containing data only.

        Args:
            s3_file_handle: s3_file_handle
            df (pd.DataFrame): The pandas DataFrame containing the data.
            ds (Dataset): The dataset object.
        Returns:
            None
        """
        partition_keys = self.dataset_config["partition_keys"]

        df = self._add_timestamp_df(df)
        df = self._add_columns_df(df, ds, s3_file_handle)
        df = self._rm_bad_timestamp_df(df, s3_file_handle)
        if "polygon" in partition_keys:
            if not "spatial_extent" in self.dataset_config:
                self.logger.error(
                    f"{self.uuid_log}: Missing spatial_extent from dataset configuration"
                )
                # raise ValueError
            else:
                df = self._add_polygon(df)

        filename = os.path.basename(s3_file_handle.path)

        # Needs to be specified here as df is here a pandas df, while later on, it is a pyarrow table. some renaming should happen
        if isinstance(df.index, pd.MultiIndex):
            df_var_list = df.columns.tolist() + [name for name in df.index.names]
        else:
            df_var_list = list(df.columns) + [df.index.name]

        pdf = pa.Table.from_pandas(df)  # Convert pandas DataFrame to PyArrow Table

        # Part A: casting existing columns to correct type
        # In the following part, we have to create a hugly hack which highlights the immaturity of pyarrow. Basically if some
        # variables are null in a netcdf, the type is not recorded. we have to cast every variable with the appropriate type manually,
        # following a predefined schema. BUT of course nothing work as expected, and if some variables are missing in a file, well
        # we have to create a subset of the original schema ... fun fun fun
        # Get the names of columns present in the PyArrow table
        df_columns = pdf.schema.names

        # Create a new list of fields for the subset schema
        subset_fields = []

        # Iterate over the fields in the schema and keep only those present in pyarrow_columns
        if self.pyarrow_schema is not None:
            for field in self.pyarrow_schema:
                if field.name in df_columns:
                    subset_fields.append(field)

            # Create the subset pyarrow_schema using the filtered fields
            subset_schema = pa.schema(subset_fields)

            try:
                # see Github issue https://github.com/apache/arrow/issues/27425
                # df = df.cast(subset_schema)  # shittiest function ever. have to implement my own ...
                # df.cast fails complaining that the schemas are different while they're arent. different order is often the case
                pdf = self.cast_table_by_schema(pdf, subset_schema)
            except ValueError as e:
                self.logger.error(f"{filename}: {type(e).__name__}")

        # Part B: Create NaN arrays for missing columns in the pyarrow table by comparing the self.pyarrow_schema variable
        if self.pyarrow_schema is not None:
            for field in self.pyarrow_schema:
                if field.name not in df_var_list:
                    self.logger.warning(
                        f"{self.uuid_log}: {filename}: {field.name}; variable missing from input file. creating a null array of {field.type}"
                    )
                    null_array = pa.nulls(len(pdf), field.type)
                    pdf = pdf.append_column(field.name, null_array)

        # Part C: we need to report missing variables from the given pyarrow_schema, as by default, these variables
        # will not appear (unless a pyarrow_schema is provided) during a query by a use
        if self.pyarrow_schema is not None:
            for column_name in df_columns:
                if column_name not in pdf.schema.names:
                    var_config = generate_json_schema_var_from_netcdf(
                        s3_file_handle, column_name, s3_fs=self.s3_fs
                    )
                    # if df.index.name is not None and column_name in df.index.name:
                    #    self.logger.warning(f'missing variable from provided pyarrow_schema, please add {column_name} : {df.index.dtype}')
                    # else:
                    #    #TODO: improve this to return all the varatts as well
                    #    var_config = generate_json_schema_var_from_netcdf(self.input_object_key, column_name)
                    self.logger.warning(
                        f"{self.uuid_log}: {filename}; Variable missing from provided pyarrow_schema configuration. Please add to dataset configuration (ensure correct quoting): {var_config}"
                    )

        for partition_key in partition_keys:
            if all(not elem for elem in pdf[partition_key].is_null()):
                self.logger.error(
                    f"{self.uuid_log}: The '{partition_key}' variable is filled with NULL values, likely because '{partition_key}' is missing from 'gattrs_to_variables' in the dataset configuration."
                )
                raise ValueError

        metadata_collector = []
        pq.write_to_dataset(
            pdf,
            root_path=self.cloud_optimised_output_path,
            filesystem=self.s3_fs,
            existing_data_behavior="overwrite_or_ignore",
            row_group_size=20000,
            partition_cols=partition_keys,
            use_threads=True,
            metadata_collector=metadata_collector,
            basename_template=filename
            + "-{i}.parquet",  # this is essential for the overwriting part
        )
        # TODO: when running on a remote cluster, it seems like we only get a logger per batch? maybe the logger is closed?
        self.logger.info(
            f"{self.uuid_log}: {filename}: Parquet files successfully published to {self.cloud_optimised_output_path} \n"
        )

        self._add_metadata_sidecar()

    def _add_metadata_sidecar(self) -> None:
        """
        Adds metadata from json config as sidecar attributes.

        Args:

        Returns:
            None
        """
        ########################################################################
        # Section to create the dataset_metadata file at the root of the dataset
        ########################################################################
        # Ensure attribute names and values are bytes
        # see https://arrow.apache.org/docs/python/generated/pyarrow.field.html#pyarrow-field
        # Optional field metadata, the keys and values must be coercible to bytes.
        #
        # see also https://github.com/apache/arrow/issues/38575
        #
        # basically it's horrible. The doc is extremely poor. There is no standard way to create metadata...
        # a dict, a string? where to put the sidecar file? It's pretty poor implementation
        #
        # Create an empty list to store fields
        fields = []
        byte_dict_list = []
        # Iterate over variables in the PyArrow table (pdf)
        # for col_name in pdf.column_names:
        # Check if the variable exists in the xarray Dataset
        # if col_name in ds.variables:
        # Get the xarray variable
        #    var_data = ds.variables[col_name]

        # Convert xarray variable attributes to PyArrow column metadata
        # column_metadata = {}
        # for attr_name, attr_value in var_data.attrs.items():
        ## Ensure attribute values are strings
        # attr_value_str = str(attr_value)
        # column_metadata[attr_name] = attr_value_str
        fields = []
        byte_dict_list = []
        column_metadata = {}
        for var in self.dataset_config.get("schema"):
            # column_metadata[attr_name] = attr_value_str
            var_metadata = self.dataset_config.get("schema")[var]
            # Convert xarray variable values to PyArrow data type
            # Adjust data type mapping as needed based on your data
            if var_metadata["type"] == "float64":
                data_type = pa.float64()
            elif var_metadata["type"] == "float32":
                data_type = pa.float32()
            elif var_metadata["type"] == "float":
                data_type = pa.float32()
            elif var_metadata["type"] == "double":
                data_type = pa.float64()
            elif var_metadata["type"] == "int64":
                data_type = pa.int64()
            elif var_metadata["type"] == "int32":
                data_type = pa.int32()
            elif var_metadata["type"] == "int16":
                data_type = pa.int16()
            elif var_metadata["type"] == "int8":
                data_type = pa.int8()
            elif var_metadata["type"] == "uint64":
                data_type = pa.uint64()
            elif var_metadata["type"] == "uint32":
                data_type = pa.uint32()
            elif var_metadata["type"] == "uint16":
                data_type = pa.uint16()
            elif var_metadata["type"] == "uint8":
                data_type = pa.uint8()
            elif var_metadata["type"] == "bool":
                data_type = pa.bool_()
            elif var_metadata["type"] == "datetime64[ns]":
                data_type = pa.timestamp("ns")
            elif var_metadata["type"] == "timestamp[ns]":
                data_type = pa.timestamp("ns")
            elif var_metadata["type"] == "object":
                data_type = pa.string()
            elif var_metadata["type"] == "|S1":
                data_type = pa.string()
            elif var_metadata["type"] == "string":
                data_type = pa.string()
            else:
                raise ValueError(
                    f"Unsupported data type: {var_metadata['type']}  while creating metadata sidecar"
                )

            # TODO: once pyarrow matures on the metadata side, we should modify this ...
            # Create a PyArrow field with metadata
            # Convert all values in var_metadata to strings as pyarrow schema wants bytes..
            var_metadata_str = {key: str(value) for key, value in var_metadata.items()}

            field = pa.field(
                var, data_type, metadata=var_metadata_str
            )  # Here the metadata is properly attached as expected
            # Append the field to the list of fields
            fields.append(field)  # The metadata still exists here... Good sign

            # Because of some obscure reason, the above doesnt work as expected, the byte_dict_list is an alternative way to store the metadata

            byte_dict = str(var_metadata).encode("utf-8")
            byte_dict_list.append(byte_dict)

        # Create a PyArrow schema from the list of fields
        pdf_schema = pa.schema(fields)
        # above the fields is lost
        # TODO: to access the metadata by variable name do the following
        # Create a dictionary where keys are the names and values are the elements
        # schema_dict = {obj.name: obj for obj in pdf_schema}
        # Now you can access elements by name
        # schema_dict['TIMESERIES'].metadata.get(b'cf_role')  isntead of pdf_schema[0].metadata[b'cf_role'] but here the metadata is kinda lost, see https://github.com/apache/arrow/issues/38575

        # alternative way: need to create a horrible byte dict
        # var_atts_dict = {col_name: byte_dict for col_name, byte_dict in zip(pdf.column_names, byte_dict_list)}
        var_atts_dict = {
            col_name: byte_dict
            for col_name, byte_dict in zip(
                self.dataset_config.get("schema").keys(), byte_dict_list
            )
        }
        # Add Global attributes into metadata (no schema)
        dataset_metadata = dict()
        if "metadata_uuid" in self.dataset_config.keys():
            dataset_metadata["metadata_uuid"] = self.dataset_config["metadata_uuid"]
        if "dataset_gattrs" in self.dataset_config.keys():
            for gattr in self.dataset_config["dataset_gattrs"]:
                dataset_metadata[gattr] = self.dataset_config["dataset_gattrs"][gattr]
        # TODO: add a check this exists

        var_atts_dict["dataset_metadata"] = str(dataset_metadata).encode()

        pdf_schema = pdf_schema.with_metadata(var_atts_dict)

        dataset_metadata_path = os.path.join(
            self.cloud_optimised_output_path, "_common_metadata"
        )
        pq.write_metadata(
            pdf_schema,
            dataset_metadata_path,
            filesystem=self.s3_fs,
        )

        self.logger.info(
            f"{self.uuid_log}: Parquet metadata file successfully published to {dataset_metadata_path} \n"
        )

    def delete_existing_matching_parquet(self, filename) -> None:
        """
        Delete unmatched Parquet files.

        In scenarios where we reprocess files with similar filenames but potentially different content,
        which affects partition values, we may encounter a situation where the old Parquet files are
        not overwritten because they don't match the new ones. Although this scenario is unlikely, it
        is not impossible.

        The challenge arises when we need to list all existing Parquet objects on S3, which could
        take a significant amount of time (e.g., 15s+) and could become problematic if there are
        already a large number of objects (e.g., 50,000+). In such cases, caution should be exercised,
        and batch processing strategies may need to be implemented.

        Returns:
            None
        """

        self.logger.info(
            f"{self.uuid_log}: Searching for matching Parquet objects to delete."
        )

        # could be slow if there are too many objects to list
        # remote test on local machine shows 15 sec for 50k objects

        try:
            # TODO: with moto and unittests, we get the following error:
            #       GetFileInfo() yielded path 'imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/site_code=SYD140/timestamp=1625097600/polygon=01030000000100000005000000000000000020624000000000008041C0000000000060634000000000008041C0000000000060634000000000000039C0000000000020624000000000000039C0000000000020624000000000008041C0/IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc-0.parquet', which is outside base dir 's3://imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/'
            #       obviously the file to delete is found with the unittests, but there is an issue, maybe with the way filesystem is set. Reading with pandas works, but we don't have the same capabilities
            parquet_files = pq.ParquetDataset(
                self.cloud_optimised_output_path,
                partitioning="hive",
                filesystem=self.s3_fs,
            )
        except Exception as e:
            self.logger.info(
                f"{self.uuid_log}: No Parquet files to delete. Reason: {e}"
            )
            return

        # Define the regex pattern to match existing parquet files
        pattern = rf"\/{filename}"

        # Find files matching the pattern using list comprehension and regex
        matching_files = [
            file_path
            for file_path in parquet_files.files
            if re.search(pattern, file_path)
        ]

        # The matching files returns also the bucket name. We need to strip it out of the array
        object_keys = [
            file[len(self.optimised_bucket_name) :].lstrip("/")
            for file in matching_files
        ]
        if object_keys != []:
            objects_to_delete = [{"Key": key} for key in object_keys]

            s3 = boto3.client("s3")
            response = s3.delete_objects(
                Bucket=self.optimised_bucket_name, Delete={"Objects": objects_to_delete}
            )
            self.logger.info(
                f"{self.uuid_log}: Successfully deleted previous Parquet objects: {response}"
            )

    def to_cloud_optimised_single(self, s3_file_uri) -> None:
        """
        Process a single NetCDF file from an S3 URI, converting it into Parquet format.

        Args:
            s3_file_uri (str): The S3 URI of the NetCDF file to process.

        Returns:
            None

        This method processes a NetCDF file located at `s3_file_uri`:
        - Logs the processing start.
        - Deletes existing matching Parquet files if enabled (`self.delete_pq_unmatch_enable`).
        - Creates a fileset from the S3 file URI.
        - Calls `self.preprocess_data()` to preprocess the data, yielding DataFrame and Dataset.
        - Publishes the cloud-optimised data using `self.publish_cloud_optimised()`.
        - Performs post-processing tasks using `self.postprocess()`.
        - Logs completion time and finalises the process.

        If any exception occurs during processing, it logs the error and raises the exception.

        Note:
        - Uses the logger defined in `self.logger`.
        - Uses configurations and settings from `self.dataset_config`.
        """
        # FIXME: the next 2 lines need to be here otherwise, the logging is lost when called within a dask task. Why??
        # logger_name = self.dataset_config.get("logger_name", "generic")
        # self.logger = get_logger(logger_name)

        # if no value set per batch, we create one for per file processing
        if self.uuid_log is None:
            self.uuid_log = str(uuid.uuid4())

        self.logger.info(f"{self.uuid_log}: Processing file: {s3_file_uri}")

        filename = os.path.basename(s3_file_uri)
        if self.delete_pq_unmatch_enable:
            self.delete_existing_matching_parquet(filename)

        try:
            start_time = timeit.default_timer()

            s3_file_handle = create_fileset(s3_file_uri, self.s3_fs)[0]  # only one file

            generator = self.preprocess_data(s3_file_handle)
            for df, ds in generator:

                self.publish_cloud_optimised(df, ds, s3_file_handle)
                # self.push_metadata_aws_registry()  # Deprecated

                self.postprocess(ds)

                time_spent = timeit.default_timer() - start_time
                self.logger.info(
                    f"{self.uuid_log}: Cloud-optimised file processing completed in {time_spent} seconds."
                )

        except Exception as e:
            self.logger.error(
                f"{self.uuid_log}: Issue encountered while creating Cloud Optimised file: {type(e).__name__}: {e} \n {traceback.format_exc()}"
            )

            if "ds" in locals():
                self.postprocess(ds)

            raise e

    def to_cloud_optimised(self, s3_file_uri_list) -> None:
        """
        Process a list of NetCDF files from S3 URIs, converting them into Parquet format in batches.

        Args:
            s3_file_uri_list (list): List of S3 URIs of NetCDF files to process.

        Returns:
            None

        This method processes a list of NetCDF files located at `s3_file_uri_list`:
        - Deletes existing Parquet files if `self.clear_existing_data` is set to True.
        - Logs deletion of existing Parquet files if they exist.
        - Creates a Dask cluster and submits tasks to process each file URI in batches.
        - Waits for batch tasks to complete using a timeout of 10 minutes.
        - Closes the Dask cluster after all tasks are completed.

        Note:
        - Uses the logger defined in `self.logger`.
        - Uses configurations and settings from `self.dataset_config`.
        """
        if self.clear_existing_data:
            self.logger.info(
                f"Creating new Parquet dataset - DELETING existing all Parquet objects if exist"
            )
            if prefix_exists(self.cloud_optimised_output_path):
                bucket_name, prefix = split_s3_path(self.cloud_optimised_output_path)
                self.logger.info(
                    f"Deleting existing Parquet objects from {self.cloud_optimised_output_path}."
                )
                delete_objects_in_prefix(bucket_name, prefix)

        def task(f, i):
            try:
                self.to_cloud_optimised_single(f)
            except Exception as e:
                self.logger.error(
                    f"Issue {i}/{len(s3_file_uri_list)} with {f}: {type(e).__name__}: {e}"
                )

        self.s3_file_uri_list = s3_file_uri_list
        client, cluster = self.create_cluster()

        batch_size = self.get_batch_size(client=client)

        # Do it in batches. maybe more efficient
        ii = 0
        total_batches = len(s3_file_uri_list) // batch_size + 1

        for i in range(0, len(s3_file_uri_list), batch_size):
            self.uuid_log = str(uuid.uuid4())  # value per batch

            self.logger.info(
                f"{self.uuid_log}: Processing batch {ii + 1}/{total_batches}..."
            )

            batch = s3_file_uri_list[i : i + batch_size]
            batch_tasks = [
                client.submit(task, f, idx + 1, pure=False)
                for idx, f in enumerate(
                    batch
                )  # Use pure=False for multiprocessing. More efficient to avoid GIL contention
            ]

            # timeout = batch_size * 120  # Initial timeout
            done, not_done = wait(batch_tasks, return_when="ALL_COMPLETED")

            ii += 1

            # Cleanup memory
            del batch_tasks

            # Trigger garbage collection
            gc.collect()

        self.close_cluster(client, cluster)
        self.logger.handlers.clear()
