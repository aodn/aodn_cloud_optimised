import importlib.resources
import json
import os
import re
import tempfile
import timeit
from typing import List, Tuple, Generator

import boto3
import netCDF4
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import traceback
import xarray as xr
import yaml
from jsonschema import validate, ValidationError
from shapely.geometry import Point, Polygon

from .config import load_variable_from_config, load_dataset_config
from .logging import get_logger
from .schema import create_pyarrow_schema, generate_json_schema_var_from_netcdf

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
                force_old_pq_del (bool, optional[config]): Force the deletion of existing cloud optimised files(slow) (default=False)

        """
        super().__init__(**kwargs)

        self.delete_pq_unmatch_enable = kwargs.get('force_old_pq_del',
                                                   self.dataset_config.get("force_old_pq_del", False))

        json_validation_path = str(importlib.resources.path("aodn_cloud_optimised.config", "schema_validation_parquet.json"))
        self.validate_json(json_validation_path)  # we cannot validate the json config until self.dataset_config and self.logger are set

        self.partition_period = self.dataset_config["time_extent"].get('partition_timestamp_period', 'M')

        self.pyarrow_schema = create_pyarrow_schema(self.dataset_config['schema'])

    def preprocess_data_csv(self, csv_fp) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """
        Preprocesses a CSV file using pandas and converts it into an xarray Dataset based on dataset configuration.

        Args:
            csv_fp (str): File path to the CSV file to be processed.

        Yields:
            Tuple[pd.DataFrame, xr.Dataset]: A generator yielding a tuple containing the processed pandas DataFrame
                and its corresponding xarray Dataset.

        This method reads a CSV file, csv_fp using pandas read_csv function with configuration options
        specified in the dataset configuration (stored in 'pandas_read_csv_config' key of self.dataset_config, expected
        to be a JSON-like dictionary). The resulting DataFrame is then converted into an xarray Dataset using
        xr.Dataset.from_dataframe().

        i.e.:
           "pandas_read_csv_config": {
              "delimiter": ";",
              "header": 0,
              "index_col": "detection_timestamp",
              "parse_dates": ["detection_timestamp"],
              "na_values": ["N/A", "NaN"],
              "encoding": "utf-8"
              },

              See pandas.read_csv Documentation for more options

        The method also uses the 'schema' from the dataset configuration to assign attributes to variables in the
        xarray Dataset. Each variable's attributes are extracted from the 'schema' and assigned to the Dataset variable's
        attributes. The 'type' attribute from the pyarrow_schema is removed from the Dataset variables' attributes since it
        is considered unnecessary.

        If a variable in the Dataset is not found in the schema, an error is logged.

        """
        if "pandas_read_csv_config" in self.dataset_config:
            config_from_json = self.dataset_config["pandas_read_csv_config"]
            df = pd.read_csv(csv_fp, **config_from_json)
        else:
            self.logger.warning("No options given to process CSV file with pandas. Using default pandas.read_csv configuration")
            df = pd.read_csv(csv_fp)

        ds = xr.Dataset.from_dataframe(df)

        for var in ds.variables:
            if var not in self.schema:
                self.logger.error(f"Missing variable: {var} from config")
            else:
                ds[var].attrs = self.schema.get(var)
                del ds[var].attrs['type']  # remove the type attribute which is not necessary at all

        yield df, ds

    def preprocess_data_netcdf(self, netcdf_fp) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """
        Generate DataFrame and Dataset from a NetCDF file.
        If the dataset is more complicated, this method could be rewritten in a custom class inheriting
        the GenericHandler class with super() for method delegation.

        Args:
            netcdf_fp (str): Input NetCDF filepath.

        Yields:
            tuple: A tuple containing DataFrame and Dataset.
        """

        if self.is_valid_netcdf(netcdf_fp):
            # Use open_dataset as a context manager to ensure proper handling of the dataset
            with xr.open_dataset(netcdf_fp) as ds:
                # Convert xarray to pandas DataFrame
                df = ds.to_dataframe()

                yield df, ds

    def preprocess_data(self, fp) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        if fp.endswith('.nc'):
            return self.preprocess_data_netcdf(fp)
        if fp.endswith('.csv'):
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
        str_df = str_df.stack().str.decode('utf-8').unstack()
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
        polygon_coords = [(rounded_lon - delta, rounded_lat - delta),
                          (rounded_lon + delta, rounded_lat - delta),
                          (rounded_lon + delta, rounded_lat + delta),
                          (rounded_lon - delta, rounded_lat + delta)]

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
            self.logger.error("Missing spatial_extent config")
            raise ValueError

        # load default values if not available in config
        lat_varname = self.dataset_config["spatial_extent"].get("lat", "LATITUDE")
        lon_varname = self.dataset_config["spatial_extent"].get("lon", "LONGITUDE")
        spatial_res = self.dataset_config["spatial_extent"].get("spatial_resolution", 5)  # Define delta for the polygon (in degrees)

        # Clean dataset from NaN values of LAT and LON; for ex 'IMOS/Argo/dac/csiro/5905017/5905017_prof.nc'
        for geo_var in [lat_varname, lon_varname]:
            geo_var_has_nan = df[geo_var].isna().any().any()
            if geo_var_has_nan:
                self.logger.warning(f"The NetCDF has NaN values of {geo_var}. Removing corresponding data")
                df = df.dropna(subset=[geo_var]).reset_index()#.reset_index(drop=True)

        point_geometry = [Point(lon, lat) for lon, lat in zip(df[lon_varname], df[lat_varname])]

        # Create Polygon objects around each Point

        df['polygon'] = [self.create_polygon(point, spatial_res) for point in point_geometry]

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
        elif isinstance(df.index, pd.Index) and df.index.name is not None and (time_varname in df.index.name):
            datetime_var = df.index
        else:
            # for example, soop xbt nrt profiles where the index is the pressure and TIME is a variable
            for column in df.columns:
                if (df[column].dtype == 'datetime64[ns]') and column == time_varname:
                    datetime_var = df[column].values

            if 'datetime_var' not in locals():
                if pd.api.types.is_datetime64_any_dtype(df.index):
                    datetime_var = df.index
        df['timestamp'] = np.int64(pd.to_datetime(datetime_var).to_period(self.partition_period).to_timestamp())/10**9  # for partitions with the date as the 1st of the month

        return df

    def _add_columns_df(self, df: pd.DataFrame, ds: xr.Dataset) -> pd.DataFrame:
        """
        Adds filename column to the DataFrame as well as variables defined in the json config.

        Parameters:
            df (pd.DataFrame): Input DataFrame.
            ds (YourDataSetClass): Dataset object containing site_code information.

        Returns:
            pd.DataFrame: DataFrame with added columns.
        """

        gattrs_to_variables = self.dataset_config['gattrs_to_variables']
        for attr in gattrs_to_variables:
            if attr in ds.attrs:
                df[attr] = getattr(ds, attr)
            else:
                self.logger.warning(f"{attr} global attribute doesn't exist in the original NetCDF. The corresponding variable won't be created")

        df["filename"] = os.path.basename(self.input_object_key)

        return df

    def _rm_bad_timestamp_df(self, df: pd.DataFrame) -> pd.DataFrame:
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

        if any(df['timestamp'] < 0):
            self.logger.warning(f'{self.filename}: NaN values of {time_varname} time variable in dataset. Trimming data from NaN values')
            df2 = df[df['timestamp'] > 0].copy()
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

    def publish_cloud_optimised(self, df: pd.DataFrame, ds: xr.Dataset,) -> None:
        """
        Create a parquet file containing data only.

        Args:
            df (pd.DataFrame): The pandas DataFrame containing the data.
            ds (Dataset): The dataset object.
        Returns:
            None
        """
        partition_keys = self.dataset_config['partition_keys']

        df = self._add_timestamp_df(df)
        df = self._add_columns_df(df, ds)
        df = self._rm_bad_timestamp_df(df)

        if "polygon" in partition_keys:
            if not "spatial_extent" in self.dataset_config:
                self.logger.error("Missing spatial_extent config")
                #raise ValueError
            else:
                df = self._add_polygon(df)

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
                self.logger.error(f"{self.filename}: {type(e).__name__}")

        # Part B: Create NaN arrays for missing columns in the pyarrow table by comparing the self.pyarrow_schema variable
        if self.pyarrow_schema is not None:
            for field in self.pyarrow_schema:
                if field.name not in df_var_list:
                    self.logger.warning(f"{self.filename}: {field.name} variable missing from dataset. creating a null array of {field.type}")
                    null_array = pa.nulls(len(pdf), field.type)
                    pdf = pdf.append_column(field.name, null_array)

        # Part C: we need to report missing variables from the given pyarrow_schema, as by default, these variables
        # will not appear (unless a pyarrow_schema is provided) during a query by a use
        if self.pyarrow_schema is not None:
            for column_name in df_columns:
                if column_name not in pdf.schema.names:
                    var_config = generate_json_schema_var_from_netcdf(self.tmp_input_file, column_name)
                    #if df.index.name is not None and column_name in df.index.name:
                    #    self.logger.warning(f'missing variable from provided pyarrow_schema, please add {column_name} : {df.index.dtype}')
                    #else:
                    #    #TODO: improce this to return all the varatts as well
                    #    var_config = generate_json_schema_var_from_netcdf(self.input_object_key, column_name)
                    self.logger.warning(
                        f'missing variable from provided pyarrow_schema config, please add to dataset config (respect double quotes): {var_config}')

        for partition_key in partition_keys:
            if all(not elem for elem in pdf[partition_key].is_null()):
                self.logger.error(f"{partition_key} variable is full of NULL values. Most likely due to {partition_key} missing from \"gattrs_to_variables\" in dataset config")
                raise ValueError

        metadata_collector = []
        pq.write_to_dataset(pdf,
                            root_path=self.cloud_optimised_output_path,
                            existing_data_behavior="overwrite_or_ignore",
                            row_group_size=20000,
                            partition_cols=partition_keys,
                            use_threads=True,
                            metadata_collector=metadata_collector,
                            basename_template=os.path.basename(self.input_object_key) + "-{i}.parquet"  # this is essential for the overwriting part
                            )
        self.logger.info(f"{self.filename}: Parquet files successfully created in {self.cloud_optimised_output_path} \n")

        self._add_metadata_sidecar()#pdf, ds)

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
        #for col_name in pdf.column_names:
            # Check if the variable exists in the xarray Dataset
            #if col_name in ds.variables:
                # Get the xarray variable
            #    var_data = ds.variables[col_name]

                # Convert xarray variable attributes to PyArrow column metadata
                #column_metadata = {}
                #for attr_name, attr_value in var_data.attrs.items():
                    ## Ensure attribute values are strings
                    # attr_value_str = str(attr_value)
                    # column_metadata[attr_name] = attr_value_str
        fields = []
        byte_dict_list = []
        column_metadata = {}
        for var in self.dataset_config.get('schema'):
            #column_metadata[attr_name] = attr_value_str
            var_metadata = self.dataset_config.get('schema')[var]
            # Convert xarray variable values to PyArrow data type
            # Adjust data type mapping as needed based on your data
            if var_metadata['type'] == 'float64':
                data_type = pa.float64()
            elif var_metadata['type'] == 'float32':
                data_type = pa.float32()
            elif var_metadata['type'] == 'float':
                data_type = pa.float32()
            elif var_metadata['type'] == 'double':
                data_type = pa.float64()
            elif var_metadata['type'] == 'int64':
                data_type = pa.int64()
            elif var_metadata['type'] == 'int32':
                data_type = pa.int32()
            elif var_metadata['type'] == 'int16':
                data_type = pa.int16()
            elif var_metadata['type'] == 'int8':
                data_type = pa.int8()
            elif var_metadata['type'] == 'uint64':
                data_type = pa.uint64()
            elif var_metadata['type'] == 'uint32':
                data_type = pa.uint32()
            elif var_metadata['type'] == 'uint16':
                data_type = pa.uint16()
            elif var_metadata['type'] == 'uint8':
                data_type = pa.uint8()
            elif var_metadata['type'] == 'bool':
                data_type = pa.bool_()
            elif var_metadata['type'] == 'datetime64[ns]':
                data_type = pa.timestamp('ns')
            elif var_metadata['type'] == 'timestamp[ns]':
                data_type = pa.timestamp('ns')
            elif var_metadata['type'] == 'object':
                data_type = pa.string()
            elif var_metadata['type'] == '|S1':
                data_type = pa.string()
            elif var_metadata['type'] == 'string':
                data_type = pa.string()
            else:
                raise ValueError(f"Unsupported data type: {var_metadata['type']}  while creating metadata sidecar")

            # TODO: once pyarrow matures on the metadata side, we should modify this ...
            # Create a PyArrow field with metadata
            # Convert all values in var_metadata to strings as pyarrow schema wants bytes..
            var_metadata_str = {key: str(value) for key, value in var_metadata.items()}

            field = pa.field(var, data_type, metadata=var_metadata_str)  # Here the metadata is properly attached as expected
            # Append the field to the list of fields
            fields.append(field)  # The metadata still exists here... Good sign

            # Because of some obscure reason, the above doesnt work as expected, the byte_dict_list is an alternative way to store the metadata

            byte_dict = str(var_metadata).encode('utf-8')
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
        #var_atts_dict = {col_name: byte_dict for col_name, byte_dict in zip(pdf.column_names, byte_dict_list)}
        var_atts_dict = {col_name: byte_dict for col_name, byte_dict in zip(self.dataset_config.get('schema').keys(), byte_dict_list)}
        # Add Global attributes into metadata (no schema)
        dataset_metadata = dict()
        if "metadata_uuid" in self.dataset_config.keys():
            dataset_metadata["metadata_uuid"] = self.dataset_config['metadata_uuid']
        if "dataset_gattrs" in self.dataset_config.keys():
            for gattr in self.dataset_config["dataset_gattrs"]:
                dataset_metadata[gattr] = self.dataset_config["dataset_gattrs"][gattr]
        # TODO: add a check this exists

        var_atts_dict["dataset_metadata"] = str(dataset_metadata).encode()

        pdf_schema = pdf_schema.with_metadata(var_atts_dict)

        dataset_metadata_path = os.path.join(self.cloud_optimised_output_path, '_common_metadata')
        pq.write_metadata(pdf_schema, dataset_metadata_path)

        self.logger.info(f"{self.filename}: Parquet metadata file successfully created in {dataset_metadata_path} \n")

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
            self.logger.warning("Missing dataset configuration to populate AWS OpenData Registry")
        else:
            aws_registry_config = self.dataset_config["aws_opendata_registry"]
            yaml_data = yaml.dump(aws_registry_config)

            s3 = boto3.client('s3')

            key = os.path.join(self.root_prefix_cloud_optimised_path, self.dataset_name + '.yaml')
            # Upload the YAML data to S3
            s3.put_object(
                Bucket=self.optimised_bucket_name,
                Key=key,
                Body=yaml_data.encode('utf-8')
            )
            self.logger.info(f"Push AWS Registry file to: {os.path.join(self.cloud_optimised_output_path, self.dataset_name + '.yaml')}")

    def delete_existing_matching_parquet(self) -> None:
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

        self.logger.info(f'Looking for matching Parquet objects to delete')

        # could be slow if there are too many objects to list
        # remote test on local machine shows 15 sec for 50k objects

        try:
            parquet_files = pq.ParquetDataset(self.cloud_optimised_output_path, partitioning='hive')
        except FileNotFoundError as e:
            self.logger.info(f"No files to delete: {e}")
            return

        # Define the regex pattern to match existing parquet files
        pattern = rf'\/{self.filename}'

        # Find files matching the pattern using list comprehension and regex
        matching_files = [file_path for file_path in parquet_files.files if re.search(pattern, file_path)]

        # The matching files returns also the bucket name. We need to strip it out of the array
        object_keys = [file[len(self.optimised_bucket_name):].lstrip('/') for file in matching_files]
        if object_keys != []:
            objects_to_delete = [{'Key': key} for key in object_keys]

            s3 = boto3.client('s3')
            response = s3.delete_objects(
                Bucket=self.optimised_bucket_name,
                Delete={
                    'Objects': objects_to_delete
                }
            )
            self.logger.info(f'Previous parquet objects successfully deleted: {response}')

    def to_cloud_optimised(self) -> None:
        """
        Create Parquet files from a NetCDF file.

        Returns:
            None
        """
        if self.tmp_input_file.endswith(".nc"):
            self.is_valid_netcdf(self.tmp_input_file)  # check file validity before doing anything else

        if self.delete_pq_unmatch_enable:
            self.delete_existing_matching_parquet()

        try:
            generator = self.preprocess_data(self.tmp_input_file)
            for df, ds in generator:

                self.publish_cloud_optimised(df, ds)
                self.push_metadata_aws_registry()

                time_spent = (timeit.default_timer() - self.start_time)
                self.logger.info(f'Cloud Optimised file completed in {time_spent}s')

                self.postprocess(ds)

        except Exception as e:
            self.logger.error(f"Issue while creating Cloud Optimised file: {type(e).__name__}: {e} \n {traceback.print_exc()}")

            if 'ds' in locals():
                self.postprocess(ds)

            raise e
