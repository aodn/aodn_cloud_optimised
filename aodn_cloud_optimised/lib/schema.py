import importlib.resources
import json
import logging
import re
import tempfile
from typing import Dict

import numpy as np
import polars as pl
import pyarrow as pa
import s3fs
import xarray as xr

logger = logging.getLogger(__name__)

# Standard NetCDF-style fill values
FILL_VALUES = {
    str: "",
    int: -9999,
    float: -9999.0,
    bool: False,  # often False is used for empty/missing
    np.int32: np.int32(-9999),
    np.int64: np.int64(-9999),
    np.float32: np.float32(-9999.0),
    np.float64: np.float64(-9999.0),
}


def custom_encoder(obj):
    if isinstance(obj, np.generic):
        return obj.item()  # Convert NumPy scalar to its corresponding Python type
    elif isinstance(obj, np.ndarray):
        return obj.tolist()  # Convert NumPy array to a list
    raise TypeError(f"Object {obj} of type {type(obj)} is not JSON serializable")


def generate_json_schema_var_from_netcdf(nc_path, var_name, indent=2, s3_fs=None):
    """
    Extracts variable names, types, and attributes from a NetCDF file and returns a JSON-formatted schema.

    Args:
        nc_path (str or S3File): Path to a local NetCDF file or S3 address of the NetCDF file,
                                 e.g., "s3://your-bucket/path/to/file.nc", or an open S3File object.
        var_name (str): Name of the variable or coordinate to extract schema for.
        indent (int, optional): Number of spaces for JSON indentation (default is 2).
        s3_fs (s3fs.S3FileSystem, optional): S3FileSystem instance used to open S3 objects (default is None).

    Returns:
        str: JSON-formatted string representing the variable schema.
    """
    if isinstance(nc_path, s3fs.S3File):
        if s3_fs is None:
            s3_fs = s3fs.S3FileSystem(
                anon=False,  # because we are authenticating
            )

        # Open dataset from S3 file-like object using with statement
        with s3_fs.open(nc_path) as f:
            with xr.open_dataset(f) as dataset:
                schema = extract_variable_schema(dataset, var_name)
    elif nc_path.startswith("s3://"):
        with s3_fs.open(nc_path) as f:
            with xr.open_dataset(f) as dataset:
                schema = extract_variable_schema(dataset, var_name)
    else:
        with xr.open_dataset(nc_path) as dataset:
            schema = extract_variable_schema(dataset, var_name)

    json_str = json.dumps(schema, indent=indent, default=custom_encoder)

    return json_str


def extract_variable_schema(dataset, var_name):
    """
    Extracts variable schema (dtype and attributes) from an xarray Dataset or DataArray.

    Args:
        dataset (xarray.Dataset or xarray.DataArray): The xarray dataset or data array.
        var_name (str): Name of the variable or coordinate to extract schema for.

    Returns:
        dict: Dictionary representing the variable schema.
    """
    schema = {}

    def map_dtype(dtype):
        dtype_str = str(dtype)
        if dtype_str.startswith("float"):
            return "float"
        elif dtype_str.startswith("|S"):
            return "string"
        return dtype_str

    # Process variables
    if var_name in dataset.variables:
        var_dtype = dataset.variables[var_name].dtype
        dtype_str = map_dtype(var_dtype)
        var_attrs = dataset.variables[var_name].attrs
        schema[var_name] = {"type": dtype_str, **var_attrs}

    elif var_name in dataset.coords:
        coord_dtype = dataset.coords[var_name].dtype
        dtype_str = map_dtype(coord_dtype)
        coord_attrs = dataset.coords[var_name].attrs
        schema[var_name] = {"type": dtype_str, **coord_attrs}

    return schema


def generate_json_schema_from_s3_netcdf(
    s3_object_address, cloud_format, indent=2, s3_fs=None
):
    """
    Extracts variable names, types, and attributes from a NetCDF file in S3 and returns a JSON-formatted schema.

    Args:
        s3_object_address (str): The address of the NetCDF object in S3 format,
                                e.g., "s3://your-bucket/path/to/file.nc".
        indent (int, optional): Number of spaces for JSON indentation (default is 2).
        s3_fs (s3fs.S3FileSystem, optional): S3FileSystem instance used to open S3 objects (default is None).

    Returns:
        str: Path to a temporary JSON file containing the variable schema.
    """

    if s3_fs is None:
        s3_fs = s3fs.S3FileSystem(
            anon=False,  # because we are authenticating
        )

    from aodn_cloud_optimised.lib.s3Tools import (
        create_fileset,
    )

    fileset = create_fileset(s3_object_address, s3_fs)
    # with s3_fs.open(s3_object_address, "rb") as f:
    dataset = xr.open_dataset(fileset[0])

    schema = {}

    # Process variables
    for var_name in dataset.variables:
        var_dtype = dataset.variables[var_name].dtype
        dtype_str = convert_dtype_to_str(var_dtype)
        var_attrs = extract_serialisable_attrs(dataset.variables[var_name].attrs)
        if cloud_format == "zarr":
            schema[var_name] = {
                "type": dtype_str,
                "dims": list(dataset[var_name].dims),
                **var_attrs,
            }
        else:
            schema[var_name] = {
                "type": dtype_str,
                **var_attrs,
            }

    # Process coordinates
    for coord_name in dataset.coords:
        coord_dtype = dataset.coords[coord_name].dtype
        dtype_str = convert_dtype_to_str(coord_dtype)
        coord_attrs = extract_serialisable_attrs(dataset.coords[coord_name].attrs)
        schema[coord_name] = {
            "type": dtype_str,
            **coord_attrs,
        }

    # Convert the pyarrow_schema dictionary to a JSON-formatted string with indentation
    json_str = json.dumps(schema, indent=indent)

    # Print the JSON string with double quotes for easy copy/paste
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        # Serialise the pyarrow_schema dictionary to JSON with indentation
        json.dump(schema, temp_file, indent=indent)
        # Get the path to the temporary file
        temp_file_path = temp_file.name

    return temp_file_path


def convert_dtype_to_str(dtype):
    """Converts NumPy dtype to string representation."""
    if np.issubdtype(dtype, np.integer):
        return "int32"
    elif np.issubdtype(dtype, np.floating):
        return "double" if dtype == np.float64 else "float"
    elif np.issubdtype(dtype, np.datetime64):
        return "timestamp[ns]"  # string type understood by parrow, but not by np.issubdtype
    else:
        return "string"


def extract_serialisable_attrs(attrs):
    """Extracts and serialises attributes into a dictionary."""
    serialisable_attrs = {}
    for attr_name, attr_value in attrs.items():
        try:
            # Convert attribute value to string if possible
            if isinstance(attr_value, np.integer):
                serialisable_attrs[attr_name] = int(attr_value)
            elif isinstance(attr_value, np.floating):
                serialisable_attrs[attr_name] = float(attr_value)
            elif isinstance(attr_value, np.ndarray):
                serialisable_attrs[attr_name] = attr_value.tolist()
            else:
                serialisable_attrs[attr_name] = str(attr_value)
        except Exception:
            # If conversion fails, skip this attribute
            pass
    return serialisable_attrs


def get_pyarrow_type_map() -> Dict[str, pa.DataType]:
    """
    Returns a dictionary mapping schema config type strings to PyArrow DataTypes.
    """
    return {
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint8": pa.uint8(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
        "float": pa.float32(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "double": pa.float64(),
        "string": pa.string(),
        "object": pa.string(),
        "|S1": pa.string(),
        "byte": pa.binary(),
        "bool": pa.bool_(),
        "datetime64[ns]": pa.timestamp("ns"),
        "timestamp[ns]": pa.timestamp("ns"),
        "timestamp[ms]": pa.timestamp("ms"),
        "timedelta64[ns]": pa.duration("ns"),
        "date32[day]": pa.date32(),
        "time32[s]": pa.time32("s"),
        "time64[us]": pa.time64("us"),
    }


def get_polars_dtypes_from_pyarrow(
    schema: dict, pyarrow_map: Dict[str, pa.DataType]
) -> Dict[str, pl.DataType]:
    """
    Convert schema using PyArrow map into Polars dtypes.
    """
    pa_to_pl = {
        pa.int8(): pl.Int8,
        pa.int16(): pl.Int16,
        pa.int32(): pl.Int32,
        pa.int64(): pl.Int64,
        pa.uint8(): pl.UInt8,
        pa.uint16(): pl.UInt16,
        pa.uint32(): pl.UInt32,
        pa.uint64(): pl.UInt64,
        pa.float32(): pl.Float32,
        pa.float64(): pl.Float64,
        pa.string(): pl.Utf8,
        pa.bool_(): pl.Boolean,
        pa.timestamp("ns"): pl.Datetime("ns"),
        pa.timestamp("ms"): pl.Datetime("ms"),
        pa.date32(): pl.Date,
        pa.binary(): pl.Binary,
    }
    dtypes = {}
    for col, meta in schema.items():
        pa_type = pyarrow_map.get(meta["type"])
        if pa_type is not None:
            dtypes[col] = pa_to_pl.get(pa_type, pl.Utf8)
        else:
            dtypes[col] = pl.Utf8
    return dtypes


def map_config_type_to_pyarrow_type(config_type: str) -> pa.DataType:
    """
    Maps a schema config type string to a corresponding PyArrow DataType.

    Args:
        config_type (str): Type string (e.g. 'int32', 'string', 'timestamp[ns]').

    Returns:
        pa.DataType: The corresponding PyArrow type.

    Raises:
        ValueError: If the type is not supported.
    """
    type_map = get_pyarrow_type_map()
    if config_type not in type_map:
        raise ValueError(f"Unsupported data type: {config_type}")
    return type_map[config_type]


def create_pyarrow_schema_from_list(schema_strings):
    fields = []
    for line in schema_strings:
        name, dtype_str = map(str.strip, line.split(":"))
        dtype = map_config_type_to_pyarrow_type(dtype_str)
        fields.append(pa.field(name, dtype))
    return pa.schema(fields)


def create_pyarrow_schema_from_dict(schema_dict):
    """
    Create a PyArrow schema from a dict describing column names and types.

    Expected format example:
        {
            "col1": {"type": "string"},
            "col2": {"type": "int64"}
        }

    Args:
        schema_dict (dict): Mapping of column names to type definitions.

    Returns:
        pyarrow.Schema: The corresponding PyArrow schema.

    Raises:
        TypeError: If schema_dict or any column definition is not a dict.
        KeyError: If any column is missing a "type" key.
    """
    expected_format = {
        "example_column": {"type": "string"},
        "another_column": {"type": "int64"},
    }

    # --- Validate schema_dict structure ---
    if not isinstance(schema_dict, dict):
        message = (
            f"Invalid schema configuration. Expected a dictionary describing columns and types, "
            f"but got {type(schema_dict).__name__}.\n"
            f"Expected format:\n{json.dumps(expected_format, indent=4)}\n"
            "Please fix the schema config."
        )
        logger.error(message)
        raise TypeError(message)

    fields = []
    for name, info in schema_dict.items():
        if not isinstance(info, dict):
            message = (
                f"Invalid entry for column '{name}'. Expected a dict, got {type(info).__name__}.\n"
                f"Entry content: {info}\n"
                f"Expected format:\n{json.dumps(expected_format, indent=4)}\n"
                "Please fix the schema config."
            )
            logger.error(message)
            raise TypeError(message)

        if "type" not in info:
            message = (
                f"Missing 'type' key for column '{name}' in schema: {info}\n"
                f"Expected format:\n{json.dumps(expected_format, indent=4)}\n"
                "Please fix the schema config."
            )
            logger.error(message)
            raise KeyError(message)

        dtype_str = info["type"].strip()
        try:
            dtype = map_config_type_to_pyarrow_type(dtype_str)
        except Exception as e:
            message = (
                f"Failed to map type '{dtype_str}' for column '{name}': {e}\n"
                f"Please ensure the schema config matches this format:\n"
                f"{json.dumps(expected_format, indent=4)}"
            )
            logger.error(message)
            raise

        logger.debug(f"Added field: {name}, type: {dtype_str} → {dtype}")
        fields.append(pa.field(name, dtype))

    return pa.schema(fields)


def extract_new_variables_schema(schema_transformation: dict) -> dict:
    """
    Extracts new variables from the 'add_variables' section of a schema_transformation dict
    and returns a dictionary where each key is the variable name and the value is a dictionary
    that includes the 'type' and all schema fields from the original 'schema' entry.

    Args:
        schema_transformation (dict): The input dictionary containing the schema transformation.

    Returns:
        dict: A dictionary of variables with their full schema definitions.
    """
    new_variables_schema = {}
    add_variables = schema_transformation.get("add_variables", {})

    for var_name, var_info in add_variables.items():
        schema = var_info.get("schema", {})
        var_type = schema.get("type", "unknown")
        # Build new dictionary with 'type' and other schema attributes
        new_variables_schema[var_name] = {"type": var_type}
        for key, value in schema.items():
            if key != "type":
                new_variables_schema[var_name][key] = value

    return new_variables_schema


def merge_schema_dict(schema_input, schema_transformation):
    """
    Merge the orginal schema and the schema_transformation from a json configuration to output a new dict containing
    ALL of the variables in the dataset
    """
    new_variables_schema = extract_new_variables_schema(schema_transformation)

    if isinstance(schema_input, dict):
        schema_input.update(new_variables_schema)

    return schema_input


def create_pyarrow_schema(schema_input, schema_transformation=None):
    """
    handles 2 different ways to write the pyarrow_schema
    Args:
        schema_input:

    Returns:

    """
    # Apply schema transformations
    # Only valid for dict type
    if schema_transformation and isinstance(schema_input, dict):
        # Update new variables
        new_variables_schema = extract_new_variables_schema(schema_transformation)
        schema_input.update(new_variables_schema)

        # Drop unwanted variables
        drop_variables = schema_transformation.get("drop_variables", [])
        for var in drop_variables:
            schema_input.pop(var, None)  # safe removal

    if isinstance(schema_input, list):
        return create_pyarrow_schema_from_list(schema_input)
    elif isinstance(schema_input, dict):
        return create_pyarrow_schema_from_dict(schema_input)
    else:
        raise ValueError("Unsupported pyarrow_schema input type. Expected str or dict.")


def map_config_type_to_python_type(config_type: str):
    """
    Map config-defined type (as string) to a Python or NumPy type.

    Args:
        config_type (str): The type string from the schema config.

    Returns:
        type: A corresponding Python or NumPy type.
    """
    type_map = {
        "string": str,
        "int": int,
        "int32": np.int32,
        "int64": np.int64,
        "float": float,
        "float32": np.float32,
        "float64": np.float64,
        "bool": bool,
        # Extend as needed
    }
    return type_map.get(config_type, str)  # Default to str if unknown


def cast_value_to_config_type(value, config_type: str, fillvalue=None):
    """
    Cast a value to the type specified in the schema config.
    If the value is None, empty, or uncastable, return either the provided
    fillvalue or a sensible NetCDF-style default fill value.

    For numeric types, attempts to clean strings like "16m" -> 16.
    """
    python_type = map_config_type_to_python_type(config_type)

    # Missing or empty string
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return (
            python_type(fillvalue)
            if fillvalue is not None
            else FILL_VALUES.get(python_type, None)
        )

    # Try normal casting
    try:
        return python_type(value)
    except Exception:
        # Special case: try to extract numbers if config_type is numeric
        if python_type in [int, float, np.int32, np.int64, np.float32, np.float64]:
            if isinstance(value, str):
                match = re.match(r"^[-+]?\d*\.?\d+", value.strip())
                if match:
                    try:
                        return python_type(match.group(0))
                    except Exception:
                        pass  # fallback to fillvalue/default

        # If still failing → use fillvalue or fallback
        return (
            python_type(fillvalue)
            if fillvalue is not None
            else FILL_VALUES.get(python_type, None)
        )


def nullify_netcdf_variables(nc_path, dataset_name, s3_fs=None):
    """
    Replace all non-dimension variables in a NetCDF file with NaN, compress the result,
    and save it under aodn_cloud_optimised/config/dataset/{dataset_name}.nc.

    Args:
        nc_path (str or s3fs.S3File): Path to the NetCDF file (local or S3).
        dataset_name (str): Name for the output NetCDF file (without extension).
        s3_fs (s3fs.S3FileSystem, optional): S3FileSystem instance (defaults to anon).

    Returns:
        str: Path to the output NetCDF file.
    """
    output_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("config")
        .joinpath("dataset")
        .joinpath(f"{dataset_name}.nc")
    )

    if isinstance(nc_path, s3fs.S3File):
        if s3_fs is None:
            s3_fs = s3fs.S3FileSystem(anon=True)
        with s3_fs.open(nc_path) as f:
            with xr.open_dataset(f) as ds:
                _write_nullified_dataset(ds, output_path)
    elif isinstance(nc_path, str) and nc_path.startswith("s3://"):
        if s3_fs is None:
            s3_fs = s3fs.S3FileSystem(anon=True)
        with s3_fs.open(nc_path) as f:
            with xr.open_dataset(f) as ds:
                _write_nullified_dataset(ds, output_path)
    else:
        with xr.open_dataset(nc_path) as ds:
            _write_nullified_dataset(ds, output_path)

    return output_path


def _write_nullified_dataset(ds, output_path):
    """
    Internal helper to nullify variables (retain dtypes) and write compressed NetCDF.

    Args:
        ds (xarray.Dataset): The dataset to modify.
        output_path (str): Destination file path.
    """
    ds_null = ds.copy()
    encoding = {}

    for var in ds_null.data_vars:
        data = ds_null[var].data
        dtype = data.dtype

        if np.issubdtype(dtype, np.floating):
            # Floats can store np.nan directly
            ds_null[var].data = np.full_like(data, np.nan)
            encoding[var] = {"zlib": True, "complevel": 4}
        elif np.issubdtype(dtype, np.integer):
            # Integers can't hold NaNs — use _FillValue
            fill_value = np.iinfo(dtype).min
            ds_null[var].data = np.full_like(data, fill_value)
            encoding[var] = {"zlib": True, "complevel": 4, "_FillValue": fill_value}
        else:
            # For non-numeric or object dtypes, zero them out or set to blank
            ds_null[var].data = np.full_like(data, 0)
            encoding[var] = {"zlib": True, "complevel": 4}

    ds_null.to_netcdf(output_path, encoding=encoding, engine="netcdf4")


def convert_pandas_csv_config_to_polars(pandas_config: dict) -> dict:
    """
    Convert a pandas.read_csv configuration dictionary to a polars.read_csv equivalent.

    Args:
        pandas_config (dict): Configuration options for pandas.read_csv.

    Returns:
        dict: Converted configuration suitable for polars.read_csv.
    """
    mapping = {
        "delimiter": "separator",
        "sep": "separator",
        "encoding": "encoding",
        "na_values": "null_values",
        "header": "has_header",
        "usecols": "columns",
    }

    polars_config = {}

    for key, value in pandas_config.items():
        if key not in mapping:
            # Skip options not relevant for polars
            continue

        new_key = mapping[key]

        # --- Handle special cases ---
        if key == "header":
            # In pandas, header=0 means first row is header; None means no header
            polars_config[new_key] = value is not None
        elif key == "na_values":
            # Pandas allows list or scalar; Polars expects list[str]
            if isinstance(value, str):
                polars_config[new_key] = [value]
            elif isinstance(value, (list, tuple, set)):
                polars_config[new_key] = list(value)
        else:
            polars_config[new_key] = value

    return polars_config
