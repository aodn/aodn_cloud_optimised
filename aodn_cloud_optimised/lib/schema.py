import json
import tempfile

import numpy as np
import pyarrow as pa
import s3fs
import xarray as xr


def custom_encoder(obj):
    if isinstance(obj, np.float32):
        return float(obj)  # Convert np.float32 to Python float
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


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
            s3_fs = s3fs.S3FileSystem(anon=True)

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

    # Process variables
    if var_name in dataset.variables:
        var_dtype = dataset.variables[var_name].dtype
        dtype_str = str(var_dtype)
        var_attrs = dataset.variables[var_name].attrs
        schema[var_name] = {"type": dtype_str, **var_attrs}

    elif var_name in dataset.coords:
        coord_dtype = dataset.coords[var_name].dtype
        dtype_str = str(coord_dtype)
        coord_attrs = dataset.coords[var_name].attrs
        schema[var_name] = {"type": dtype_str, **coord_attrs}

    return schema


def generate_json_schema_from_s3_netcdf(s3_object_address, indent=2, s3_fs=None):
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
        s3_fs = s3fs.S3FileSystem(anon=True)

    with s3_fs.open(s3_object_address, "rb") as f:
        dataset = xr.open_dataset(f)

    schema = {}

    # Process variables
    for var_name in dataset.variables:
        var_dtype = dataset.variables[var_name].dtype
        dtype_str = convert_dtype_to_str(var_dtype)
        var_attrs = extract_serialisable_attrs(dataset.variables[var_name].attrs)
        schema[var_name] = {"type": dtype_str, **var_attrs}

    # Process coordinates
    for coord_name in dataset.coords:
        coord_dtype = dataset.coords[coord_name].dtype
        dtype_str = convert_dtype_to_str(coord_dtype)
        coord_attrs = extract_serialisable_attrs(dataset.coords[coord_name].attrs)
        schema[coord_name] = {"type": dtype_str, **coord_attrs}

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


def create_pyarrow_schema_from_list(schema_strings):
    fields = []
    for line in schema_strings:
        name, dtype_str = line.split(":")
        name = name.strip()
        dtype_str = dtype_str.strip()

        # Convert dtype string to PyArrow type
        if dtype_str == "int32":
            dtype = pa.int32()
        elif dtype_str == "int64":
            dtype = pa.int64()
        elif dtype_str == "double":
            dtype = pa.float64()  # assuming 'double' refers to float64
        elif dtype_str == "float":
            dtype = pa.float32()  # assuming 'float' refers to float32
        elif dtype_str == "string":
            dtype = pa.string()
        elif dtype_str == "byte":
            dtype = pa.binary()
        elif dtype_str == "timestamp[ns]":
            dtype = pa.timestamp("ns")
        else:
            raise ValueError(f"Unsupported data type: {dtype_str}")

        fields.append(pa.field(name, dtype))

    schema = pa.schema(fields)
    return schema


def create_pyrarrow_schema_from_dict(schema_dict):
    fields = []
    for name, info in schema_dict.items():
        dtype_str = info["type"].strip()

        # Convert dtype string to PyArrow type
        if dtype_str == "int32":
            dtype = pa.int32()
        elif dtype_str == "int64":
            dtype = pa.int64()
        elif dtype_str == "double":
            dtype = pa.float64()  # assuming 'double' refers to float64
        elif dtype_str == "float":
            dtype = pa.float32()  # assuming 'float' refers to float32
        elif dtype_str == "string":
            dtype = pa.string()
        elif dtype_str == "byte":
            dtype = pa.binary()
        elif dtype_str == "timestamp[ns]":
            dtype = pa.timestamp("ns")
        else:
            raise ValueError(f"Unsupported data type: {dtype_str}")

        # Create PyArrow field
        fields.append(pa.field(name, dtype))

    # Create PyArrow pyarrow_schema from the list of fields
    schema = pa.schema(fields)
    return schema


def create_pyarrow_schema(schema_input):
    """
    handles 2 different ways to write the pyarrow_schema
    Args:
        schema_input:

    Returns:

    """
    if isinstance(schema_input, list):
        return create_pyarrow_schema_from_list(schema_input)
    elif isinstance(schema_input, dict):
        return create_pyrarrow_schema_from_dict(schema_input)
    else:
        raise ValueError("Unsupported pyarrow_schema input type. Expected str or dict.")
