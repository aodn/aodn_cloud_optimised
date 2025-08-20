"""
A currated list of functions used to facilitate reading AODN parquet files. These are used by the various Jupyter
Notebooks
"""

import hashlib
import json
import logging
import os
import posixpath
import re
import zipfile
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import lru_cache
from io import StringIO
from typing import Any, Final, Set

import boto3
import cartopy.crs as ccrs  # For coastline plotting
import cartopy.feature as cfeature
import cftime
import fsspec
import geopandas as gpd
import gsw  # TEOS-10 library
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import s3fs
import seaborn as sns
import xarray as xr
from botocore import UNSIGNED
from botocore.client import Config
from dateutil.parser import parse
from fuzzywuzzy import fuzz
from IPython.display import FileLink
from matplotlib.colors import LogNorm, Normalize
from s3path import PureS3Path
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon
from windrose import WindroseAxes

__version__ = "0.2.5"

REGION: Final[str] = "ap-southeast-2"
ENDPOINT_URL = "https://s3.ap-southeast-2.amazonaws.com"
# ENDPOINT_URL = "http://127.0.0.1:9000"  # for local MinIo buckets
# BUCKET_OPTIMISED_DEFAULT = "imos-data-lab-optimised"
BUCKET_OPTIMISED_DEFAULT = "aodn-cloud-optimised"
ROOT_PREFIX_CLOUD_OPTIMISED_PATH = ""
DEFAULT_TIME = datetime(1900, 1, 1)


def is_colab():
    # google colab
    try:
        import google.colab

        return True
    except ImportError:
        return False


def _make_hashable(obj):
    """Recursively convert dicts/lists to tuples for hashing."""
    if isinstance(obj, dict):
        return tuple((k, _make_hashable(v)) for k, v in sorted(obj.items()))
    elif isinstance(obj, list):
        return tuple(_make_hashable(x) for x in obj)
    else:
        return obj


def _unhashable_to_dict(obj):
    """Recursively convert tuple back to dict."""
    if isinstance(obj, tuple):
        d = {}
        for k, v in obj:
            if isinstance(v, tuple):
                d[k] = _unhashable_to_dict(v)
            else:
                d[k] = v
        return d
    return obj


@lru_cache(maxsize=1)
def _get_s3_filesystem_cached(hashable_opts=None):
    """Internal cached function that expects hashable options."""
    if hashable_opts is not None:
        opts_dict = _unhashable_to_dict(hashable_opts)
        return fs.S3FileSystem(**s3_opts_to_pyarrow(opts_dict))
    else:
        return fs.S3FileSystem(
            region=REGION, endpoint_override=ENDPOINT_URL, anonymous=True
        )


def get_s3_filesystem(s3_fs_opts=None):
    """
    Return a cached pyarrow.fs.S3FileSystem instance.

    Args:
        s3_fs_opts (dict, optional): Dictionary of S3 options (s3fs-style).
            Can include keys like 'key', 'secret', 'anon', 'client_kwargs'.
    """
    hashable_opts = _make_hashable(s3_fs_opts) if s3_fs_opts else None
    return _get_s3_filesystem_cached(hashable_opts)


def s3_opts_to_pyarrow(s3_fs_opts: dict) -> dict:
    """
    Convert a generic S3 options dict (like for s3fs) into pyarrow.fs.S3FileSystem arguments.

    Automatically maps common keys and flattens nested 'client_kwargs' for PyArrow.

    Args:
        s3_fs_opts (dict): S3 options dictionary.

    Returns:
        dict: PyArrow-compatible S3FileSystem options.
    """
    # Define a mapping from generic keys to PyArrow S3FileSystem keys
    key_map = {
        "key": "access_key",
        "access_key": "access_key",
        "secret": "secret_key",
        "secret_key": "secret_key",
        "anon": "anonymous",
        "anonymous": "anonymous",
        # endpoint_override comes from client_kwargs.endpoint_url
    }

    pa_opts = {}

    for k, v in s3_fs_opts.items():
        if k in key_map:
            pa_opts[key_map[k]] = v
        elif k == "client_kwargs" and isinstance(v, dict):
            # Flatten client_kwargs
            if "endpoint_url" in v:
                pa_opts["endpoint_override"] = v["endpoint_url"]
            # Could extend here for region, profile, etc.
        else:
            # Pass other keys as-is (future-proofing)
            pa_opts[k] = v

    # Ensure anonymous is a boolean if provided
    if "anonymous" in pa_opts:
        pa_opts["anonymous"] = bool(pa_opts["anonymous"])
    else:
        pa_opts["anonymous"] = False

    return pa_opts


def query_unique_value(dataset: ds.Dataset, partition: str) -> Set[str]:
    """Queries unique values for a given Hive partition key in a PyArrow dataset.

    Iterates through the fragments (files) of a dataset and extracts the unique
    values associated with the specified partition key from the file paths.
    Assumes Hive partitioning (e.g., ".../partition=value/...").

    Args:
        dataset: The pyarrow.dataset.Dataset object.
        partition: The name of the partition key (e.g., "year", "timestamp").

    Returns:
        A set containing the unique string values found for the partition key.
    """
    unique_values = set()
    pattern = re.compile(f".*/{partition}=([^/]*)/")
    for fragment in dataset.get_fragments():
        match = pattern.match(fragment.path)
        if match:
            unique_values.add(match.group(1))
    return unique_values


def get_temporal_extent_v1(dataset: ds.Dataset) -> tuple[datetime, datetime]:
    """Calculates temporal extent based *only* on 'timestamp' partition values.

    This is a faster but potentially less accurate method than `get_temporal_extent`
    as it relies solely on the timestamp partition values, which might represent
    time bins rather than exact start/end times within the data.

    Args:
        dataset: The pyarrow.dataset.Dataset object.

    Returns:
        A tuple containing the minimum and maximum datetime objects derived
        from the 'timestamp' partition values, localized to UTC.
    """
    unique_timestamps = query_unique_value(dataset, "timestamp")
    unique_timestamps = np.array([np.int64(ts) for ts in unique_timestamps])
    unique_timestamps.sort()

    return (
        datetime.fromtimestamp(unique_timestamps.min(), tz=timezone.utc),
        datetime.fromtimestamp(unique_timestamps.max(), tz=timezone.utc),
    )


def get_temporal_extent(
    dataset: ds.Dataset,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Calculates the precise temporal extent by reading min/max time variable values.

    This method finds the minimum and maximum 'timestamp' partition values, then
    reads the actual time variable (e.g., 'TIME' or 'JULD') from the corresponding
    Parquet files to get the precise minimum and maximum timestamps in the dataset.
    This is more accurate but slower than `get_temporal_extent_v1`.

    Args:
        dataset: The pyarrow.dataset.Dataset object.

    Returns:
        A tuple containing the minimum and maximum pandas Timestamp objects
        found within the dataset's time variable.
    """
    unique_timestamps = query_unique_value(dataset, "timestamp")
    unique_timestamps = np.array([np.int64(string) for string in unique_timestamps])
    unique_timestamps = np.sort(unique_timestamps)

    # Detect the time variable name in the schema
    names = dataset.schema.names
    for candidate in ("TIME", "JULD", "detection_timestamp"):
        if candidate in names:
            time_varname = candidate
            break
    else:
        raise ValueError(
            "No known time variable ('TIME', 'JULD', 'detection_timestamp') found in dataset schema"
        )

    expr = pc.field("timestamp") == np.int64(unique_timestamps.max())
    table = dataset.to_table(filter=expr, columns=[time_varname])
    df = table.to_pandas()
    time_max = df[time_varname].max()

    expr = pc.field("timestamp") == np.int64(unique_timestamps.min())
    table = dataset.to_table(filter=expr, columns=[time_varname])
    df = table.to_pandas()
    time_min = df[time_varname].min()

    return time_min, time_max


def get_timestamps_boundary_values(
    dataset: ds.Dataset, date_start: str, date_end: str
) -> tuple[np.int64, np.int64]:
    """Finds partition timestamp boundaries bracketing a date range.

    Given a start and end date, this function identifies the 'timestamp'
    partition values in the PyArrow dataset that immediately precede the
    start and end dates. This is useful for filtering partitions efficiently.

    Args:
        dataset: The pyarrow.dataset.Dataset object.
        date_start: The start date string (e.g., "YYYY-MM-DD").
        date_end: The end date string (e.g., "YYYY-MM-DD").

    Returns:
        A tuple containing two int64 timestamps:
        - The partition timestamp value just before or equal to `date_start`.
        - The partition timestamp value just before or equal to `date_end`.
    """
    # Get unique partition values of 'timestamp'
    unique_timestamps = query_unique_value(dataset, "timestamp")
    unique_timestamps = np.array([np.int64(ts) for ts in unique_timestamps])
    unique_timestamps.sort()

    # Convert input date strings to Unix timestamps
    ts_start = int(pd.to_datetime(date_start).timestamp())
    ts_end = int(pd.to_datetime(date_end).timestamp())

    # Find closest partition timestamp ≤ date_start
    idx_start = np.searchsorted(unique_timestamps, ts_start)
    ts_start_part = (
        unique_timestamps[0] if idx_start == 0 else unique_timestamps[idx_start - 1]
    )

    # Find closest partition timestamp ≤ date_end
    idx_end = np.searchsorted(unique_timestamps, ts_end)
    ts_end_part = unique_timestamps[idx_end - 1]

    return ts_start_part, ts_end_part


def create_bbox_filter(dataset: ds.Dataset, **kwargs) -> pc.Expression:
    """Creates a PyArrow filter expression for spatial bounding box queries.

    Combines partition pruning based on the 'polygon' partition key (finding
    partitions intersecting the query box) with row-level filtering based on
    latitude and longitude variable values.

    Args:
        dataset: The pyarrow.dataset.Dataset() object.
        **kwargs: Keyword arguments defining the bounding box and variable names.
            lon_min (float): Minimum longitude.
            lon_max (float): Maximum longitude.
            lat_min (float): Minimum latitude.
            lat_max (float): Maximum latitude.
            lat_varname (str, optional): Name of the latitude variable.
                Defaults to "LATITUDE".
            lon_varname (str, optional): Name of the longitude variable.
                Defaults to "LONGITUDE".

    Returns:
        A PyArrow compute expression suitable for the `filters` argument in
        `pyarrow.parquet.read_table` or `pd.read_parquet`.

    Raises:
        ValueError: If any of lon_min, lon_max, lat_min, lat_max are None.
        ValueError: If no 'polygon' partitions intersect the bounding box.
    """
    lon_min = kwargs.get("lon_min")
    lon_max = kwargs.get("lon_max")
    lat_min = kwargs.get("lat_min")
    lat_max = kwargs.get("lat_max")

    lat_varname = kwargs.get("lat_varname", "LATITUDE")
    lon_varname = kwargs.get("lon_varname", "LONGITUDE")

    if None in (lon_min, lon_max, lat_min, lat_max):
        raise ValueError("Bounding box coordinates must be provided.")

    bounding_box = [
        (lon_min, lat_max),
        (lon_max, lat_max),
        (lon_max, lat_min),
        (lon_min, lat_min),
    ]
    bounding_box_polygon = Polygon(bounding_box)

    polygon_partitions = query_unique_value(dataset, "polygon")
    wkb_list = list(polygon_partitions)

    polygon_set = set(map(lambda x: wkb.loads(bytes.fromhex(x)), wkb_list))
    polygon_array_partitions = np.array(list(polygon_set))

    results = [
        polygon.intersects(bounding_box_polygon) for polygon in polygon_array_partitions
    ]

    # Filter polygon_array_partitions based on results
    intersecting_polygons = [
        polygon for polygon, result in zip(polygon_array_partitions, results) if result
    ]

    if intersecting_polygons == []:
        raise ValueError("No data for given bounding box. Amend lat/lon values ")

    # Convert intersecting polygons to WKB hexadecimal strings
    wkb_list = [polygon.wkb_hex for polygon in intersecting_polygons]

    expression = None
    for wkb_polygon in wkb_list:
        sub_expr = pc.field("polygon") == wkb_polygon
        if type(expression) != pc.Expression:
            expression = sub_expr
        else:
            expression = expression | sub_expr

    expr1 = pc.field(lat_varname) >= np.float64(lat_min)
    expr2 = pc.field(lat_varname) <= np.float64(lat_max)
    expr3 = pc.field(lon_varname) >= np.float64(lon_min)
    expr4 = pc.field(lon_varname) <= np.float64(lon_max)

    expression = expression & expr1 & expr2 & expr3 & expr4

    return expression


def ensure_utc_aware(dt):
    """Convert naive datetime to UTC-aware. Leave aware datetimes unchanged."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def create_time_filter(dataset: ds.Dataset, **kwargs) -> pc.Expression:
    """Creates a PyArrow filter expression for temporal range queries.

    Combines partition pruning based on the 'timestamp' partition key (using
    `get_timestamps_boundary_values`) with row-level filtering based on the
    actual time variable values (e.g., 'TIME' or 'JULD').

    Args:
        dataset: The pyarrow.dataset.dataset() object.
        **kwargs: Keyword arguments defining the time range and variable name.
            date_start (str): Start date string (e.g., "YYYY-MM-DD").
            date_end (str): End date string (e.g., "YYYY-MM-DD").
            time_varname (str, optional): Name of the time variable. Defaults
                to "TIME", but checks for "JULD" if "TIME" is not present.

    Returns:
        A PyArrow compute expression suitable for the `filters` argument in
        `pyarrow.parquet.read_table` or `pd.read_parquet`.

    Raises:
        ValueError: If `date_start` or `date_end` are None.
    """
    date_start = kwargs.get("date_start")
    date_end = kwargs.get("date_end")
    time_varname = kwargs.get("time_varname", "TIME")

    if None in (date_start, date_end):
        raise ValueError("Start and end dates must be provided.")

    # timestamp_start, timestamp_end = get_temporal_extent_v1(dataset)
    timestamp_start, timestamp_end = get_temporal_extent(dataset)
    timestamp_start = ensure_utc_aware(timestamp_start)
    timestamp_end = ensure_utc_aware(timestamp_end)

    # boundary check
    # Convert date_start string to a datetime object
    # date_start_dt = parse(date_start, default=DEFAULT_TIME).replace(tzinfo=timezone.utc)
    date_start_dt = ensure_utc_aware(parse(date_start, default=DEFAULT_TIME))

    # Compare
    is_date_start_after_timestamp_end = date_start_dt > timestamp_end
    if is_date_start_after_timestamp_end:
        timestamp_end_str = timestamp_end.strftime("%Y-%m-%d %H:%M:%S")

        raise ValueError(
            f"date_start={date_start} is out of range of dataset. The maximum date_end is {timestamp_end_str}."
        )
    # do the same for the other part of the time boundary check
    # Convert date_start string to a datetime object
    # date_end_dt = parse(date_end, default=DEFAULT_TIME).replace(tzinfo=timezone.utc)
    date_end_dt = ensure_utc_aware(parse(date_end, default=DEFAULT_TIME))

    # Compare
    is_date_end_before_timestamp_start = date_end_dt < timestamp_start
    if is_date_end_before_timestamp_start:
        timestamp_start_str = timestamp_start.strftime("%Y-%m-%d %H:%M:%S")

        raise ValueError(
            f"date_end={date_end} is out of range of dataset. The minimum date_start is {timestamp_start_str}."
        )

    timestamp_start, timestamp_end = get_timestamps_boundary_values(
        dataset, date_start, date_end
    )

    expr1 = pc.field("timestamp") >= np.int64(timestamp_start)
    expr2 = pc.field("timestamp") <= np.int64(timestamp_end)

    # ARGO Specific:
    if "TIME" in dataset.schema.names:
        time_varname = "TIME"
    elif "JULD" in dataset.schema.names:
        time_varname = "JULD"
    elif (
        "detection_timestamp" in dataset.schema.names
    ):  # animal_acoustic_tracking_delayed_qc specific
        time_varname = "detection_timestamp"

    expr3 = pc.field(time_varname) >= pd.to_datetime(date_start)
    expr4 = pc.field(time_varname) <= pd.to_datetime(date_end)

    expression = expr1 & expr2 & expr3 & expr4
    return expression


def get_spatial_extent(dataset: ds.Dataset) -> MultiPolygon:
    """Retrieves the spatial extent as a MultiPolygon from 'polygon' partitions.

    Reads the unique values from the 'polygon' partition key, decodes the WKB
    hex strings into Shapely Polygon objects, and combines them into a single
    MultiPolygon object representing the total spatial coverage defined by the
    partitions.

    Args:
        dataset: The pyarrow.dataset.Dataset() object.

    Returns:
        A Shapely MultiPolygon object.
    """
    # Retrieve unique polygon partitions
    polygon_partitions = query_unique_value(dataset, "polygon")

    # Convert WKB hex strings to Shapely geometries and create a set of unique polygons
    wkb_list = list(polygon_partitions)
    polygon_set = set(map(lambda x: wkb.loads(bytes.fromhex(x)), wkb_list))

    # Convert the set of polygons to a numpy array
    polygon_array_partitions = np.array(list(polygon_set))

    # Create a MultiPolygon from the array of polygons
    multi_polygon = MultiPolygon(polygon_array_partitions.tolist())

    return multi_polygon


def validate_date(date_str: str) -> bool:
    """Validates if a string is a valid date in 'YYYY-MM-DD' format.

    Args:
        date_str: The date string to validate.

    Returns:
        True if the string is a valid date in the specified format,
        False otherwise.
    """
    try:
        # datetime.strptime(date_str, "%Y-%m-%d")
        parse(date_str, default=DEFAULT_TIME)
        return True
    except ValueError:
        return False


def normalize_date(date_str: str) -> str:
    """Normalizes a date string, handling potential invalid dates like 'YYYY-MM-31'.

    Uses pandas `to_datetime` with `errors='coerce'` to parse the date string.
    If the date is invalid (e.g., February 30th), pandas might adjust it or
    return NaT. If valid, returns the date formatted as
    'YYYY-MM-DD HH:MM:SS'.

    Args:
        date_str: The input date string (ideally 'YYYY-MM-DD').

    Returns:
        The normalized date string in 'YYYY-MM-DD HH:MM:SS' format.

    Raises:
        ValueError: If the input string cannot be parsed into a date by pandas.
    """
    # Use pd.to_datetime to handle normalization and invalid dates
    date_obj = pd.to_datetime(date_str, errors="coerce")

    if pd.isna(date_obj):
        raise ValueError(f"Invalid date: {date_str}")

    return date_obj.strftime("%Y-%m-%d %H:%M:%S")


def create_timeseries(
    ds,
    var_name,
    lat,
    lon,
    start_time,
    end_time,
    lat_name="lat",
    lon_name="lon",
    time_name="time",
) -> pd.DataFrame:
    """Extracts and plots a time series for a variable at the nearest point.

    Selects data from an xarray Dataset for a given variable (`var_name`)
    at the spatial coordinates (`lat`, `lon`) nearest to the specified values,
    within a given time range (`start_time`, `end_time`). It then plots the
    resulting time series and returns the data as a pandas DataFrame.

    Args:
        ds: The input xarray Dataset.
        var_name: The name of the data variable to extract.
        lat: The target latitude.
        lon: The target longitude.
        start_time: The start time string (e.g., "YYYY-MM-DD").
        end_time: The end time string (e.g., "YYYY-MM-DD").
        lat_name: The name of the latitude coordinate/variable in `ds`.
            Defaults to "lat".
        lon_name: The name of the longitude coordinate/variable in `ds`.
            Defaults to "lon".
        time_name: The name of the time coordinate in `ds`. Defaults to "time".

    Returns:
        A pandas DataFrame containing the extracted time series data with
        columns for time, the variable, latitude, and longitude of the
        selected nearest point.

    Raises:
        ValueError: If the requested lat, lon, start_time, or end_time are
            outside the bounds of the dataset.
    """

    start_time = normalize_date(start_time)
    end_time = normalize_date(end_time)

    ds = ds.sortby(time_name)

    # Get latitude, longitude, and time extents
    lat_min, lat_max = ds[lat_name].min().item(), ds[lat_name].max().item()
    lon_min, lon_max = ds[lon_name].min().item(), ds[lon_name].max().item()
    time_min, time_max = (
        pd.to_datetime(ds[time_name].min().item()),
        pd.to_datetime(ds[time_name].max().item()),
    )

    # Test if latitude and longitude are within bounds
    if not (lat_min <= lat <= lat_max):
        raise ValueError(
            f"Latitude {lat} is out of bounds. Dataset latitude extent is ({lat_min}, {lat_max})"
        )
    if not (lon_min <= lon <= lon_max):
        raise ValueError(
            f"Longitude {lon} is out of bounds. Dataset longitude extent is ({lon_min}, {lon_max})"
        )

    # Test if start_time and end_time are within bounds
    if not (time_min <= pd.to_datetime(start_time) <= time_max):
        raise ValueError(
            f"Start time {start_time} is out of bounds. Dataset time extent is ({time_min.strftime('%Y-%m-%d %H:%M:%S')}, {time_max.strftime('%Y-%m-%d %H:%M:%S')})"
            # f"Start time {start_time} is out of bounds. Dataset time extent is ({time_min.date()}, {time_max.date()})"
        )
    if not (time_min <= pd.to_datetime(end_time) <= time_max):
        raise ValueError(
            f"End time {end_time} is out of bounds. Dataset time extent is ({time_min.strftime('%Y-%m-%d %H:%M:%S')}, {time_max.strftime('%Y-%m-%d %H:%M:%S')})"
            # f"End time {end_time} is out of bounds. Dataset time extent is ({time_min.date()}, {time_max.date()})"
        )

    # First, slice the dataset to the time range
    time_sliced_data = ds[var_name].sel({time_name: slice(start_time, end_time)})

    # Then, select the nearest latitude and longitude
    selected_data = time_sliced_data.sel(
        {lat_name: lat, lon_name: lon}, method="nearest"
    )

    # Convert the selected data to a pandas DataFrame
    time_series_df = selected_data.to_dataframe().reset_index()

    # Plot the selected data
    selected_data.plot()

    # Dynamic title based on the selected lat/lon and time range
    plt.title(
        f"{ds[var_name].attrs.get('long_name', var_name)} at {lat_name}={selected_data[lat_name].values}, "
        f"{lon_name}={selected_data[lon_name].values} from {start_time} to {end_time}"
    )

    plt.xlabel("Time")
    plt.ylabel(f"{ds[var_name].attrs.get('units', 'unitless')}")
    plt.show()

    # Return the plot and the pandas DataFrame
    return time_series_df


#
def plot_spatial_extent(
    dataset: ds.Dataset, coastline_resolution: str = "110m"
) -> None:
    """Plots the spatial extent derived from Parquet 'polygon' partitions.

    Retrieves the spatial extent using `get_spatial_extent`, creates a
    GeoDataFrame, and plots it on a map with coastlines, borders, land,
    and ocean features using Cartopy.

    Args:
        padataset: The pyarrow.dataset.Dataset() object.
        coastline_resolution: The resolution for the Cartopy coastline
            feature ('110m', '50m', '10m'). Defaults to "110m".
    """
    multi_polygon = get_spatial_extent(dataset)

    # Create a GeoDataFrame for the MultiPolygon
    gdf = gpd.GeoDataFrame(geometry=[multi_polygon], crs="EPSG:4326")  # Assuming WGS84

    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(
        1, 1, 1, projection=ccrs.PlateCarree()
    )  # Corrected subplot index

    # Add coastline
    ax.add_feature(
        cfeature.COASTLINE.with_scale(coastline_resolution), linewidth=1, color="blue"
    )

    gdf.plot(
        ax=ax,
        color="red",
        alpha=0.5,
        label="Spatial Extent",
        transform=ccrs.PlateCarree(),
    )

    ax.add_feature(cfeature.BORDERS, linestyle=":", edgecolor="gray")
    ax.add_feature(cfeature.LAND, edgecolor="black")
    ax.add_feature(cfeature.OCEAN)  # , facecolor="lightblue")

    # Add grid lines

    gl = ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
    gl.xlocator = mticker.FixedLocator(range(-180, 181, 10))  # Longitudes every 10°
    gl.ylocator = mticker.FixedLocator(range(-90, 91, 10))  # Latitudes every 10°
    gl.xlabel_style = {"size": 10, "color": "black"}
    gl.ylabel_style = {"size": 10, "color": "black"}

    ax.set_title("Spatial Extent and Coastline")
    # ax.legend()

    plt.show()


def plot_time_coverage(ds: xr.Dataset, time_var: str = "time") -> None:
    """Plots a heatmap showing the temporal data coverage (data points per month).

    Calculates the number of data points for each year/month combination based
    on the specified time coordinate in the xarray Dataset and displays it as
    a heatmap. Handles cftime objects if present in the time coordinate.

    Args:
        ds: The input xarray Dataset.
        time_var: The name of the time coordinate variable in `ds`.
            Defaults to "time".
    """
    # Convert the time dimension to a pandas DatetimeIndex
    ds = ds.sortby(time_var)

    time_values = ds[time_var].values

    cftime_types = (
        cftime.DatetimeGregorian,
        cftime.DatetimeProlepticGregorian,
        cftime.DatetimeJulian,
        cftime.DatetimeNoLeap,
        cftime.DatetimeAllLeap,
        cftime.Datetime360Day,
    )

    # Convert to pandas datetime; handle cftime objects if needed
    if isinstance(time_values[0], cftime_types):
        time_series = pd.to_datetime([t.isoformat() for t in time_values])
    else:
        time_series = pd.to_datetime(time_values)

    # Create a DataFrame with the year and month as separate columns
    time_df = pd.DataFrame({"year": time_series.year, "month": time_series.month})

    # Create a pivot table counting the occurrences of data points per year-month combination
    coverage = time_df.groupby(["year", "month"]).size().unstack(fill_value=0)

    # Only include the available months and years in the dataset
    plt.figure(figsize=(10, 6))
    heatmap = sns.heatmap(
        coverage.T, cmap="Greens", cbar=True, linewidths=0.5, square=True, vmin=0
    )

    # Customize plot
    plt.title("Time Coverage (per month)")
    plt.xlabel("Year")
    plt.ylabel("Month")

    # Set the color bar title
    colorbar = heatmap.collections[0].colorbar
    colorbar.set_label("N Data Points per Month")

    # Adjust y-axis ticks to only show months that are present in the dataset
    available_months = coverage.columns.get_level_values(0).unique()
    plt.yticks(
        ticks=range(len(available_months)),
        labels=[f"{i:02d}" for i in available_months],
        rotation=45,
    )
    plt.xticks(rotation=45)


def plot_radar_water_velocity_rose(
    ds: xr.Dataset, date_start: str, date_end: str, time_name_override: str = "TIME"
) -> None:
    """Plots a velocity rose (similar to wind rose) for radar data.

    Calculates the time-averaged U and V velocity components (UCUR, VCUR)
    over the specified time range, derives average speed and direction, and
    plots them using the `windrose` library.

    Args:
        ds: The input xarray Dataset, expected to contain 'UCUR', 'VCUR',
            'LONGITUDE', 'LATITUDE', and a time coordinate.
        date_start: The start time string for averaging (e.g., "YYYY-MM-DDTHH:MM:SS").
        date_end: The end time string for averaging (e.g., "YYYY-MM-DDTHH:MM:SS").
        time_name_override: The name of the time coordinate in `ds`. Defaults to "TIME".
    """
    # Select the data in the specified time range
    subset = ds.sel({time_name_override: slice(date_start, date_end)})

    # Calculate average speed and direction
    u_avg = subset.UCUR.mean(dim=time_name_override).values
    v_avg = subset.VCUR.mean(dim=time_name_override).values
    speed_avg = np.sqrt(u_avg**2 + v_avg**2)

    # Calculate direction in degrees (meteorological convention)
    direction = (np.arctan2(-u_avg, -v_avg) * 180 / np.pi) % 360

    # Flatten data for wind rose plotting
    speed_flat = speed_avg.flatten()
    direction_flat = direction.flatten()

    # Create the wind rose plot
    fig = plt.figure(figsize=(8, 8))
    ax = WindroseAxes.from_ax()
    ax.bar(
        direction_flat,
        speed_flat,
        bins=np.arange(0, np.nanmax(speed_flat) + 1, 1),
        cmap=plt.cm.viridis,
        normed=True,
    )

    # Add labels and legend
    ax.set_title(f"Velocity Rose: {date_start} to {date_end}")
    ax.set_legend(title="Speed (m/s)", loc="lower right", bbox_to_anchor=(1.2, 0))

    # Show the plot
    plt.show()


def plot_radar_water_velocity_gridded(
    ds: xr.Dataset,
    date_start: str,
    time_name_override: str = "TIME",
    coastline_resolution: str = "50m",
) -> None:
    """Plots gridded radar velocity data for 6 consecutive time steps.

    Starting from the time step nearest to `time_start`, this function creates
    a 3x2 grid of plots showing water speed (color) and velocity vectors
    (quiver arrows) for 6 consecutive time steps. Includes coastlines and
    gridlines.

    Args:
        ds: The input xarray Dataset, expected to contain 'UCUR', 'VCUR',
            'LONGITUDE', 'LATITUDE', and a time coordinate.
        time_start: The starting time string (e.g., "YYYY-MM-DDTHH:MM:SS").
            The plot will start from the time step nearest to this.
        time_name: The name of the time coordinate in `ds`. Defaults to "TIME".
        coastline_resolution: The resolution for the Cartopy coastline
            feature ('110m', '50m', '10m'). Defaults to "50m".
    """
    ds = ds.sortby(time_name_override)

    # Create a 3x2 grid of subplots with Cartopy projections
    fig, axes = plt.subplots(
        nrows=3,
        ncols=2,
        figsize=(15, 15),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )

    # Locate the time index for the starting time
    ii = 0
    iTime = list(ds[time_name_override].values).index(
        ds.sel({time_name_override: date_start}, method="nearest")[time_name_override]
    )

    # Create a colorbar axis
    cbar_ax = fig.add_axes(
        [0.92, 0.15, 0.02, 0.7]
    )  # Adjust the position and size of the colorbar

    for i in range(3):
        for j in range(2):
            # Extract data for plotting
            uData = ds.UCUR[iTime + ii, :, :]
            vData = ds.VCUR[iTime + ii, :, :]
            speed = np.sqrt(uData**2 + vData**2)
            lonData = ds.LONGITUDE.values
            latData = ds.LATITUDE.values

            # Plot speed as a filled contour
            p = axes[i, j].pcolor(
                lonData, latData, speed, transform=ccrs.PlateCarree(), cmap="viridis"
            )

            # Plot velocity vectors
            # axes[i, j].quiver(
            #     lonData, latData, uData, vData,
            #     transform=ccrs.PlateCarree(), scale=1, scale_units='xy', units='width'
            # )
            axes[i, j].quiver(
                lonData,
                latData,
                uData,
                vData,
                transform=ccrs.PlateCarree(),
                units="width",
            )

            # Add coastlines
            axes[i, j].coastlines(resolution=coastline_resolution)

            # Add gridlines and title
            axes[i, j].gridlines(
                draw_labels=True, linestyle="--", color="gray", alpha=0.5
            )
            axes[i, j].set_title(
                f"{np.datetime_as_string(ds[time_name_override].values[iTime + ii])}"
            )

            ii += 1

    # Add a common colorbar
    fig.colorbar(p, cax=cbar_ax, label="Speed (m/s)")

    # Adjust layout for better appearance
    plt.tight_layout(rect=[0, 0, 0.9, 1])  # Leave space for the colorbar

    # Show the plot
    plt.show()


# TODO: check if this can be deprecated
def plot_gridded_variable(
    ds,
    start_date,
    lon_slice=None,
    lat_slice=None,
    var_name="sea_surface_temperature",
    n_days=6,
    lat_name="lat",
    lon_name="lon",
    time_name="time",
    coastline_resolution="110m",
    log_scale=False,  # Optional argument to use a logarithmic color scale
):
    """
    Plots a variable (e.g., SST) data for 6 consecutive days starting from start_date with coastlines.

    Parameters:
    - ds: xarray.Dataset containing the data.
    - start_date: str, start date in 'YYYY-MM-DD' format.
    - lon_slice: tuple, longitude slice (start_lon, end_lon). (min val, max val)
    - lat_slice: tuple, latitude slice (start_lat, end_lat). (min val, max val)
    - var_name: str, variable name to plot (default is 'sea_surface_temperature').
    - coastline_resolution: str, resolution of the coastlines ('110m', '50m', '10m').
    - log_scale: bool, whether to use a logarithmic color scale (default is False).
    """
    logger = logging.getLogger("aodn.GetAodn")

    ds = ds.sortby(time_name)

    # Get latitude and longitude extents
    lat_min, lat_max = ds[lat_name].min().item(), ds[lat_name].max().item()
    lon_min, lon_max = ds[lon_name].min().item(), ds[lon_name].max().item()

    if lat_slice is None:
        lat_slice = (lat_min, lat_max)

    if lon_slice is None:
        lon_slice = (lon_min, lon_max)

    # Decide on the slice order
    if ds[lat_name][0] < ds[lat_name][-1]:
        lat_slice = lat_slice
    elif ds[lat_name][0] > ds[lat_name][-1]:
        # Reverse the slice
        lat_slice = lat_slice[::-1]

    # Test if lat_slice and lon_slice are within bounds
    if not (lat_min <= lat_slice[0] <= lat_max and lat_min <= lat_slice[1] <= lat_max):
        raise ValueError(
            f"Latitude slice {lat_slice} is out of bounds. Dataset latitude extent is ({lat_min}, {lat_max})"
        )

    if not (lon_min <= lon_slice[0] <= lon_max and lon_min <= lon_slice[1] <= lon_max):
        raise ValueError(
            f"Longitude slice {lon_slice} is out of bounds. Dataset longitude extent is ({lon_min}, {lon_max})"
        )

    # Parse the start date
    start_date_parsed = pd.to_datetime(start_date)

    # Ensure the dataset has a time dimension and it's sorted
    assert time_name in ds.dims, "Dataset does not have a 'time' dimension"
    ds = ds.sortby(time_name)

    # Find the nearest date in the dataset
    nearest_date = ds.sel({time_name: start_date_parsed}, method="nearest")[time_name]
    logger.info(f"Nearest date in dataset: {nearest_date}")

    # Get the index of the nearest date
    nearest_date_index = (
        ds[time_name].where(ds[time_name] == nearest_date, drop=True).squeeze().values
    )

    # Find the position of the nearest date in the time array
    nearest_date_position = int((ds[time_name] == nearest_date_index).argmax().values)

    # Get the next n_days date values including the nearest date
    dates = ds[time_name][nearest_date_position : nearest_date_position + n_days].values
    dates = [pd.Timestamp(date) for date in dates]

    # Retrieve variable-specific metadata
    var_long_name = ds[var_name].attrs.get("long_name", var_name)
    logger.info(f"Variable Long Name: {var_long_name}")

    # Create subplots with Cartopy for coastlines
    fig, axes = plt.subplots(
        nrows=int(n_days / 3),
        ncols=3,
        figsize=(18, 10),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    axes = axes.flatten()

    # Set up a variable for the colormap
    cmap = plt.get_cmap("coolwarm")

    # Create a placeholder for the color data range
    vmin, vmax = float("inf"), float("-inf")

    # First pass: gather all the data to find vmin and vmax
    for date in dates:
        try:
            data = ds[var_name].sel(
                {
                    time_name: date.strftime("%Y-%m-%d %H:%M:%S"),
                    lon_name: slice(lon_slice[0], lon_slice[1]),
                    lat_name: slice(lat_slice[0], lat_slice[1]),
                }
            )

            # Check for NaNs
            if data.isnull().all():
                logger.warning(
                    f"No valid data for {date.strftime('%Y-%m-%d %H:%M:%S')}, skipping this date."
                )
                continue

            # Retrieve and check units for the current plot
            var_units = ds[var_name].attrs.get("units", "unknown units")

            # Convert Kelvin to Celsius if needed
            if var_units.lower() == "kelvin":
                data = data - 273.15
                var_units = "°C"  # Change units in the label

            # Update vmin and vmax for colorbar scaling
            vmin = min(vmin, data.min().values)
            vmax = max(vmax, data.max().values)

            # Set the color scale norm based on whether log_scale is True or False
            if log_scale:
                norm = LogNorm(vmin=vmin, vmax=vmax)  # Logarithmic scale
                cbar_label = f"Log({var_long_name}) ({var_units})"  # Colorbar label for log scale
            else:
                norm = Normalize(vmin=vmin, vmax=vmax)  # Linear scale
                cbar_label = (
                    f"{var_long_name} ({var_units})"  # Colorbar label for linear scale
                )

        except Exception as err:
            logger.error(f"Error processing date {date.strftime('%Y-%m-%d')}: {err}")
            continue

    # Check if vmin or vmax are still invalid (if no data was found)
    if not np.isfinite(vmin) or not np.isfinite(vmax):
        raise ValueError(
            "No valid data found in the selected range of dates and coordinates."
        )

    # Plot each date after determining vmin and vmax
    for date in dates:
        try:
            data = ds[var_name].sel(
                {
                    time_name: date.strftime("%Y-%m-%d %H:%M:%S"),
                    lon_name: slice(lon_slice[0], lon_slice[1]),
                    lat_name: slice(lat_slice[0], lat_slice[1]),
                }
            )

            # Skip if no valid data is found for the date
            if data.isnull().all():
                logger.warning(
                    f"No data for {date.strftime('%Y-%m-%d %H:%M:%S')}, skipping plot."
                )
                continue

            var_units = ds[var_name].attrs.get("units", "unknown units")
            if var_units.lower() == "kelvin":
                data = data - 273.15
                var_units = "°C"

            # Plot the data
            img = data.plot(
                ax=axes[dates.index(date)],
                cmap=cmap,
                vmin=vmin,
                vmax=vmax,
                norm=norm,
                add_colorbar=False,
            )

            # Add coastlines and gridlines
            ax = axes[dates.index(date)]
            ax.coastlines(resolution=coastline_resolution)
            ax.add_feature(cfeature.BORDERS, linestyle=":")
            ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)

            # Set the title with the date
            ax.set_title(date.strftime("%Y-%m-%d %H:%M:%S"))

        except Exception as err:
            logger.error(
                f"Error processing date {date.strftime('%Y-%m-%d %H:%M:%S')}: {err}"
            )
            ax.set_title(f"No data for {date.strftime('%Y-%m-%d %H:%M:%S')}")
            ax.axis("off")

    # Create a single colorbar for all plots
    cbar_ax = fig.add_axes([1, 0.15, 0.02, 0.7])
    cbar = fig.colorbar(img, cax=cbar_ax, orientation="vertical")
    # Set the colorbar label and title based on the scale type
    if log_scale:
        cbar.set_label(f"Log({var_long_name}) ({var_units})")
    else:
        cbar.set_label(f"{var_long_name} ({var_units})")

    cbar.set_ticks([vmin, (vmin + vmax) / 2, vmax])
    cbar.ax.set_yticklabels([f"{vmin:.2f}", f"{(vmin + vmax) / 2:.2f}", f"{vmax:.2f}"])

    fig.suptitle(f"{var_long_name} Over Time", fontsize=16, fontweight="bold")
    # Adjust layout to give more space for the colorbar
    plt.subplots_adjust(right=0.85, top=0.9)
    plt.tight_layout()
    plt.show()


def plot_ts_diagram(
    df: pd.DataFrame,
    temp_col: str = "TEMP",
    psal_col: str = "PSAL",
    z_col: str = "DEPTH",
) -> None:
    """Plots a Temperature-Salinity (T-S) diagram with density contours.

    Generates a scatter plot of temperature vs. salinity from the input
    DataFrame, color-coded by depth. Overlays density anomaly (sigma0)
    contours calculated using the `gsw` library. Filters data points with
    salinity below 25 before plotting.

    Args:
        df: DataFrame containing temperature, salinity, and depth data.
        temp_col: Name of the temperature column. Defaults to "TEMP".
        psal_col: Name of the salinity column. Defaults to "PSAL".
        z_col: Name of the depth column used for color-coding. Defaults
            to "DEPTH".
    """
    # Filter data where PSAL >= 25
    filtered_df = df[(df[psal_col] >= 25)]

    fig, ax = plt.subplots(figsize=(10, 6))

    # Define the colormap for depth
    depths = filtered_df[z_col]
    cmap = plt.get_cmap(
        "viridis_r"
    )  # Reverse the colormap to make deeper depths darker
    norm = plt.Normalize(
        vmin=depths.min(), vmax=depths.max()
    )  # Normalize the depth values for color mapping

    # Plot the T-S diagram, color-coded by DEPTH
    sc = ax.scatter(
        filtered_df[psal_col],
        filtered_df[temp_col],
        c=depths,
        cmap=cmap,
        norm=norm,
        s=10,
        label="Data",
    )

    # Generate temperature and salinity grids for contour plot
    temp_range = np.linspace(
        filtered_df[temp_col].min(), filtered_df[temp_col].max(), 100
    )
    psal_range = np.linspace(
        filtered_df[psal_col].min(), filtered_df[psal_col].max(), 100
    )
    TEMP_grid, PSAL_grid = np.meshgrid(temp_range, psal_range)

    # Compute density anomaly (sigma0) using gsw (pressure=0 for surface)
    density = gsw.sigma0(
        PSAL_grid, TEMP_grid
    )  # Sigma0 = density anomaly (kg/m^3 - 1000)

    # Plot density contours
    density_levels = np.arange(
        density.min(), density.max(), 1
    )  # Customize levels if needed
    contour = ax.contour(
        PSAL_grid,
        TEMP_grid,
        density,
        levels=density_levels,
        colors="k",
        linestyles="--",
    )
    ax.clabel(contour, fmt="%1.1f", fontsize=10)  # Add contour labels

    # Create a colorbar for depth
    cbar = plt.colorbar(sc, ax=ax, label=f"{z_col}", orientation="vertical")

    ax.set_xlabel("Salinity (PSAL)")
    ax.set_ylabel("Temperature (TEMP)")
    ax.set_title("T-S Diagram with Density Contours")

    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.show()


def get_schema_metadata(dname: str, s3_fs_opts=None) -> dict:
    """Retrieves and decodes metadata from a Parquet dataset's common metadata.

    Reads the schema from the `_common_metadata` file within the specified
    Parquet dataset directory (`dname`) on S3. It then decodes the schema's
    metadata (which is stored as bytes key-value pairs where values are often
    JSON strings) into a standard Python dictionary.

    Args:
        dname: The S3 path to the root of the Parquet dataset directory
            (e.g., "s3://bucket/path/dataset.parquet/").

    Returns:
        A dictionary containing the decoded metadata. Keys and string values
        within the original JSON are decoded from UTF-8. JSON structures are
        parsed into Python objects. Returns an empty dict if metadata is empty
        or decoding fails.
    """
    name = dname.replace("s3://", "")
    name = name.replace("anonymous@", "")
    logger = logging.getLogger("aodn.GetAodn")
    logger.info(f"Retrieving metadata for {name}")

    s3 = get_s3_filesystem(s3_fs_opts=s3_fs_opts)
    meta_name = posixpath.join(name, "_common_metadata")
    dataset = ds.dataset(meta_name, format="parquet", filesystem=s3)
    parquet_meta = dataset.schema
    # parquet_meta = pa.parquet.read_schema(
    #     os.path.join(name, "_common_metadata"),
    #     # Pyarrow can infer file system from path prefix with s3 but it will try
    #     # to scan local file system before infer and get a pyarrow s3 file system
    #     # which is very slow to start, read_schema no s3 prefix needed
    #     filesystem=s3,
    # )

    # horrible ... but got to be done. The dictionary of metadata has to be a dictionnary with byte keys and byte values.
    # meaning that we can't have nested dictionaries ...

    # the code below works, but in some case, when the metadata file is poorly written (with a wrong double quote escape
    # for example, none of the metadata file is converted and it's hard to spot the issue. We replace this with the
    # decode_and_load_json function
    # decoded_meta = {
    #     key.decode("utf-8"): json.loads(value.decode("utf-8").replace("'", '"'))
    #     for key, value in parquet_meta.metadata.items()
    # }

    decoded_meta = decode_and_load_json(parquet_meta.metadata)

    # handling old configuration of parquet creation. Should be deprecated at some stage but not impacting much
    if "dataset_metadata" in decoded_meta:
        logger.warning(
            "Old 'dataset_metadata' deprecated key found in parquet schema. Renamed to 'global_attributes'. Dataset should be updated"
        )
        decoded_meta["global_attributes"] = decoded_meta.pop("dataset_metadata")

    return decoded_meta


def decode_and_load_json(metadata: dict[bytes, bytes]) -> dict[str, any]:
    """Decodes keys and JSON-encoded values from Parquet metadata.

    Iterates through a dictionary where keys and values are bytes (as obtained
    from PyArrow schema metadata). It decodes keys to UTF-8 strings and attempts
    to decode values as UTF-8 strings, replace single quotes with double quotes
    (common issue), and then parse them as JSON. Logs errors for values that
    fail decoding or JSON parsing.

    Args:
        metadata: A dictionary with bytes keys and bytes values, typically
            from `pyarrow.Schema.metadata`.

    Returns:
        A dictionary with decoded string keys and parsed Python objects as values.
        Keys or values that failed decoding/parsing are omitted.
    """
    logger = logging.getLogger("aodn.GetAodn")
    decoded_metadata = {}
    for key, value in metadata.items():
        try:
            # Decode bytes to string
            value_str = value.decode("utf-8")
            value_str = value_str.replace("'", '"')

            decoded_metadata[key.decode("utf-8")] = json.loads(value_str)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON for key {key}: {e}")
            logger.error(f"Problematic JSON string: {value_str}")
        except Exception as e:
            logger.error(f"Unexpected error for key {key}: {e}")
    return decoded_metadata


def get_zarr_metadata(dname: str, s3_fs_opts=None) -> dict:
    """Retrieves metadata from a Zarr store using xarray.

    Opens the Zarr store located at the S3 path `dname` using xarray and
    fsspec for anonymous access. Extracts global attributes and attributes
    for all variables (including coordinates) into a dictionary.

    Args:
        dname: The S3 path to the Zarr store (e.g., "s3://bucket/path/dataset.zarr/").

    Returns:
        A dictionary containing the metadata. It has a top-level key
        "global_attributes" and keys for each variable/coordinate name,
        with values being dictionaries of their respective attributes.
        Returns a basic dict with an "error" key if opening fails.
    """
    logger = logging.getLogger("aodn.GetAodn")
    name = dname.replace("anonymous@", "")
    logger.info(f"Retrieving metadata for {name}")

    if s3_fs_opts:
        # Use provided S3 filesystem
        s3 = s3fs.S3FileSystem(**s3_fs_opts)
        # mapper = fsspec.get_mapper(name, storage_options={"fs": s3})
        mapper = s3.get_mapper(name)
    else:
        # Default: anonymous access
        mapper = fsspec.get_mapper(name, anon=True)

    try:
        # Use fsspec mapper for xarray to access S3 anonymously
        with xr.open_zarr(mapper, chunks=None, consolidated=True) as ds:
            metadata = {"global_attributes": ds.attrs.copy()}
            for var_name, variable in ds.variables.items():
                metadata[var_name] = variable.attrs.copy()
        return metadata
    except Exception as e:
        logger.error(f"Error opening or processing Zarr metadata from {dname}: {e}")
        return {"global_attributes": {}, "error": str(e)}


####################################################################################################################
# Work done during IMOS HACKATHON 2024
# https://github.com/aodn/IMOS-hackathon/blob/main/2024/Projects/CARSv2/notebooks/get_aodn_example_hackathon.ipynb
###################################################################################################################


class TimeSeriesResult:
    """Wrapper for time series DataFrame that includes built-in plotting."""

    def __init__(
        self,
        df: pd.DataFrame,
        var_name: str,
        lat: float,
        lon: float,
        date_start: str,
        date_end: str,
        actual_lat_name: str = "lat",
        actual_lon_name: str = "lon",
        actual_time_name: str = "time",
    ):
        self.df = df
        self.var_name = var_name
        self.lat = lat
        self.lon = lon
        self.date_start = date_start
        self.date_end = date_end
        self.lat_name = actual_lat_name
        self.lon_name = actual_lon_name
        self.time_name = actual_time_name

    def plot_timeseries(self) -> pd.DataFrame:
        """Plot and return the time series DataFrame."""
        self.df.plot(x=self.time_name, y=self.var_name)
        plt.title(
            f"{self.var_name} at ({self.lat}, {self.lon}) from {self.date_start} to {self.date_end}"
        )
        plt.xlabel("Time")
        plt.ylabel(self.var_name)
        plt.grid(True)
        plt.tight_layout()
        plt.show()
        return self.df  # Optional: return to maintain compatibility


class DataSource(ABC):
    """Abstract Base Class for accessing different data source formats (Parquet, Zarr)."""

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        dataset_name: str,
        logger: logging.Logger | None = None,
        s3_fs_opts: dict | None = None,
    ):
        """Initialises the DataSource.

        Args:
            bucket_name: The S3 bucket name.
            prefix: The S3 prefix (folder path) within the bucket.
            dataset_name: The name of the dataset (including extension,
                e.g., "my_data.parquet" or "my_data.zarr").
            logger: Optional logger to use. If not provided, a default logger will be created.
        """
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.dataset_name = dataset_name
        self.dname = self._build_data_path()
        self.s3_fs_opts = s3_fs_opts

        self.logger = logger or logging.getLogger("aodn.GetAodn")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.propagate = False

        if ".parquet" in self.dname:
            self.s3 = get_s3_filesystem(s3_fs_opts=self.s3_fs_opts)
            self.dataset = self._create_pyarrow_dataset()

    @abstractmethod
    def _build_data_path(self) -> str:
        """Constructs the full S3 path to the data source."""
        pass

    @abstractmethod
    def get_data(
        self,
        date_start: str | None = None,
        date_end: str | None = None,
        lat_min: float | None = None,
        lat_max: float | None = None,
        lon_min: float | None = None,
        lon_max: float | None = None,
        scalar_filter: dict | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame | xr.Dataset:
        """Retrieves data, potentially filtered by arguments."""
        pass

    @abstractmethod
    def get_spatial_extent(self) -> list | MultiPolygon:
        """Returns the spatial extent of the dataset."""
        pass

    @abstractmethod
    def plot_spatial_extent(self) -> None:
        """Plots the spatial extent of the dataset."""
        pass

    @abstractmethod
    def get_temporal_extent(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Returns the temporal extent (min_time, max_time) of the dataset."""
        pass

    @abstractmethod
    def get_temporal_extent_from_timestamp_partition(
        self,
    ) -> tuple[datetime, datetime]:
        """Returns the temporal extent (min_time, max_time) of the dataset."""
        pass

    def get_unique_partition_values(self, partition_name: str) -> Set[str]:
        pass

    @abstractmethod
    def get_metadata(self) -> dict:
        """Retrieves metadata associated with the dataset."""
        pass

    @abstractmethod
    def get_timeseries_data(
        self,
        lat: float,
        lon: float,
        date_start: str,
        date_end: str,
        var_name: str | None = None,  # Made var_name optional
        lat_name_override: str | None = None,
        lon_name_override: str | None = None,
        time_name_override: str | None = None,
    ) -> TimeSeriesResult:
        """Extracts time series data for a variable (or all variables) at the nearest point."""
        pass

    @abstractmethod
    def plot_timeseries(
        self,
        timeseries_df: pd.DataFrame,
        var_name: str,  # Specific variable from the DataFrame to plot
        lat: float,
        lon: float,
        date_start: str,
        date_end: str,
        actual_lat_name: str,
        actual_lon_name: str,
        actual_time_name: str,
    ) -> None:
        """Plotsthe extracted time series data."""
        pass

    @abstractmethod
    def plot_radar_water_velocity_gridded(self, **kwargs) -> None:
        pass

    @abstractmethod
    def plot_radar_water_velocity_rose(self, **kwargs) -> None:
        pass


class ParquetDataSource(DataSource):
    """DataSource implementation for Parquet datasets."""

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        dataset_name: str,
        s3_fs_opts: dict | None = None,
    ):
        """Initialises the ParquetDataSource with optional S3 filesystem overrides."""
        self.s3_fs_opts = s3_fs_opts or {}
        if self.s3_fs_opts:
            self.s3 = fs.S3FileSystem(**s3_opts_to_pyarrow(self.s3_fs_opts))
        else:
            self.s3 = fs.S3FileSystem(
                region=self.s3_fs_opts.get("region", REGION),
                endpoint_override=ENDPOINT_URL,
                anonymous=True,
            )

        super().__init__(bucket_name, prefix, dataset_name, s3_fs_opts=s3_fs_opts)
        if ".parquet" in self.dname:
            # override S3 filesystem and re-create dataset with custom options
            self.dataset = self._create_pyarrow_dataset()

    def _build_data_path(self) -> str:
        """Constructs the S3 path for the Parquet dataset directory.

        Returns:
            The S3 path string (e.g., "bucket/prefix/my_dataset.parquet/").
            Removes any "s3://anonymous@" prefix added by PureS3Path.
        """
        # self.dataset_name now includes the extension, e.g., "my_dataset.parquet"
        # We need to form the path like ".../my_dataset.parquet/"
        dname_uri = (
            PureS3Path.from_uri(f"s3://anonymous@{self.bucket_name}/{self.prefix}/")
            .joinpath(f"{self.dataset_name}/")  # Add trailing slash
            .as_uri()
        )
        return dname_uri.replace("s3://anonymous%40", "")

    def get_unique_partition_values(self, partition_name) -> Set[str]:
        return query_unique_value(self.dataset, partition_name)

    def _create_pyarrow_dataset(self, filters=None) -> ds.Dataset:
        """Creates a PyArrow Dataset object for the data source using the modern API.

        Args:
            filters: Optional PyArrow filter expression to apply when creating
                the dataset object (for fragment-level filtering). Defaults to None.

        Returns:
            A pyarrow.dataset.Dataset instance.
        """
        return ds.dataset(
            self.dname,
            format="parquet",
            partitioning="hive",
            filesystem=self.s3,
        )

    def partition_keys_list(self) -> pa.Schema:
        """Returns the schema of the Hive partitioning keys.

        Returns:
            A pyarrow.Schema object representing the partition keys.
        """
        dataset = self._create_pyarrow_dataset()
        partition_keys = dataset.partitioning.schema
        return partition_keys

    def get_spatial_extent(self) -> MultiPolygon:
        """Returns the spatial extent derived from 'polygon' partitions.

        Uses the global `get_spatial_extent` function.

        Returns:
            A Shapely MultiPolygon object.
        """
        return get_spatial_extent(self.dataset)

    def plot_spatial_extent(self) -> None:
        """Plots the spatial extent derived from 'polygon' partitions.

        Uses the global `plot_spatial_extent` function.
        """
        return plot_spatial_extent(self.dataset)

    def get_temporal_extent(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Returns the precise temporal extent by reading min/max time values.

        Uses the global `get_temporal_extent` function.

        Returns:
            A tuple containing the minimum and maximum pandas Timestamp objects.
        """
        return get_temporal_extent(self.dataset)

    def get_temporal_extent_from_timestamp_partition(
        self,
    ) -> tuple[datetime, datetime]:
        """Returns the temporal extent by reading min/max timestamp partition values.

        Uses the global `get_temporal_extent_v1` function.

        Returns:
            A tuple containing the minimum and maximum pandas Timestamp objects.
        """
        return get_temporal_extent_v1(self.dataset)

    def get_data(
        self,
        date_start: str | None = None,
        date_end: str | None = None,
        lat_min: float | None = None,
        lat_max: float | None = None,
        lon_min: float | None = None,
        lon_max: float | None = None,
        scalar_filter: dict | None = None,
        columns: list[str] | None = None,
        lat_varname: str | None = None,
        lon_varname: str | None = None,
        time_varname: str | None = None,
    ) -> pd.DataFrame:
        """Retrieves data from the Parquet dataset, applying filters.

        Constructs PyArrow filter expressions based on the provided time range,
        bounding box, and scalar filter conditions. Reads the filtered data

        Args:
            date_start: Start date string (e.g., "YYYY-MM-DD").
            date_end: End date string (e.g., "YYYY-MM-DD"). Defaults to now if
                `date_start` is provided but `date_end` is None.
            lat_min: Minimum latitude for bounding box filter.
            lat_max: Maximum latitude for bounding box filter.
            lon_min: Minimum longitude for bounding box filter.
            lon_max: Maximum longitude for bounding box filter.
            scalar_filter: Dictionary for simple equality filters, e.g.,
                `{'platform_code': 'SLABC'}`. Filters are combined with AND.
            columns: List of column names to read. Reads all columns if None.

        Returns:
            A pandas DataFrame containing the requested data.
        """
        # TODO fix the whole logic as not everything is considered

        # time filter: doesnt require date_end
        if date_end is None:
            now = datetime.now()
            date_end = now.strftime("%Y-%m-%d %H:%M:%S")

        if date_start is None:
            filter_time = None
        else:
            time_kwargs = {}
            if time_varname is not None:
                time_kwargs["time_varname"] = time_varname
            filter_time = create_time_filter(
                self.dataset, date_start=date_start, date_end=date_end, **time_kwargs
            )

        # Geometry filter requires ALL optional args to be defined
        if lat_min is None or lat_max is None or lon_min is None or lon_max is None:
            filter_geo = None
        else:
            bbox_kwargs = {}

            if lat_varname is not None:
                bbox_kwargs["lat_varname"] = lat_varname

            if lon_varname is not None:
                bbox_kwargs["lon_varname"] = lon_varname

            filter_geo = create_bbox_filter(
                self.dataset,
                lat_min=lat_min,
                lat_max=lat_max,
                lon_min=lon_min,
                lon_max=lon_max,
                **bbox_kwargs,
            )

        # scalar filter
        if scalar_filter is not None:
            expr = None
            for item in scalar_filter:
                expr_1 = pc.field(item) == pa.scalar(scalar_filter[item])
                if not isinstance(expr, pc.Expression):
                    expr = expr_1
                else:
                    expr = expr_1 & expr

        # use isinstance as it support type check for subclasss relationship
        # we want to merge type together, if both are expression then use and to join together
        if isinstance(filter_geo, pc.Expression) & isinstance(
            filter_time, pc.Expression
        ):
            data_filter = filter_geo & filter_time
        elif isinstance(filter_time, pc.Expression):
            data_filter = filter_time
        elif isinstance(filter_geo, pc.Expression):
            data_filter = filter_geo
        else:
            data_filter = None

        # add scalar filter to data_filter
        if scalar_filter is not None:
            if data_filter is None:
                data_filter = expr
            else:
                data_filter = data_filter & expr

        # Set file system explicitly do not require folder prefix s3://
        table = self.dataset.to_table(filter=data_filter, columns=columns)
        df = table.to_pandas()

        def append_metadata_to_df(metadata, df):
            global_attrs = metadata.get("global_attributes", {})
            variable_attrs = {
                k: v for k, v in metadata.items() if k != "global_attributes"
            }

            # Attach variable metadata to each column's attrs, if column exists in df
            # for var, attrs in variable_attrs.items():
            # if var in df.columns:
            for var in df.columns:
                if not hasattr(df[var], "attrs"):
                    df[var].attrs = {}
                if var in variable_attrs:
                    df[var].attrs.update(variable_attrs[var])

            # Attach global metadata to the DataFrame attrs
            # has to be done here, otherwise, gattrs will be added to all variables for some obscure reason
            df.attrs.update(global_attrs)

            return df

        df = append_metadata_to_df(self.get_metadata(), df)

        return df

    def get_metadata(self) -> dict:
        """Retrieves metadata from the Parquet dataset's common metadata.

        Uses the global `get_schema_metadata` function.

        Returns:
            A dictionary containing the decoded metadata.
        """
        return get_schema_metadata(self.dname, s3_fs_opts=self.s3_fs_opts)

    def get_timeseries_data(
        self,
        lat: float,
        lon: float,
        date_start: str,
        date_end: str,
        var_name: str | None = None,
        lat_name_override: str | None = None,
        lon_name_override: str | None = None,
        time_name_override: str | None = None,
    ) -> TimeSeriesResult:
        """Extracts time series data for a variable (or all variables) at the nearest point. (Not Implemented for Parquet)"""
        raise NotImplementedError(
            "get_timeseries_data is not implemented for ParquetDataSource."
        )

    def plot_timeseries(
        self,
        timeseries_df: pd.DataFrame,
        var_name: str,
        lat: float,
        lon: float,
        date_start: str,
        date_end: str,
        actual_lat_name: str,
        actual_lon_name: str,
        actual_time_name: str,
    ) -> None:
        """Plots the extracted time series data. (Not Implemented for Parquet)"""
        raise NotImplementedError(
            "plot_timeseries is not implemented for ParquetDataSource."
        )

    def plot_radar_water_velocity_gridded(self, **kwargs) -> None:
        raise NotImplementedError(
            "plot_radar_water_velocity_gridded is not implemented for ParquetDataSource."
        )

    def plot_radar_water_velocity_rose(self, **kwargs) -> None:
        raise NotImplementedError(
            "plot_radar_water_velocity_gridded is not implemented for ParquetDataSource."
        )


class ZarrDataSource(DataSource):
    """DataSource implementation for Zarr datasets."""

    def _build_data_path(self) -> str:
        """Constructs the S3 path for the Zarr store directory.

        Returns:
            The S3 path string (e.g., "s3://bucket/prefix/my_dataset.zarr/").
            Ensures the path starts with "s3://" and removes any "anonymous@".
        """
        # self.dataset_name now includes the extension, e.g., "my_dataset.zarr"
        # We need to form the path like ".../my_dataset.zarr/"
        dname_uri = (
            PureS3Path.from_uri(f"s3://{self.bucket_name}/{self.prefix}/")
            .joinpath(f"{self.dataset_name}/")  # Add trailing slash
            .as_uri()
        )
        # Ensure it starts with s3:// and no anonymous@ (PureS3Path should handle s3://)
        if "anonymous@" in dname_uri:
            dname_uri = dname_uri.replace("anonymous@", "")
        if not dname_uri.startswith("s3://"):
            dname_uri = f"s3://{dname_uri}"
        return dname_uri

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        dataset_name: str,
        s3_fs_opts: dict | None = None,
    ):
        """Initialises the ZarrDataSource with optional S3 filesystem overrides."""
        self.s3_fs_opts = s3_fs_opts or {}
        if self.s3_fs_opts:
            self.s3 = s3fs.S3FileSystem(**self.s3_fs_opts)
        else:
            # Default anonymous S3 with optional region and endpoint
            self.s3 = s3fs.S3FileSystem(
                anon=True,
                key=None,
                secret=None,
                client_kwargs={"endpoint_url": ENDPOINT_URL, "region_name": REGION},
            )

        super().__init__(bucket_name, prefix, dataset_name)
        self.zarr_store = self._open_zarr_store()

    def _open_zarr_store(self) -> xr.Dataset:
        """Opens the Zarr store using xarray and fsspec for anonymous access.

        Returns:
            An xarray Dataset object representing the Zarr store.

        Raises:
            Exception: If opening the Zarr store fails.
            ValueError: If a suitable time variable cannot be found for sorting.
        """
        try:
            mapper = self.s3.get_mapper(self.dname)
            ds = xr.open_zarr(mapper, chunks=None, consolidated=True)

            # Find the time variable name to sort by
            time_names = [
                "time",
                "TIME",
                "datetime",
                "date",
                "Date",
                "DateTime",
                "JULD",
            ]
            # We need a logger instance here if _find_var_name uses it.
            # Since _find_var_name is part of ZarrDataSource, it can call self.get_logger()
            # However, _open_zarr_store is called in __init__ before logger might be fully set up by CommonHandler if not careful.
            # For now, assuming _find_var_name_global can access a logger or doesn't strictly need it for this path.
            # A safer approach might be to pass a logger or make _find_var_name_global static if it doesn't need self.
            # Let's assume self.get_logger() is available or _find_var_name_global handles its absence.
            try:
                time_var_name = _find_var_name_global(ds, time_names, "time")
                return ds.sortby(time_var_name)
            except ValueError as ve:
                # Log this, but still return the unsorted dataset if time var not found for sorting.
                # Or re-raise if sorting is critical. For now, let's log and return.
                # Consider if self.get_logger() is available here.
                # If not, use a simple print or a default logger.
                self.logger.warning(
                    f"Could not find time variable to sort Zarr store {self.dname}: {ve}. Returning unsorted."
                )
                return ds  # Return unsorted if time var not found, or raise
        except Exception as e:
            self.logger.error(f"Opening Zarr store {self.dname}: {e}")
            raise

    def _find_lat_lon_vars(self, ds: xr.Dataset) -> tuple[xr.DataArray, xr.DataArray]:
        """Finds latitude and longitude variables/coordinates in a dataset.

        Searches for common names (e.g., 'latitude', 'lat', 'LATITUDE', 'LAT')
        first in the dataset coordinates and then in the data variables.

        Args:
            ds: The xarray Dataset to search within.

        Returns:
            A tuple containing the xarray DataArrays for latitude and longitude.

        Raises:
            ValueError: If a latitude or longitude variable/coordinate cannot
                be found using the common names.
        """
        lat_names = ["latitude", "lat", "LATITUDE", "LAT"]
        lon_names = ["longitude", "lon", "LONGITUDE", "LON"]

        lat_var = None
        lon_var = None

        # Check coordinates first
        for name in lat_names:
            if name in ds.coords:
                lat_var = ds.coords[name]
                break
        for name in lon_names:
            if name in ds.coords:
                lon_var = ds.coords[name]
                break

        # If not in coords, check data variables
        if lat_var is None:
            for name in lat_names:
                if name in ds.data_vars:
                    lat_var = ds.data_vars[name]
                    break
        if lon_var is None:
            for name in lon_names:
                if name in ds.data_vars:
                    lon_var = ds.data_vars[name]
                    break

        if lat_var is None:
            raise ValueError(
                f"Latitude variable/coordinate not found in dataset {self.dataset_name}. Searched for {lat_names}."
            )
        if lon_var is None:
            raise ValueError(
                f"Longitude variable/coordinate not found in dataset {self.dataset_name}. Searched for {lon_names}."
            )

        return lat_var, lon_var

    def get_data(
        self,
        date_start: str | None = None,
        date_end: str | None = None,
        lat_min: float | None = None,
        lat_max: float | None = None,
        lon_min: float | None = None,
        lon_max: float | None = None,
        scalar_filter: dict[str, Any] | None = None,
    ) -> xr.Dataset:
        """Retrieves data from the Zarr store, applying scalar and spatio-temporal filters.

        Scalar filters are applied before coordinate slicing to optimise remote access.

        Args:
            date_start: Optional start date string (e.g., "YYYY-MM-DD").
            date_end: Optional end date string.
            lat_min: Optional minimum latitude.
            lat_max: Optional maximum latitude.
            lon_min: Optional minimum longitude.
            lon_max: Optional maximum longitude.
            scalar_filter: Optional dict of scalar equality filters (e.g., {"vessel_name": "VNCF"}).

        Returns:
            An xarray.Dataset containing the selected data.

        Raises:
            ValueError: If a scalar filter variable is not found.
        """

        if self.zarr_store is None:
            self._open_zarr_store()

        ds = self.zarr_store
        selectors = {}
        mask_conditions = []

        # Time slicing
        if date_start is not None or date_end is not None:
            time_names = [
                "time",
                "TIME",
                "datetime",
                "date",
                "Date",
                "DateTime",
                "JULD",
            ]
            time_var_name = _find_var_name_global(ds, time_names, "time")
            selectors[time_var_name] = slice(date_start, date_end)

        # Latitude slicing
        if lat_min is not None or lat_max is not None:
            lat_names = ["latitude", "lat", "LATITUDE", "LAT"]
            lat_var_name = _find_var_name_global(ds, lat_names, "latitude")
            lat_is_dim = lat_var_name in ds.dims

            if lat_is_dim:
                selectors[lat_var_name] = slice(lat_min, lat_max)
            else:
                if lat_min is not None:
                    mask_conditions.append(ds[lat_var_name] >= lat_min)
                if lat_max is not None:
                    mask_conditions.append(ds[lat_var_name] <= lat_max)

        # Longitude slicing
        if lon_min is not None or lon_max is not None:
            lon_names = ["longitude", "lon", "LONGITUDE", "LON"]
            lon_var_name = _find_var_name_global(ds, lon_names, "longitude")
            lon_is_dim = lon_var_name in ds.dims

            if lon_is_dim:
                selectors[lon_var_name] = slice(lon_min, lon_max)
            else:
                if lon_min is not None:
                    mask_conditions.append(ds[lon_var_name] >= lon_min)
                if lon_max is not None:
                    mask_conditions.append(ds[lon_var_name] <= lon_max)

        # Apply .sel() first if possible
        if selectors:
            ds = ds.sel(selectors)

        # Apply .where() if needed
        # will be slow as LATITUDE and LONGITUDE are variables and not indexed dimensions
        if mask_conditions:
            combined_mask = mask_conditions[0]
            for cond in mask_conditions[1:]:
                combined_mask &= cond
            ds = ds.where(combined_mask, drop=True)

        # Scalar filters (e.g. {'vessel_name': 'VNCF'})
        if scalar_filter:
            for var_name, desired_value in scalar_filter.items():
                if var_name not in ds.variables:
                    self.logger.error(
                        f"Scalar filter variable '{var_name}' does not exist in the dataset."
                    )
                    continue  # or raise ValueError(...) if you'd prefer to halt

                var_data = ds[var_name]

                # Handle string comparisons with stripping
                if hasattr(var_data, "dtype") and np.issubdtype(
                    var_data.dtype, np.str_
                ):
                    mask = var_data.str.strip() == str(desired_value).strip()
                else:
                    mask = var_data == desired_value

                ds = ds.where(mask, drop=True)

        if not selectors and not scalar_filter:
            self.logger.warning(
                "No filters provided to get_data for Zarr source. Returning entire dataset."
            )

        return ds

    def get_spatial_extent(self) -> list[float]:
        """Calculates the spatial extent (min/max lat/lon) of the Zarr dataset.

        Finds the latitude and longitude variables using `_find_lat_lon_vars`
        and computes their minimum and maximum values.

        Returns:
            A list containing [min_lat, min_lon, max_lat, max_lon].
        """
        if self.zarr_store is None:
            self._open_zarr_store()

        lat_var, lon_var = self._find_lat_lon_vars(self.zarr_store)

        min_lat = float(
            lat_var.min().compute() if hasattr(lat_var, "compute") else lat_var.min()
        )
        max_lat = float(
            lat_var.max().compute() if hasattr(lat_var, "compute") else lat_var.max()
        )
        min_lon = float(
            lon_var.min().compute() if hasattr(lon_var, "compute") else lon_var.min()
        )
        max_lon = float(
            lon_var.max().compute() if hasattr(lon_var, "compute") else lon_var.max()
        )

        return [min_lat, min_lon, max_lat, max_lon]

    def plot_spatial_extent(self) -> None:
        """Plots the spatial extent of the Zarr dataset.

        Currently a placeholder. Intended to potentially use the results from
        `get_spatial_extent` and plot them on a map.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        # Placeholder for Zarr spatial extent plotting
        raise NotImplementedError("Zarr spatial extent plotting not yet implemented.")

    def get_temporal_extent(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Calculates and returns the temporal extent of the Zarr dataset.

        Identifies the time variable and calculates the minimum and maximum
        time values from it. Handles cftime objects.

        Returns:
            A tuple containing the minimum and maximum pandas Timestamp objects.

        Raises:
            ValueError: If a suitable time variable cannot be found.
        """
        if self.zarr_store is None:
            self._open_zarr_store()

        time_var_name = _find_var_name_global(
            self.zarr_store,
            ["time", "TIME", "datetime", "date", "Date", "DateTime", "JULD"],
            "time",
        )

        # Calculate and return the temporal extent
        # time_values = self.zarr_store[time_var_name].values # Not strictly needed if using .min()/.max() on DataArray

        # Ensure time_values are sorted before taking min/max for safety, though plot_time_coverage also sorts.
        # However, direct access to .values might not be sorted.
        # For xarray DataArrays, min/max handle this, but if it's a raw numpy array from .values, sorting is safer.
        # Actually, xarray's .min() and .max() on a DataArray should be correct without pre-sorting values.

        min_val = self.zarr_store[time_var_name].min().item()
        max_val = self.zarr_store[time_var_name].max().item()

        cftime_types = (
            cftime.DatetimeGregorian,
            cftime.DatetimeProlepticGregorian,
            cftime.DatetimeJulian,
            cftime.DatetimeNoLeap,
            cftime.DatetimeAllLeap,
            cftime.Datetime360Day,
        )

        if isinstance(min_val, cftime_types):
            min_time = pd.to_datetime(min_val.isoformat())
        else:
            min_time = pd.to_datetime(min_val)

        if isinstance(max_val, cftime_types):
            max_time = pd.to_datetime(max_val.isoformat())
        else:
            max_time = pd.to_datetime(max_val)

        return min_time, max_time

    def plot_time_coverage(self) -> None:
        """Plots a heatmap showing the temporal data coverage for the Zarr dataset.

        Internally finds the time variable and calls the global `plot_time_coverage`
        function.

        Raises:
            ValueError: If a suitable time variable cannot be found.
        """
        if self.zarr_store is None:
            self._open_zarr_store()

        time_var_name = _find_var_name_global(
            self.zarr_store,
            ["time", "TIME", "datetime", "date", "Date", "DateTime", "JULD"],
            "time",
        )

        # Call the global plotting function
        plot_time_coverage(self.zarr_store, time_var=time_var_name)

    def get_temporal_extent_from_timestamp_partition(self):
        raise NotImplementedError(
            "get_temporal_extent_from_timestamp_partition is not supported for ZarrDataSource"
        )

    def get_timeseries_data(
        self,
        lat: float,
        lon: float,
        date_start: str,
        date_end: str,
        var_name: str | None = None,
        lat_name_override: str | None = None,
        lon_name_override: str | None = None,
        time_name_override: str | None = None,
    ) -> TimeSeriesResult:
        """Extracts time series data for specified or all variables at the nearest point from the Zarr store.

        Args:
            lat: The target latitude.
            lon: The target longitude.
            date_start: The start date string (e.g., "YYYY-MM-DD").
            date_end: The end date string (e.g., "YYYY-MM-DD").
            var_name: Optional. The name of the specific data variable to extract.
                      If None, all data variables are extracted.
            lat_name_override: Optional override for the latitude coordinate name.
            lon_name_override: Optional override for the longitude coordinate name.
            time_name_override: Optional override for the time coordinate name.

        Returns:
            A pandas DataFrame containing the time series data. Columns include
            the actual time, latitude, and longitude coordinate names found/used,
            and the requested variable(s).

        Raises:
            ValueError: If requested lat, lon, date_start, or date_end are
                outside dataset bounds, or if coordinate names cannot be found.
        """
        if self.zarr_store is None:
            self.zarr_store = (
                self._open_zarr_store()
            )  # Ensures store is open and sorted by time

        ds = self.zarr_store

        # Determine actual coordinate names
        actual_time_name = (
            time_name_override
            if time_name_override
            else _find_var_name_global(
                ds,
                ["time", "TIME", "datetime", "date", "Date", "DateTime", "JULD"],
                "time",
            )
        )
        actual_lat_name = (
            lat_name_override
            if lat_name_override
            else _find_var_name_global(
                ds, ["latitude", "lat", "LATITUDE", "LAT"], "latitude"
            )
        )
        actual_lon_name = (
            lon_name_override
            if lon_name_override
            else _find_var_name_global(
                ds, ["longitude", "lon", "LONGITUDE", "LON"], "longitude"
            )
        )

        norm_date_start = normalize_date(date_start)
        norm_date_end = normalize_date(date_end)

        # Get latitude, longitude, and time extents for validation
        ds_lat_min, ds_lat_max = (
            ds[actual_lat_name].min().item(),
            ds[actual_lat_name].max().item(),
        )
        ds_lon_min, ds_lon_max = (
            ds[actual_lon_name].min().item(),
            ds[actual_lon_name].max().item(),
        )

        time_min_val = ds[actual_time_name].min().item()
        time_max_val = ds[actual_time_name].max().item()
        cftime_types = (
            cftime.DatetimeGregorian,
            cftime.DatetimeProlepticGregorian,
            cftime.DatetimeJulian,
            cftime.DatetimeNoLeap,
            cftime.DatetimeAllLeap,
            cftime.Datetime360Day,
        )
        ds_time_min = pd.to_datetime(
            time_min_val.isoformat()
            if isinstance(time_min_val, cftime_types)
            else time_min_val
        )
        ds_time_max = pd.to_datetime(
            time_max_val.isoformat()
            if isinstance(time_max_val, cftime_types)
            else time_max_val
        )

        if not (ds_lat_min <= lat <= ds_lat_max):
            raise ValueError(
                f"Latitude {lat} is out of bounds. Dataset latitude extent is ({ds_lat_min}, {ds_lat_max}) using '{actual_lat_name}'."
            )
        if not (ds_lon_min <= lon <= ds_lon_max):
            raise ValueError(
                f"Longitude {lon} is out of bounds. Dataset longitude extent is ({ds_lon_min}, {ds_lon_max}) using '{actual_lon_name}'."
            )
        if not (ds_time_min <= pd.to_datetime(norm_date_start) <= ds_time_max):
            raise ValueError(
                f"Start date {norm_date_start} is out of bounds. Dataset time extent is ({ds_time_min.strftime('%Y-%m-%d %H:%M:%S')}, {ds_time_max.strftime('%Y-%m-%d %H:%M:%S')}) using '{actual_time_name}'."
            )
        if not (ds_time_min <= pd.to_datetime(norm_date_end) <= ds_time_max):
            raise ValueError(
                f"End date {norm_date_end} is out of bounds. Dataset time extent is ({ds_time_min.strftime('%Y-%m-%d %H:%M:%S')}, {ds_time_max.strftime('%Y-%m-%d %H:%M:%S')}) using '{actual_time_name}'."
            )

        # Determine which part of the dataset to select (single variable or all)
        target_ds_selection = ds
        retrieved_var_names = list(ds.data_vars.keys())  # Default to all data vars

        if var_name:
            if var_name in ds.data_vars:
                target_ds_selection = ds[
                    [var_name]
                ]  # Select specific variable, keep as Dataset
                retrieved_var_names = [var_name]
            else:
                self.logger.warning(
                    f"Variable '{var_name}' not found in dataset. Returning all available data variables for the selected point."
                )

        # Slice by time, then select nearest lat/lon
        time_sliced_data = target_ds_selection.sel(
            {actual_time_name: slice(norm_date_start, norm_date_end)}
        )
        selected_data_point = time_sliced_data.sel(
            {actual_lat_name: lat, actual_lon_name: lon}, method="nearest"
        )

        timeseries_df = selected_data_point.to_dataframe().reset_index()

        timeseries_df.attrs["actual_time_name"] = actual_time_name
        timeseries_df.attrs["actual_lat_name"] = actual_lat_name
        timeseries_df.attrs["actual_lon_name"] = actual_lon_name
        timeseries_df.attrs["retrieved_vars"] = (
            retrieved_var_names  # List of vars in the df
        )
        # If a single var was requested (and found), store its name for convenience in plotting
        if var_name and var_name in retrieved_var_names:
            timeseries_df.attrs["requested_var_name"] = var_name
        else:
            timeseries_df.attrs["requested_var_name"] = None

        attributes = ds.attrs
        timeseries_df.attrs = attributes
        for var in ds:
            if var in timeseries_df:
                timeseries_df[var].attrs = ds[var].attrs

        return TimeSeriesResult(
            df=timeseries_df,
            var_name=var_name,
            lat=lat,
            lon=lon,
            date_start=date_start,
            date_end=date_end,
            actual_lat_name=actual_lat_name,
            actual_lon_name=actual_lon_name,
            actual_time_name=actual_time_name,
        )

    def plot_timeseries(
        self,
        var_name: str,
        lat: float,
        lon: float,
        date_start: str,
        date_end: str,
        lat_name_override: str = "lat",
        lon_name_override: str = "lon",
        time_name_override: str = "time",
    ) -> pd.DataFrame:
        """
        Shortcut to extract and plot time series data in one call.

        Internally calls get_timeseries_data(...).plot_timeseries().
        """
        ts = self.get_timeseries_data(
            var_name=var_name,
            lat=lat,
            lon=lon,
            date_start=date_start,
            date_end=date_end,
            lat_name_override=lat_name_override,
            lon_name_override=lon_name_override,
            time_name_override=time_name_override,
        )
        return ts.plot_timeseries()

    def plot_gridded_variable(
        self,
        var_name: str,
        lon_slice: tuple[float, float] | None = None,
        lat_slice: tuple[float, float] | None = None,
        date_start: str | None = None,
        date_end: str | None = None,
        n_days: int = 6,
        coastline_resolution: str | None = "110m",
        log_scale: bool = False,
        lat_name_override: str | None = None,
        lon_name_override: str | None = None,
        time_name_override: str | None = None,
    ) -> None:
        """Plots a gridded variable for multiple consecutive time steps from the Zarr store.

        Displays maps of the specified variable (`var_name`) for up to `n_days`
        consecutive time steps, starting from the time step nearest to `date_start`.
        Plotting is capped by `date_end` if provided.
        The spatial extent is defined by a central point (`lat`, `lon`) and
        radii (`lat_slice_radius_deg`, `lon_slice_radius_deg`).
        Includes coastlines, gridlines, and optional logarithmic color scaling.
        Handles unit conversion from Kelvin to Celsius if applicable.

        Args:
            var_name: The name of the data variable to plot.
            date_start: The start date string (e.g., "YYYY-MM-DD"). Plotting starts
                from the time step nearest to this date.
            date_end: Optional end date string (e.g., "YYYY-MM-DD"). If provided,
                plotting will not extend beyond this date.
            n_days: The maximum number of consecutive time steps to plot. Defaults to 6.
            coastline_resolution: The resolution for the Cartopy coastline
                feature ('110m', '50m', '10m'). Defaults to "110m".
            log_scale: If True, use a logarithmic color scale. Defaults to False.
            lat_name_override: Optional override for the latitude coordinate name.
            lon_name_override: Optional override for the longitude coordinate name.
            time_name_override: Optional override for the time coordinate name.

        Raises:
            ValueError: If slices are outside dataset bounds, no valid data is found,
                        or coordinate names cannot be determined.
            AssertionError: If the dataset does not have the determined time dimension.
        """

        if date_start is None:
            raise ValueError(f"date_start value must be a valid time string, not None")

        if self.zarr_store is None:
            self.zarr_store = self._open_zarr_store()

        ds = self.zarr_store

        actual_lat_name = (
            lat_name_override
            if lat_name_override
            else _find_var_name_global(
                ds, ["latitude", "lat", "LATITUDE", "LAT"], "latitude"
            )
        )
        actual_lon_name = (
            lon_name_override
            if lon_name_override
            else _find_var_name_global(
                ds, ["longitude", "lon", "LONGITUDE", "LON"], "longitude"
            )
        )
        actual_time_name = (
            time_name_override
            if time_name_override
            else _find_var_name_global(
                ds,
                ["time", "TIME", "datetime", "date", "Date", "DateTime", "JULD"],
                "time",
            )
        )

        ds = ds.sortby(actual_time_name)

        # Get latitude and longitude extents
        lat_min, lat_max = (
            ds[actual_lat_name].min().item(),
            ds[actual_lat_name].max().item(),
        )
        lon_min, lon_max = (
            ds[actual_lon_name].min().item(),
            ds[actual_lon_name].max().item(),
        )

        if lat_slice is None:
            lat_slice = (lat_min, lat_max)

        if lon_slice is None:
            lon_slice = (lon_min, lon_max)

        # Get latitude and longitude extents from dataset for validation
        ds_lat_min, ds_lat_max = (
            ds[actual_lat_name].min().item(),
            ds[actual_lat_name].max().item(),
        )
        ds_lon_min, ds_lon_max = (
            ds[actual_lon_name].min().item(),
            ds[actual_lon_name].max().item(),
        )

        # Validate derived slices
        # Ensure slice is within [ds_lat_min, ds_lat_max] and handle reversed axes if necessary
        # For simplicity, we'll check individual bounds of the calculated slice.
        # A more robust check would consider the order of ds[actual_lat_name].values if it can be reversed.

        _lat_slice_to_check = sorted(lat_slice)  # Ensure min, max order for checking
        if not (
            ds_lat_min <= _lat_slice_to_check[0] <= ds_lat_max
            and ds_lat_min <= _lat_slice_to_check[1] <= ds_lat_max
        ):
            raise ValueError(
                f"Derived latitude slice {lat_slice} is out of bounds. Dataset latitude extent is ({ds_lat_min}, {ds_lat_max}) using '{actual_lat_name}'."
            )

        _lon_slice_to_check = sorted(lon_slice)
        if not (
            ds_lon_min <= _lon_slice_to_check[0] <= ds_lon_max
            and ds_lon_min <= _lon_slice_to_check[1] <= ds_lon_max
        ):
            raise ValueError(
                f"Derived longitude slice {lon_slice} is out of bounds. Dataset longitude extent is ({ds_lon_min}, {ds_lon_max}) using '{actual_lon_name}'."
            )

        # Decide on the slice order for actual slicing based on dataset coordinate order
        if ds[actual_lat_name][0] > ds[actual_lat_name][-1]:
            lat_slice_for_sel = (max(lat_slice), min(lat_slice))
        else:
            lat_slice_for_sel = (min(lat_slice), max(lat_slice))

        if ds[actual_lon_name][0] > ds[actual_lon_name][-1]:
            lon_slice_for_sel = (max(lon_slice), min(lon_slice))
        else:
            lon_slice_for_sel = (min(lon_slice), max(lon_slice))

        # Parse dates
        start_date_parsed = pd.to_datetime(date_start)
        end_date_parsed = pd.to_datetime(date_end) if date_end else None

        assert (
            actual_time_name in ds.dims
        ), f"Dataset does not have a '{actual_time_name}' dimension"

        # Find the nearest date in the dataset to start_date_parsed
        try:
            nearest_date_val = ds.sel(
                {actual_time_name: start_date_parsed}, method="nearest"
            )[actual_time_name].data
        except KeyError:  # If sel returns empty
            raise ValueError(
                f"Start date {date_start} is likely outside the dataset's time range for '{actual_time_name}'."
            )

        # Find the position of the nearest date in the time array
        time_coords_values = ds[actual_time_name].values
        try:
            nearest_date_position = np.where(time_coords_values == nearest_date_val)[0][
                0
            ]
        except IndexError:
            raise ValueError(
                f"Could not locate the nearest start date {nearest_date_val} in the time coordinates of '{actual_time_name}'."
            )

        # Get up to n_days date values starting from nearest_date_position
        potential_dates_raw = ds[actual_time_name][
            nearest_date_position : nearest_date_position + n_days
        ].values
        dates_to_plot = [pd.Timestamp(date) for date in potential_dates_raw]

        # Filter by end_date if provided
        if end_date_parsed:
            dates_to_plot = [date for date in dates_to_plot if date <= end_date_parsed]

        if not dates_to_plot:
            self.logger.warning(
                f"No dates to plot for variable '{var_name}' in the specified range [{date_start} - {date_end if date_end else '...'} with n_days={n_days}]."
            )
            return

        var_long_name = ds[var_name].attrs.get("long_name", var_name)
        self.logger.info(
            f"Plotting '{var_long_name}' for {len(dates_to_plot)} time steps."
        )

        num_plots = len(dates_to_plot)
        ncols = 3
        nrows = (num_plots + ncols - 1) // ncols

        fig, axes = plt.subplots(
            nrows=nrows,
            ncols=ncols,
            figsize=(18, 6 * nrows if nrows > 0 else 6),  # Adjust height based on nrows
            subplot_kw={"projection": ccrs.PlateCarree()},
            squeeze=False,  # Ensure axes is always 2D array
        )
        axes_flat = axes.flatten()

        cmap = plt.get_cmap("coolwarm")
        vmin_all, vmax_all = float("inf"), float("-inf")

        # Store data for each plot to avoid re-selecting
        plot_data_cache = {}

        # First pass: gather all data to find global vmin and vmax for consistent color scaling
        for date_obj in dates_to_plot:
            try:
                data = ds[var_name].sel(
                    {
                        actual_time_name: date_obj,  # Use exact match for already selected dates
                        actual_lon_name: slice(
                            lon_slice_for_sel[0], lon_slice_for_sel[1]
                        ),
                        actual_lat_name: slice(
                            lat_slice_for_sel[0], lat_slice_for_sel[1]
                        ),
                    }
                )
                if data.isnull().all():
                    plot_data_cache[date_obj] = None  # Mark as no data
                    continue

                current_var_units = ds[var_name].attrs.get("units", "unknown units")
                if current_var_units.lower() == "kelvin":
                    data = data - 273.15

                plot_data_cache[date_obj] = data  # Cache data (potentially converted)
                vmin_all = min(vmin_all, data.min().item())
                vmax_all = max(vmax_all, data.max().item())

            except Exception as err:
                self.logger.error(
                    f"Processing data for date {date_obj.strftime('%Y-%m-%d %H:%M:%S')}: {err}"
                )
                plot_data_cache[date_obj] = None  # Mark as error/no data

        if not np.isfinite(vmin_all) or not np.isfinite(vmax_all):
            self.logger.warning(
                "No valid data found across all selected dates and coordinates to determine color scale."
            )
            # Clean up empty figure if no plots will be made
            if num_plots > 0 and all(p is None for p in plot_data_cache.values()):
                plt.close(fig)
            return

        norm_to_use = (
            LogNorm(vmin=vmin_all, vmax=vmax_all)
            if log_scale
            else Normalize(vmin=vmin_all, vmax=vmax_all)
        )

        img_for_colorbar = None  # To store one of the plot images for the colorbar

        for idx, date_obj in enumerate(dates_to_plot):
            ax = axes_flat[idx]
            data_to_plot = plot_data_cache.get(date_obj)

            if data_to_plot is None or data_to_plot.isnull().all():
                self.logger.warning(
                    f"No valid data for {date_obj.strftime('%Y-%m-%d %H:%M:%S')}, skipping plot."
                )
                ax.set_title(f"No data for {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
                ax.axis("off")
                continue

            var_units_display = ds[var_name].attrs.get("units", "unknown units")
            if var_units_display.lower() == "kelvin":
                var_units_display = "°C"

            img = data_to_plot.plot(
                ax=ax,
                cmap=cmap,
                norm=norm_to_use,
                add_colorbar=False,
            )
            if img_for_colorbar is None:  # Capture one image for the colorbar
                img_for_colorbar = img

            ax.coastlines(resolution=coastline_resolution)
            ax.add_feature(cfeature.BORDERS, linestyle=":")
            gl = ax.gridlines(
                draw_labels=True, dms=True, x_inline=False, y_inline=False
            )
            gl.top_labels = False
            gl.right_labels = False
            ax.set_title(date_obj.strftime("%Y-%m-%d %H:%M:%S"))

        # Hide any unused subplots
        for i in range(num_plots, len(axes_flat)):
            axes_flat[i].set_visible(False)

        if img_for_colorbar:  # Only add colorbar if at least one plot was made
            cbar_label_text = f"{var_long_name} ({var_units_display})"
            if log_scale:
                cbar_label_text = f"Log({cbar_label_text})"

            # Adjust colorbar position; [left, bottom, width, height] in figure-relative coords
            fig.subplots_adjust(right=0.85)  # Make space for colorbar
            cbar_ax = fig.add_axes([0.88, 0.15, 0.03, 0.7])  # Position to the right
            cbar = fig.colorbar(img_for_colorbar, cax=cbar_ax, orientation="vertical")
            cbar.set_label(cbar_label_text)
            # cbar.set_ticks([vmin_all, (vmin_all + vmax_all) / 2, vmax_all])
            # cbar.ax.set_yticklabels([f"{vmin_all:.2f}", f"{(vmin_all + vmax_all) / 2:.2f}", f"{vmax_all:.2f}"])
        else:  # No plots made, clean up figure
            plt.close(fig)
            self.logger.warning("No images were plotted, so no colorbar will be shown.")
            return

        fig.suptitle(
            f"{var_long_name} Over Time\n",
            fontsize=16,
            fontweight="bold",
        )
        plt.tight_layout(
            rect=[0, 0, 0.85, 0.95]
        )  # Adjust rect to prevent suptitle overlap and leave space for cbar
        plt.show()

    def plot_radar_water_velocity_gridded(self, **kwargs):
        return plot_radar_water_velocity_gridded(self.zarr_store, **kwargs)

    def plot_radar_water_velocity_rose(self, **kwargs):
        return plot_radar_water_velocity_rose(self.zarr_store, **kwargs)

    def get_metadata(self) -> dict:
        """Retrieves metadata from the Zarr store.

        Uses the global `get_zarr_metadata` function.

        Returns:
            A dictionary containing the Zarr store's metadata.
        """
        return get_zarr_metadata(self.dname)


def _find_var_name_global(
    ds: xr.Dataset, common_names: list[str], var_description: str
) -> str:
    """Helper to find a variable/coordinate name from a list of common names.

    Prioritizes coordinates, then 1D data variables.

    Args:
        ds: The xarray Dataset to search within.
        common_names: A list of common names for the variable (e.g., ['time', 'TIME']).
        var_description: A string describing the variable type (e.g., "time", "latitude")
                         for use in error messages.

    Returns:
        The found variable/coordinate name.

    Raises:
        ValueError: If a suitable variable/coordinate cannot be found.
    """
    # Check coordinates first
    for name in common_names:
        if name in ds.coords:
            return name

    # Then check 1D data variables
    for name in common_names:
        if name in ds.data_vars:
            if ds[name].ndim == 1:
                return name

    raise ValueError(
        f"Could not find a suitable 1D {var_description} variable/coordinate in the provided Dataset. Searched for {common_names}."
    )


class GetAodn:
    """
    Main class for discovering and accessing AODN cloud-optimised datasets

    Example:
        aodn = GetAodn()
        aodn = GetAodn(
                    bucket_name="aodn-cloud-optimised",
                    prefix="",
                    s3_fs_opts={
                        "key": "minioadmin",
                        "secret": "minioadmin",
                        "client_kwargs": {
                            "endpoint_url": "http://127.0.0.1:9000"
                        }
                    }
                    )
    """

    def __init__(
        self,
        bucket_name: str = BUCKET_OPTIMISED_DEFAULT,
        prefix: str = ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
        s3_fs_opts: dict | None = None,
    ):
        """Initialises GetAodn with default S3 bucket, prefix, and s3 filesystem options."""
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3_fs_opts = s3_fs_opts or {}

        self.logger = logging.getLogger("aodn.GetAodn")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.propagate = False  # Prevent double logging

    def get_logger(self) -> logging.Logger:
        return self.logger

    def list_datasets(self) -> list[str]:
        """
        Lists the top-level 'folders' (datasets) available in the S3 bucket under the configured prefix.

        Returns:
            list: A list of dataset names (folder names).
        """
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        s3_prefix = self.prefix
        if s3_prefix and not s3_prefix.endswith("/"):
            s3_prefix += "/"

        paginator = s3.get_paginator("list_objects_v2")
        datasets = []

        for page in paginator.paginate(
            Bucket=self.bucket_name, Prefix=s3_prefix, Delimiter="/"
        ):
            for common_prefix in page.get("CommonPrefixes", []):
                folder_path = common_prefix["Prefix"]  # Full path from bucket root
                # Extract the folder name relative to s3_prefix
                relative_folder_path = folder_path
                if s3_prefix:  # if s3_prefix is not empty
                    relative_folder_path = folder_path[len(s3_prefix) :]

                # Remove trailing slash to get the dataset name
                dataset_name = relative_folder_path.rstrip("/")
                if (
                    dataset_name
                ):  # Ensure it's not an empty string if prefix itself was listed
                    datasets.append(dataset_name)

        return sorted(list(set(datasets)))  # Sort and ensure uniqueness

    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        """Retrieves a DataSource object for the specified dataset.

        Infers the data format (Parquet or Zarr) from the file extension
        in `dataset_name_with_ext` and returns the corresponding DataSource
        implementation instance.

        Args:
            dataset_name_with_ext: The name of the dataset including its
                extension (e.g., "my_data.parquet", "my_data.zarr").

        Returns:
            An instance of `ParquetDataSource` or `ZarrDataSource`.

        Raises:
            ValueError: If the extension is not ".parquet" or ".zarr".
        """
        if dataset_name_with_ext.endswith(".parquet"):
            return ParquetDataSource(
                self.bucket_name,
                self.prefix,
                dataset_name_with_ext,
                s3_fs_opts=self.s3_fs_opts,
            )
        elif dataset_name_with_ext.endswith(".zarr"):
            return ZarrDataSource(
                self.bucket_name,
                self.prefix,
                dataset_name_with_ext,
                s3_fs_opts=self.s3_fs_opts,
            )
        else:
            raise ValueError(
                f"Unsupported dataset extension in '{dataset_name_with_ext}'. Must end with '.parquet' or '.zarr'."
            )

    def get_metadata(self) -> "Metadata":
        """Returns a Metadata object for browsing dataset metadata.

        Instantiates and returns a `Metadata` object configured with the
        same bucket and prefix as this `GetAodn` instance.

        Returns:
            A `Metadata` instance.
        """
        # This will need to be adapted to potentially list both Parquet and Zarr datasets
        # and fetch metadata accordingly. For now, it retains Parquet-centric logic.
        return Metadata(self.bucket_name, self.prefix)


class Metadata:
    """Provides methods to access and query metadata across datasets."""

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        logger: logging.Logger | None = None,
        s3_fs_opts: dict | None = None,
    ):
        """Initialises the Metadata object.

        Args:
            bucket_name: The S3 bucket name containing the datasets.
            prefix: The S3 prefix (folder path) within the bucket where
                datasets reside.
            logger: Optional logger to use. If not provided, a default logger will be created.
        """
        # super().__init__()
        # initialise the class by calling the needed methods
        self.logger = logger or logging.getLogger("aodn.GetAodn")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.propagate = False

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3_fs_opts = s3_fs_opts
        self.catalog = self.metadata_catalog()

    def metadata_catalog_uncached(self, s3_fs_opts=None) -> dict:
        """Builds the metadata catalog by scanning S3 and reading metadata.

        Lists folders ending in ".parquet/" and ".zarr/" under the configured
        prefix. For each found dataset, it calls the appropriate metadata
        retrieval function (`get_schema_metadata` or `get_zarr_metadata`)
        and stores the result in a dictionary keyed by the dataset name
        (without the extension).

        Note:
            If datasets exist in both Parquet and Zarr format with the same base
            name, the Zarr metadata will overwrite the Parquet metadata in the
            current implementation.

        Returns:
            A dictionary where keys are dataset base names (str) and values
            are their corresponding metadata dictionaries.
        """
        # print('Running metadata_catalog_uncached...')  # Debug output

        # This method currently only lists Parquet datasets.
        # It will need to be updated to discover Zarr datasets as well.
        catalog = {}

        # Process Parquet datasets
        folders_with_parquet = self.list_folders_with_data(".parquet/")
        for (
            dataset_path
        ) in folders_with_parquet:  # dataset_path is like 'my_dataset.parquet/'
            # Construct dname for Parquet metadata
            dname = (
                PureS3Path.from_uri(f"s3://anonymous@{self.bucket_name}/{self.prefix}/")
                .joinpath(dataset_path)  # Use dataset_path directly
                .as_uri()
            )
            dname = dname.replace("s3://anonymous%40", "")

            try:
                metadata = get_schema_metadata(dname, s3_fs_opts=s3_fs_opts)
            except Exception as e:
                self.logger.error(
                    f"Processing Parquet metadata from {dataset_path}: {e}"
                )
                continue

            # Extract dataset_name from dataset_path (e.g., 'my_dataset' from 'my_dataset.parquet/')
            dataset_name = dataset_path.rstrip("/")
            catalog[dataset_name] = metadata

        # Process Zarr datasets
        folders_with_zarr = self.list_folders_with_data(".zarr/")
        for (
            dataset_path
        ) in folders_with_zarr:  # dataset_path is like 'my_dataset.zarr/'
            dname = (
                PureS3Path.from_uri(f"s3://anonymous@{self.bucket_name}/{self.prefix}/")
                .joinpath(dataset_path)  # Use dataset_path directly
                .as_uri()
            )
            dname = dname.replace("s3://anonymous%40", "s3://")

            try:
                metadata = get_zarr_metadata(dname, s3_fs_opts=s3_fs_opts)
            except Exception as e:
                self.logger.error(f"Processing Zarr metadata from {dataset_path}: {e}")
                continue

            dataset_name = dataset_path.rstrip("/")
            # If a dataset with the same name (but different format) exists,
            # we might want to merge or handle it. For now, Zarr will overwrite if name clashes.
            # A more robust solution might involve storing format in the catalog key or structure.
            catalog[dataset_name] = metadata

        return catalog

    @lru_cache(maxsize=None)
    def metadata_catalog(self) -> dict:
        """Returns the metadata catalog, using a cache.

        Calls `metadata_catalog_uncached` on the first call and caches the
        result for subsequent calls.

        Returns:
            The cached metadata catalog dictionary.
        """
        # print('Running metadata_catalog...')  # Debug output
        if "catalog" in self.__dict__:
            return self.catalog
        else:
            return self.metadata_catalog_uncached(s3_fs_opts=self.s3_fs_opts)

    def list_folders_with_data(self, extension_filter: str = ".parquet/") -> list[str]:
        """Lists folders in S3 matching a specific extension filter.

        Uses `boto3.list_objects_v2` with a delimiter to find common prefixes
        (folders) under `self.prefix` that end with the specified
        `extension_filter`.

        Args:
            extension_filter: The file extension (including trailing slash,
                e.g., ".parquet/", ".zarr/") to filter folders by.
                Defaults to ".parquet/".

        Returns:
            A list of relative folder paths (e.g., "my_dataset.parquet/")
            matching the filter.
        """
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        # prefix_path = self.prefix # Ensure prefix is treated as a path component

        # if (prefix_path is not None) and (not prefix_path.endswith("/")):
        #    prefix_path += "/"

        # Ensure prefix is correctly formatted for S3 listing
        # If prefix is empty, list from the root of the bucket.
        # If prefix is not empty and doesn't end with '/', add it.
        s3_prefix = self.prefix
        if s3_prefix and not s3_prefix.endswith("/"):
            s3_prefix += "/"

        response = s3.list_objects_v2(
            Bucket=self.bucket_name, Prefix=s3_prefix, Delimiter="/"
        )

        folders = []

        for common_prefix in response.get("CommonPrefixes", []):
            folder_path = common_prefix[
                "Prefix"
            ]  # This is the full path from bucket root
            # We need the relative path from self.prefix
            relative_folder_path = folder_path
            if s3_prefix:  # if s3_prefix is not empty
                relative_folder_path = folder_path[len(s3_prefix) :]

            if relative_folder_path.endswith(extension_filter):
                # folder_name = folder_path[len(s3_prefix):] # Get the name relative to the prefix
                folders.append(relative_folder_path)
        return folders

    def find_datasets_with_attribute(
        self,
        target_value: str,
        target_key: str = "standard_name",
        data_dict: dict | None = None,
        threshold: int = 80,
    ) -> list[str]:
        """Finds datasets having an attribute matching a target value using fuzzy matching.

        Recursively searches through the metadata catalog (`data_dict`) for
        nested dictionaries containing the `target_key`. It compares the value
        associated with `target_key` to the `target_value` using fuzzy string
        matching (`fuzzywuzzy.fuzz.partial_ratio`). If the similarity score
        meets or exceeds the `threshold`, the dataset name is added to the results.

        Args:
            target_value: The string value to search for.
            target_key: The metadata key to check the value of (e.g.,
                "standard_name", "long_name"). Defaults to "standard_name".
            data_dict: The metadata catalog dictionary to search within. If None,
                uses `self.metadata_catalog()`. Defaults to None.
            threshold: The minimum similarity score (0-100) required for a match.
                Defaults to 80.

        Returns:
            A list of unique dataset names whose metadata contains a sufficiently
            similar value for the specified target key.
        """
        matching_datasets = []
        # https://stackoverflow.com/questions/56535948/python-why-cant-you-use-a-self-variable-as-an-optional-argument-in-a-method
        if data_dict == None:
            data_dict = self.metadata_catalog()

        if not isinstance(data_dict, dict):
            return matching_datasets  # handle bad cases

        for dataset_name, attributes in data_dict.items():
            if not isinstance(attributes, dict):
                continue

            for key, value in attributes.items():
                if isinstance(value, dict) and target_key in value:
                    # Check for any attribute available in a dict(catalog) match using fuzzy matching
                    current_standard_name = value.get(target_key, "")
                    similarity_score = fuzz.partial_ratio(
                        target_value.lower(), current_standard_name.lower()
                    )
                    if similarity_score >= threshold:
                        matching_datasets.append(
                            dataset_name
                        )  # Add dataset name to list

                # Recursively search
                matching_datasets.extend(
                    self.find_datasets_with_attribute(value, target_value, threshold)
                )

        return list(set(matching_datasets))


# =====================================================================
# Monkey patching of pandas dataframe to add custom downloading method
# such as:
# df.aodn.download_as_netcdf()
# df.aodn.download_as_csv()
# =====================================================================


@pd.api.extensions.register_dataframe_accessor("aodn")
class AODNAccessor:
    """Custom accessor for AODN-specific DataFrame download utilities.

    This accessor is available on any pandas DataFrame as `.aodn` and provides
    methods to export the DataFrame in formats useful for AODN workflows.
    """

    def __init__(self, pandas_obj: pd.DataFrame):
        """Initialises the AODN accessor with the parent DataFrame.

        Args:
            pandas_obj (pd.DataFrame): The DataFrame to which this accessor is attached.
        """
        self._obj = pandas_obj

    @staticmethod
    def df_to_cf_compliant_xarray(df: pd.DataFrame) -> xr.Dataset:
        """
        Convert a DataFrame with string (object) columns to a CF-compliant xarray Dataset.

        Object (string) columns are converted to integer codes, with CF-compliant
        'flag_values' and 'flag_meanings' attributes.

        Args:
            df (pd.DataFrame): Input DataFrame with potential object (string) columns.

        Returns:
            xr.Dataset: CF-compliant Dataset ready for NetCDF export.
        """
        data_vars = {}
        for col in df.columns:
            col_data = df[col]

            # Exclude _QC columns from transformation
            if col.endswith("_QC"):
                data_vars[col] = xr.DataArray(col_data.values, dims=["obs"])
                continue

            if pd.api.types.is_object_dtype(col_data):
                # Convert to categorical
                cat = pd.Categorical(col_data)
                codes = cat.codes
                categories = list(cat.categories)

                # Add CF-compliant flags
                data_vars[col] = xr.DataArray(
                    codes,
                    dims=["obs"],
                    attrs={
                        "flag_values": list(range(len(categories))),
                        "flag_meanings": " ".join(
                            str(cat).replace(" ", "_") for cat in categories
                        ),
                    },
                )
            else:
                # Use as-is
                data_vars[col] = xr.DataArray(col_data.values, dims=["obs"])

        # Create Dataset
        return xr.Dataset(data_vars)

    @staticmethod
    def hash_dataframe(df: pd.DataFrame) -> str:
        # Create a stable string representation of the DataFrame
        content = df.to_csv(index=False).encode("utf-8")
        return hashlib.sha256(content).hexdigest()[:8]

    def to_csv(
        self, dataset_name: str | None = None, output_dir: str | None = None
    ) -> str:
        """Save the DataFrame as a compressed CSV with metadata comments.

        Args:
            dataset_name (str | None): Name of the dataset. If None, tries df.attrs['dataset_name'], otherwise 'unknown'.

        Returns:
            str: Path to the .csv.zip file.
        """
        df = self._obj

        # Resolve dataset name
        if dataset_name is None:
            dataset_name = df.attrs.get("dataset_name", "unknown")

        hash_val = self.hash_dataframe(df)
        metadata_uuid = df.attrs.get("metadata_uuid", "unknown")
        filename_base = (
            f"aodn_metadata-uuid_{metadata_uuid}_{dataset_name}_data-hash_{hash_val}"
        )
        zip_filename = f"{filename_base}.csv.zip"
        csv_filename = f"{filename_base}.csv"

        if output_dir is None:
            temp_dir = os.getcwd()
            # temp_dir = tempfile.mkdtemp()
        else:
            os.makedirs(output_dir)
            temp_dir = output_dir

        zip_path = os.path.join(temp_dir, zip_filename)

        # Build metadata header
        metadata_lines = []

        if df.attrs:
            metadata_lines.append("# Global Attributes:")
            for key, value in df.attrs.items():
                metadata_lines.append(f"# {key}: {value}")
            metadata_lines.append("#")

        metadata_lines.append("# Variable Attributes:")
        for col in df.columns:
            attrs = getattr(df[col], "attrs", {})
            for key, value in attrs.items():
                metadata_lines.append(f"# {col}.{key}: {value}")
        metadata_lines.append("#")

        # Write CSV to a string buffer
        buffer = StringIO()
        for line in metadata_lines:
            buffer.write(line + "\n")
        df.to_csv(buffer, index=False)

        # Write to zip file
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(csv_filename, buffer.getvalue())

        return os.path.abspath(zip_path)

    def download_as_csv(self, dataset_name: str | None = None) -> FileLink:
        """Save the DataFrame as a zipped CSV and return a download link (Jupyter only).

        Args:
            dataset_name (str | None): Dataset name, defaults from df.attrs if not provided.

        Returns:
            FileLink: Jupyter download link.
        """
        zip_path = self.to_csv(dataset_name)
        if is_colab():
            from google.colab import files

            print(
                "run in a new cell the following to download the CSV file:\n\nfiles.download(zip_path)\n\n Alternatively, the file is accessible via the folder icon on the left menu"
            )

        else:
            return FileLink(os.path.basename(zip_path))

    def download_as_netcdf(
        self, filename: str = "aodn_data.nc", compression_level: int = 4
    ) -> FileLink:
        df = self._obj.map(lambda x: x.strip() if isinstance(x, str) else x)

        ds = self.df_to_cf_compliant_xarray(df)

        comp = dict(zlib=True, complevel=compression_level)
        encoding = {}

        for var_name, da in ds.data_vars.items():
            # Compress only numeric variables (skip strings / objects)
            if da.dtype.kind in ("i", "u", "f"):  # integer, unsigned int, float
                encoding[var_name] = comp

        ds.to_netcdf(path=filename, encoding=encoding)
        return FileLink(filename)
