"""
A currated list of functions used to facilitate reading AODN parquet files. These are used by the various Jupyter
Notebooks
"""

import json
import os
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Final

import boto3
import cartopy.crs as ccrs  # For coastline plotting
import cartopy.feature as cfeature
import fsspec
import cftime
import geopandas as gpd
import gsw  # TEOS-10 library
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as fs
import pyarrow.parquet as pq
import seaborn as sns
import xarray as xr
from botocore import UNSIGNED
from botocore.client import Config
from fuzzywuzzy import fuzz
from matplotlib.colors import LogNorm, Normalize
from s3path import PureS3Path
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon
from windrose import WindroseAxes

REGION: Final[str] = "ap-southeast-2"
ENDPOINT_URL = f"https://s3.ap-southeast-2.amazonaws.com"
BUCKET_OPTIMISED_DEFAULT = "aodn-cloud-optimised"
ROOT_PREFIX_CLOUD_OPTIMISED_PATH = ""


def query_unique_value(dataset: pq.ParquetDataset, partition: str) -> set[str]:
    """Queries unique values for a given Hive partition key in a Parquet dataset.

    Iterates through the fragments (files) of a Parquet dataset and extracts
    the unique values associated with the specified partition key from the
    file paths. Assumes Hive partitioning (e.g., ".../partition=value/...").

    Args:
        dataset: The pyarrow.parquet.ParquetDataset object.
        partition: The name of the partition key (e.g., "year", "timestamp").

    Returns:
        A set containing the unique string values found for the partition key.
    """
    unique_values = set()
    pattern = re.compile(f".*/{partition}=([^/]*)/")
    for p in dataset.fragments:
        value = re.match(pattern, p.path).group(1)
        unique_values.add(value)
    return unique_values


def get_temporal_extent_v1(parquet_ds: pq.ParquetDataset) -> tuple[datetime, datetime]:
    """Calculates temporal extent based *only* on 'timestamp' partition values.

    This is a faster but potentially less accurate method than `get_temporal_extent`
    as it relies solely on the timestamp partition values, which might represent
    time bins rather than exact start/end times within the data.

    Args:
        parquet_ds: The pyarrow.parquet.ParquetDataset object.

    Returns:
        A tuple containing the minimum and maximum datetime objects derived
        from the 'timestamp' partition values, localized to UTC.
    """
    unique_timestamps = query_unique_value(parquet_ds, "timestamp")
    unique_timestamps = np.array([np.int64(string) for string in unique_timestamps])
    unique_timestamps = np.sort(unique_timestamps)

    return (
        datetime.fromtimestamp(unique_timestamps.min(), tz=timezone.utc),
        datetime.fromtimestamp(unique_timestamps.max(), tz=timezone.utc),
    )


def get_temporal_extent(parquet_ds: pq.ParquetDataset) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Calculates the precise temporal extent by reading min/max time variable values.

    This method finds the minimum and maximum 'timestamp' partition values, then
    reads the actual time variable (e.g., 'TIME' or 'JULD') from the corresponding
    Parquet files to get the precise minimum and maximum timestamps in the dataset.
    This is more accurate but slower than `get_temporal_extent_v1`.

    Args:
        parquet_ds: The pyarrow.parquet.ParquetDataset object.

    Returns:
        A tuple containing the minimum and maximum pandas Timestamp objects
        found within the dataset's time variable.
    """
    dname = f"s3://anonymous@{parquet_ds.__dict__['_base_dir']}"
    unique_timestamps = query_unique_value(parquet_ds, "timestamp")
    unique_timestamps = np.array([np.int64(string) for string in unique_timestamps])
    unique_timestamps = np.sort(unique_timestamps)

    if "TIME" in parquet_ds.schema.names:
        time_varname = "TIME"
    elif "JULD" in parquet_ds.schema.names:
        time_varname = "JULD"

    expr = pc.field("timestamp") == np.int64(unique_timestamps.max())
    df = pd.read_parquet(dname, engine="pyarrow", columns=[time_varname], filters=expr)
    time_max = df[time_varname].max()

    expr = pc.field("timestamp") == np.int64(unique_timestamps.min())
    df = pd.read_parquet(dname, engine="pyarrow", columns=[time_varname], filters=expr)
    time_min = df[time_varname].min()

    return time_min, time_max


def get_timestamps_boundary_values(
    parquet_ds: pq.ParquetDataset, date_start: str, date_end: str
) -> tuple[np.int64, np.int64]:
    """Finds partition timestamp boundaries bracketing a date range.

    Given a start and end date, this function identifies the 'timestamp'
    partition values in the Parquet dataset that immediately precede the
    start date and the end date. This is useful for filtering partitions
    efficiently.

    Args:
        parquet_ds: The pyarrow.parquet.ParquetDataset object.
        date_start: The start date string (e.g., "YYYY-MM-DD").
        date_end: The end date string (e.g., "YYYY-MM-DD").

    Returns:
        A tuple containing two int64 timestamps:
        - The partition timestamp value just before or equal to `date_start`.
        - The partition timestamp value just before or equal to `date_end`.
    """

    # Get the unique partition values of timestamp available in the parquet dataset
    unique_timestamps = query_unique_value(parquet_ds, "timestamp")
    unique_timestamps = np.array([np.int64(string) for string in unique_timestamps])
    unique_timestamps = np.sort(unique_timestamps)

    # We need to find the matching values of timestamp. the following logic does this
    # 1) convert simply the date_start and date_end into timestamps
    timestamp_start = pd.to_datetime(date_start).timestamp()
    timestamp_end = pd.to_datetime(date_end).timestamp()

    # 2) Look for the closest value and get the one before fore timestamp_start
    index = np.searchsorted(unique_timestamps, timestamp_start)
    if index == 0:
        timestamp_start = unique_timestamps[index]
    else:
        timestamp_start = unique_timestamps[index - 1]

    # 3) Look for the closest value and get the one after fore timestamp_end
    index = np.searchsorted(unique_timestamps, timestamp_end)
    timestamp_end = unique_timestamps[index - 1]

    return timestamp_start, timestamp_end


def create_bbox_filter(parquet_ds: pq.ParquetDataset, **kwargs) -> pc.Expression:
    """Creates a PyArrow filter expression for spatial bounding box queries.

    Combines partition pruning based on the 'polygon' partition key (finding
    partitions intersecting the query box) with row-level filtering based on
    latitude and longitude variable values.

    Args:
        parquet_ds: The pyarrow.parquet.ParquetDataset object.
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

    polygon_partitions = query_unique_value(parquet_ds, "polygon")
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


def create_time_filter(parquet_ds: pq.ParquetDataset, **kwargs) -> pc.Expression:
    """Creates a PyArrow filter expression for temporal range queries.

    Combines partition pruning based on the 'timestamp' partition key (using
    `get_timestamps_boundary_values`) with row-level filtering based on the
    actual time variable values (e.g., 'TIME' or 'JULD').

    Args:
        parquet_ds: The pyarrow.parquet.ParquetDataset object.
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

    timestamp_start, timestamp_end = get_timestamps_boundary_values(
        parquet_ds, date_start, date_end
    )

    expr1 = pc.field("timestamp") >= np.int64(timestamp_start)
    expr2 = pc.field("timestamp") <= np.int64(timestamp_end)

    # ARGO Specific:
    if "TIME" in parquet_ds.schema.names:
        time_varname = "TIME"
    elif "JULD" in parquet_ds.schema.names:
        time_varname = "JULD"

    expr3 = pc.field(time_varname) >= pd.to_datetime(date_start)
    expr4 = pc.field(time_varname) <= pd.to_datetime(date_end)

    expression = expr1 & expr2 & expr3 & expr4
    return expression


def get_spatial_extent(parquet_ds: pq.ParquetDataset) -> MultiPolygon:
    """Retrieves the spatial extent as a MultiPolygon from 'polygon' partitions.

    Reads the unique values from the 'polygon' partition key, decodes the WKB
    hex strings into Shapely Polygon objects, and combines them into a single
    MultiPolygon object representing the total spatial coverage defined by the
    partitions.

    Args:
        parquet_ds: The pyarrow.parquet.ParquetDataset object.

    Returns:
        A Shapely MultiPolygon object.
    """
    # Retrieve unique polygon partitions
    polygon_partitions = query_unique_value(parquet_ds, "polygon")

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
        datetime.strptime(date_str, "%Y-%m-%d")
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
    parquet_ds: pq.ParquetDataset, coastline_resolution: str = "110m"
) -> None:
    """Plots the spatial extent derived from Parquet 'polygon' partitions.

    Retrieves the spatial extent using `get_spatial_extent`, creates a
    GeoDataFrame, and plots it on a map with coastlines, borders, land,
    and ocean features using Cartopy.

    Args:
        parquet_ds: The pyarrow.parquet.ParquetDataset object.
        coastline_resolution: The resolution for the Cartopy coastline
            feature ('110m', '50m', '10m'). Defaults to "110m".
    """
    multi_polygon = get_spatial_extent(parquet_ds)

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
    ax.add_feature(cfeature.OCEAN, facecolor="lightblue")

    # Add grid lines

    gl = ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
    gl.xlocator = mticker.FixedLocator(range(-180, 181, 10))  # Longitudes every 10°
    gl.ylocator = mticker.FixedLocator(range(-90, 91, 10))  # Latitudes every 10°
    gl.xlabel_style = {"size": 10, "color": "black"}
    gl.ylabel_style = {"size": 10, "color": "black"}

    ax.set_title("Spatial Extent and Coastline")
    ax.legend()

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
    ds: xr.Dataset, time_start: str, time_end: str, time_name: str = "TIME"
) -> None:
    """Plots a velocity rose (similar to wind rose) for radar data.

    Calculates the time-averaged U and V velocity components (UCUR, VCUR)
    over the specified time range, derives average speed and direction, and
    plots them using the `windrose` library.

    Args:
        ds: The input xarray Dataset, expected to contain 'UCUR', 'VCUR',
            'LONGITUDE', 'LATITUDE', and a time coordinate.
        time_start: The start time string for averaging (e.g., "YYYY-MM-DDTHH:MM:SS").
        time_end: The end time string for averaging (e.g., "YYYY-MM-DDTHH:MM:SS").
        time_name: The name of the time coordinate in `ds`. Defaults to "TIME".
    """
    # Select the data in the specified time range
    subset = ds.sel({time_name: slice(time_start, time_end)})

    # Calculate average speed and direction
    u_avg = subset.UCUR.mean(dim=time_name).values
    v_avg = subset.VCUR.mean(dim=time_name).values
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
    ax.set_title(f"Velocity Rose: {time_start} to {time_end}")
    ax.set_legend(title="Speed (m/s)", loc="lower right", bbox_to_anchor=(1.2, 0))

    # Show the plot
    plt.show()


def plot_radar_water_velocity_gridded(
    ds: xr.Dataset,
    time_start: str,
    time_name: str = "TIME",
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
    ds = ds.sortby(time_name)

    # Create a 3x2 grid of subplots with Cartopy projections
    fig, axes = plt.subplots(
        nrows=3,
        ncols=2,
        figsize=(15, 15),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )

    # Locate the time index for the starting time
    ii = 0
    iTime = list(ds[time_name].values).index(
        ds.sel({time_name: time_start}, method="nearest")[time_name]
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
                f"{np.datetime_as_string(ds[time_name].values[iTime + ii])}"
            )

            ii += 1

    # Add a common colorbar
    fig.colorbar(p, cax=cbar_ax, label="Speed (m/s)")

    # Adjust layout for better appearance
    plt.tight_layout(rect=[0, 0, 0.9, 1])  # Leave space for the colorbar

    # Show the plot
    plt.show()


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
    log_scale: bool = False,
) -> None:
    """Plots a gridded variable for multiple consecutive time steps.

    Displays maps of the specified variable (`var_name`) for `n_days`
    consecutive time steps, starting from the time step nearest to `start_date`.
    Allows spatial slicing and optional logarithmic color scaling. Includes
    coastlines and gridlines. Handles unit conversion from Kelvin to Celsius
    if applicable.

    Args:
        ds: The input xarray Dataset.
        start_date: The start date string (e.g., "YYYY-MM-DD"). Plotting starts
            from the time step nearest to this date.
        lon_slice: Optional tuple (min_lon, max_lon) for longitude slicing.
            Defaults to the full dataset extent.
        lat_slice: Optional tuple (min_lat, max_lat) for latitude slicing.
            Defaults to the full dataset extent. Handles reversed latitude axes.
        var_name: The name of the data variable to plot. Defaults to
            "sea_surface_temperature".
        n_days: The number of consecutive time steps to plot. Defaults to 6.
        lat_name: The name of the latitude coordinate/variable in `ds`.
            Defaults to "lat".
        lon_name: The name of the longitude coordinate/variable in `ds`.
            Defaults to "lon".
        time_name: The name of the time coordinate in `ds`. Defaults to "time".
        coastline_resolution: The resolution for the Cartopy coastline
            feature ('110m', '50m', '10m'). Defaults to "110m".
        log_scale: If True, use a logarithmic color scale. Defaults to False.

    Raises:
        ValueError: If the requested `lat_slice` or `lon_slice` are outside
            the dataset bounds.
        ValueError: If no valid data is found within the selected time and
            spatial range.
        AssertionError: If the dataset does not have the specified `time_name`
            dimension.
    """

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
    print(f"Nearest date in dataset: {nearest_date}")

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
    print(f"Variable Long Name: {var_long_name}")

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
                print(
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
            print(f"Error processing date {date.strftime('%Y-%m-%d')}: {err}")
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
                print(
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
            print(f"Error processing date {date.strftime('%Y-%m-%d %H:%M:%S')}: {err}")
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
    df: pd.DataFrame, temp_col: str = "TEMP", psal_col: str = "PSAL", z_col: str = "DEPTH"
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


def get_schema_metadata(dname: str) -> dict:
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

    parquet_meta = pa.parquet.read_schema(
        os.path.join(name, "_common_metadata"),
        # Pyarrow can infer file system from path prefix with s3 but it will try
        # to scan local file system before infer and get a pyarrow s3 file system
        # which is very slow to start, read_schema no s3 prefix needed
        filesystem=fs.S3FileSystem(
            region=REGION, endpoint_override=ENDPOINT_URL, anonymous=True
        ),
    )

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
    decoded_metadata = {}
    for key, value in metadata.items():
        try:
            # Decode bytes to string
            value_str = value.decode("utf-8")
            value_str = value_str.replace("'", '"')

            decoded_metadata[key.decode("utf-8")] = json.loads(value_str)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for key {key}: {e}")
            print(f"Problematic JSON string: {value_str}")
        except Exception as e:
            print(f"Unexpected error for key {key}: {e}")
    return decoded_metadata


def get_zarr_metadata(dname: str) -> dict:
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
    name = dname.replace("anonymous@", "")

    try:
        # Use fsspec mapper for xarray to access S3 anonymously
        with xr.open_zarr(
            fsspec.get_mapper(name, anon=True), chunks=None, consolidated=True
        ) as ds:
            metadata = {"global_attributes": ds.attrs.copy()}
            for var_name, variable in ds.variables.items():
                metadata[var_name] = variable.attrs.copy()
        return metadata
    except Exception as e:
        print(f"Error opening or processing Zarr metadata from {dname}: {e}")
        return {"global_attributes": {}, "error": str(e)}


####################################################################################################################
# Work done during IMOS HACKATHON 2024
# https://github.com/aodn/IMOS-hackathon/blob/main/2024/Projects/CARSv2/notebooks/get_aodn_example_hackathon.ipynb
###################################################################################################################
class DataSource(ABC):
    """Abstract Base Class for accessing different data source formats (Parquet, Zarr)."""
    def __init__(self, bucket_name: str, prefix: str, dataset_name: str):
        """Initialises the DataSource.

        Args:
            bucket_name: The S3 bucket name.
            prefix: The S3 prefix (folder path) within the bucket.
            dataset_name: The name of the dataset (including extension,
                e.g., "my_data.parquet" or "my_data.zarr").
        """
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.dataset_name = dataset_name
        self.dname = self._build_data_path()

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
        var_name: str | None = None, # Made var_name optional
        lat_name_override: str | None = None,
        lon_name_override: str | None = None,
        time_name_override: str | None = None,
    ) -> pd.DataFrame:
        """Extracts time series data for a variable (or all variables) at the nearest point."""
        pass

    @abstractmethod
    def plot_timeseries(
        self,
        timeseries_df: pd.DataFrame,
        var_name: str, # Specific variable from the DataFrame to plot
        lat: float,
        lon: float,
        date_start: str,
        date_end: str,
        actual_lat_name: str, 
        actual_lon_name: str,
        actual_time_name: str,
    ) -> None:
        """Plots the extracted time series data."""
        pass


class ParquetDataSource(DataSource):
    """DataSource implementation for Parquet datasets."""
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

    def __init__(self, bucket_name: str, prefix: str, dataset_name: str):
        """Initialises the ParquetDataSource.

        Args:
            bucket_name: The S3 bucket name.
            prefix: The S3 prefix (folder path) within the bucket.
            dataset_name: The name of the dataset including the '.parquet'
                extension.
        """
        super().__init__(bucket_name, prefix, dataset_name)
        self.parquet_ds = self._create_parquet_dataset()

    def _create_parquet_dataset(self, filters=None) -> pq.ParquetDataset:
        """Creates a PyArrow ParquetDataset object for the data source.

        Args:
            filters: Optional PyArrow filter expression to apply when creating
                the dataset object (for metadata filtering). Defaults to None.

        Returns:
            A pyarrow.parquet.ParquetDataset instance.
        """
        return pq.ParquetDataset(
            self.dname,
            partitioning="hive",
            filters=filters,
            # Pyarrow can infer file system from path prefix with s3 but it will try
            # to scan local file system before infer and get a pyarrow s3 file system
            # which is very slow to start, ParquetDataset no s3 prefix needed
            filesystem=fs.S3FileSystem(
                region=REGION, endpoint_override=ENDPOINT_URL, anonymous=True
            ),
        )

    def partition_keys_list(self) -> pa.Schema:
        """Returns the schema of the Hive partitioning keys.

        Returns:
            A pyarrow.Schema object representing the partition keys.
        """
        dataset = self._create_parquet_dataset()
        partition_keys = dataset.partitioning.schema
        return partition_keys

    def get_spatial_extent(self) -> MultiPolygon:
        """Returns the spatial extent derived from 'polygon' partitions.

        Uses the global `get_spatial_extent` function.

        Returns:
            A Shapely MultiPolygon object.
        """
        return get_spatial_extent(self.parquet_ds)

    def plot_spatial_extent(self) -> None:
        """Plots the spatial extent derived from 'polygon' partitions.

        Uses the global `plot_spatial_extent` function.
        """
        return plot_spatial_extent(self.parquet_ds)

    def get_temporal_extent(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Returns the precise temporal extent by reading min/max time values.

        Uses the global `get_temporal_extent` function.

        Returns:
            A tuple containing the minimum and maximum pandas Timestamp objects.
        """
        return get_temporal_extent(self.parquet_ds)

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
    ) -> pd.DataFrame:
        """Retrieves data from the Parquet dataset, applying filters.

        Constructs PyArrow filter expressions based on the provided time range,
        bounding box, and scalar filter conditions. Reads the filtered data
        using `pd.read_parquet`.

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
            filter_time = create_time_filter(
                self.parquet_ds, date_start=date_start, date_end=date_end
            )

        # Geometry filter requires ALL optional args to be defined
        if lat_min is None or lat_max is None or lon_min is None or lon_max is None:
            filter_geo = None
        else:
            filter_geo = create_bbox_filter(
                self.parquet_ds,
                lat_min=lat_min,
                lat_max=lat_max,
                lon_min=lon_min,
                lon_max=lon_max,
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
            data_filter = data_filter & expr

        # Set file system explicitly do not require folder prefix s3://
        df = pd.read_parquet(
            self.dname,
            engine="pyarrow",
            filters=data_filter,
            columns=columns,
            filesystem=fs.S3FileSystem(
                region=REGION, endpoint_override=ENDPOINT_URL, anonymous=True
            ),
        )

        return df

    def get_metadata(self) -> dict:
        """Retrieves metadata from the Parquet dataset's common metadata.

        Uses the global `get_schema_metadata` function.

        Returns:
            A dictionary containing the decoded metadata.
        """
        return get_schema_metadata(self.dname)

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
    ) -> pd.DataFrame:
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

    def __init__(self, bucket_name: str, prefix: str, dataset_name: str):
        """Initialises the ZarrDataSource.

        Args:
            bucket_name: The S3 bucket name.
            prefix: The S3 prefix (folder path) within the bucket.
            dataset_name: The name of the dataset including the '.zarr'
                extension.
        """
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
            ds = xr.open_zarr(
                fsspec.get_mapper(self.dname, anon=True), chunks=None, consolidated=True
            )
            # Find the time variable name to sort by
            time_names = ['time', 'TIME', 'datetime', 'date', 'Date', 'DateTime', 'JULD']
            # We need a logger instance here if _find_var_name uses it.
            # Since _find_var_name is part of ZarrDataSource, it can call self.get_logger()
            # However, _open_zarr_store is called in __init__ before logger might be fully set up by CommonHandler if not careful.
            # For now, assuming _find_var_name can access a logger or doesn't strictly need it for this path.
            # A safer approach might be to pass a logger or make _find_var_name static if it doesn't need self.
            # Let's assume self.get_logger() is available or _find_var_name handles its absence.
            try:
                time_var_name = self._find_var_name(ds, time_names, "time")
                return ds.sortby(time_var_name)
            except ValueError as ve:
                # Log this, but still return the unsorted dataset if time var not found for sorting.
                # Or re-raise if sorting is critical. For now, let's log and return.
                # Consider if self.get_logger() is available here.
                # If not, use a simple print or a default logger.
                print(f"Warning: Could not find time variable to sort Zarr store {self.dname}: {ve}. Returning unsorted.")
                return ds # Return unsorted if time var not found, or raise
        except Exception as e:
            print(f"Error opening Zarr store {self.dname}: {e}")
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
        lat_names = ['latitude', 'lat', 'LATITUDE', 'LAT']
        lon_names = ['longitude', 'lon', 'LONGITUDE', 'LON']

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
            raise ValueError(f"Latitude variable/coordinate not found in dataset {self.dataset_name}. Searched for {lat_names}.")
        if lon_var is None:
            raise ValueError(f"Longitude variable/coordinate not found in dataset {self.dataset_name}. Searched for {lon_names}.")
            
        return lat_var, lon_var

    def _find_var_name(self, ds: xr.Dataset, common_names: list[str], var_description: str) -> str:
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
                # else:
                #     # self.get_logger().debug( # ZarrDataSource does not have get_logger
                #     #     f"Found potential {var_description} variable '{name}' but it is multi-dimensional. Skipping for sel()."
                #     # )
        
        raise ValueError(
            f"Could not find a suitable 1D {var_description} variable/coordinate in the Zarr store {self.dataset_name}. Searched for {common_names}."
        )

    def get_data(
        self,
        date_start: str | None = None,
        date_end: str | None = None,
        lat_min: float | None = None,
        lat_max: float | None = None,
        lon_min: float | None = None,
        lon_max: float | None = None,
    ) -> xr.Dataset:
        """Retrieves data from the Zarr store, applying spatio-temporal filters.

        Uses xarray's `sel()` method to slice the data based on the provided
        time, latitude, and longitude ranges.

        Args:
            date_start: Optional start date string (e.g., "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS").
            date_end: Optional end date string.
            lat_min: Optional minimum latitude for slicing.
            lat_max: Optional maximum latitude for slicing.
            lon_min: Optional minimum longitude for slicing.
            lon_max: Optional maximum longitude for slicing.

        Returns:
            An xarray.Dataset containing the selected data.

        Raises:
            ValueError: If essential coordinate names (time, lat, lon) cannot be found.
        """
        if self.zarr_store is None:
            self._open_zarr_store()

        selectors = {}

        # Time slicing
        if date_start is not None or date_end is not None:
            time_names = ['time', 'TIME', 'datetime', 'date', 'Date', 'DateTime', 'JULD']
            time_var_name = self._find_var_name(self.zarr_store, time_names, "time")
            selectors[time_var_name] = slice(date_start, date_end)

        # Latitude slicing
        if lat_min is not None or lat_max is not None:
            lat_names = ['latitude', 'lat', 'LATITUDE', 'LAT']
            lat_var_name = self._find_var_name(self.zarr_store, lat_names, "latitude")
            selectors[lat_var_name] = slice(lat_min, lat_max)

        # Longitude slicing
        if lon_min is not None or lon_max is not None:
            lon_names = ['longitude', 'lon', 'LONGITUDE', 'LON']
            lon_var_name = self._find_var_name(self.zarr_store, lon_names, "longitude")
            selectors[lon_var_name] = slice(lon_min, lon_max)
        
        if not selectors:
            self.get_logger().warning("No filters provided to get_data for Zarr source. Returning entire dataset.")
            return self.zarr_store

        return self.zarr_store.sel(selectors)

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

        min_lat = float(lat_var.min().compute() if hasattr(lat_var, 'compute') else lat_var.min())
        max_lat = float(lat_var.max().compute() if hasattr(lat_var, 'compute') else lat_var.max())
        min_lon = float(lon_var.min().compute() if hasattr(lon_var, 'compute') else lon_var.min())
        max_lon = float(lon_var.max().compute() if hasattr(lon_var, 'compute') else lon_var.max())
        
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
        """Plots time coverage and returns the temporal extent of the Zarr dataset.

        Identifies the time variable, calls the global `plot_time_coverage`
        function, and calculates the minimum and maximum time values from the
        time variable. Handles cftime objects.

        Returns:
            A tuple containing the minimum and maximum pandas Timestamp objects.

        Raises:
            ValueError: If a suitable time variable cannot be found.
        """
        if self.zarr_store is None:
            self._open_zarr_store()

        time_var_name = None
        possible_time_names = ['time', 'TIME', 'datetime', 'date', 'Date', 'DateTime'] # Common time variable names

        # Check in coordinates
        for name in possible_time_names:
            if name in self.zarr_store.coords:
                time_var_name = name
                break
        
        # If not in coordinates, check in data variables
        if time_var_name is None:
            for name in possible_time_names:
                if name in self.zarr_store.data_vars:
                    time_var_name = name
                    break
        
        if time_var_name is None:
            raise ValueError(
                f"Could not find a suitable time variable in the Zarr store {self.dataset_name}. Searched for {possible_time_names}."
            )

        # Call the plotting function as requested
        plot_time_coverage(self.zarr_store, time_var=time_var_name)

        # Calculate and return the temporal extent
        time_values = self.zarr_store[time_var_name].values

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
    ) -> pd.DataFrame:
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
            self.zarr_store = self._open_zarr_store() # Ensures store is open and sorted by time

        ds = self.zarr_store

        # Determine actual coordinate names
        actual_time_name = time_name_override if time_name_override else self._find_var_name(ds, ['time', 'TIME', 'datetime', 'date', 'Date', 'DateTime', 'JULD'], "time")
        actual_lat_name = lat_name_override if lat_name_override else self._find_var_name(ds, ['latitude', 'lat', 'LATITUDE', 'LAT'], "latitude")
        actual_lon_name = lon_name_override if lon_name_override else self._find_var_name(ds, ['longitude', 'lon', 'LONGITUDE', 'LON'], "longitude")

        norm_date_start = normalize_date(date_start)
        norm_date_end = normalize_date(date_end)

        # Get latitude, longitude, and time extents for validation
        ds_lat_min, ds_lat_max = ds[actual_lat_name].min().item(), ds[actual_lat_name].max().item()
        ds_lon_min, ds_lon_max = ds[actual_lon_name].min().item(), ds[actual_lon_name].max().item()
        
        time_min_val = ds[actual_time_name].min().item()
        time_max_val = ds[actual_time_name].max().item()
        cftime_types = (cftime.DatetimeGregorian, cftime.DatetimeProlepticGregorian, cftime.DatetimeJulian, cftime.DatetimeNoLeap, cftime.DatetimeAllLeap, cftime.Datetime360Day)
        ds_time_min = pd.to_datetime(time_min_val.isoformat() if isinstance(time_min_val, cftime_types) else time_min_val)
        ds_time_max = pd.to_datetime(time_max_val.isoformat() if isinstance(time_max_val, cftime_types) else time_max_val)

        if not (ds_lat_min <= lat <= ds_lat_max):
            raise ValueError(f"Latitude {lat} is out of bounds. Dataset latitude extent is ({ds_lat_min}, {ds_lat_max}) using '{actual_lat_name}'.")
        if not (ds_lon_min <= lon <= ds_lon_max):
            raise ValueError(f"Longitude {lon} is out of bounds. Dataset longitude extent is ({ds_lon_min}, {ds_lon_max}) using '{actual_lon_name}'.")
        if not (ds_time_min <= pd.to_datetime(norm_date_start) <= ds_time_max):
            raise ValueError(f"Start date {norm_date_start} is out of bounds. Dataset time extent is ({ds_time_min.strftime('%Y-%m-%d %H:%M:%S')}, {ds_time_max.strftime('%Y-%m-%d %H:%M:%S')}) using '{actual_time_name}'.")
        if not (ds_time_min <= pd.to_datetime(norm_date_end) <= ds_time_max):
            raise ValueError(f"End date {norm_date_end} is out of bounds. Dataset time extent is ({ds_time_min.strftime('%Y-%m-%d %H:%M:%S')}, {ds_time_max.strftime('%Y-%m-%d %H:%M:%S')}) using '{actual_time_name}'.")

        # Determine which part of the dataset to select (single variable or all)
        target_ds_selection = ds
        retrieved_var_names = list(ds.data_vars.keys()) # Default to all data vars

        if var_name:
            if var_name in ds.data_vars:
                target_ds_selection = ds[[var_name]] # Select specific variable, keep as Dataset
                retrieved_var_names = [var_name]
            else:
                # Using print as logger is not available in DataSource
                print(f"Warning: Variable '{var_name}' not found in dataset. Returning all available data variables for the selected point.")
        
        # Slice by time, then select nearest lat/lon
        time_sliced_data = target_ds_selection.sel({actual_time_name: slice(norm_date_start, norm_date_end)})
        selected_data_point = time_sliced_data.sel({actual_lat_name: lat, actual_lon_name: lon}, method="nearest")
        
        timeseries_df = selected_data_point.to_dataframe().reset_index()
        
        timeseries_df.attrs['actual_time_name'] = actual_time_name
        timeseries_df.attrs['actual_lat_name'] = actual_lat_name
        timeseries_df.attrs['actual_lon_name'] = actual_lon_name
        timeseries_df.attrs['retrieved_vars'] = retrieved_var_names # List of vars in the df
        # If a single var was requested (and found), store its name for convenience in plotting
        if var_name and var_name in retrieved_var_names:
             timeseries_df.attrs['requested_var_name'] = var_name
        else:
            timeseries_df.attrs['requested_var_name'] = None


        return timeseries_df

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
        """Plots the extracted time series data.

        Args:
            timeseries_df: DataFrame obtained from get_timeseries_data.
            var_name: The name of the variable plotted.
            lat: The target latitude for which data was extracted.
            lon: The target longitude for which data was extracted.
            start_time: The requested start time for the series.
            end_time: The requested end time for the series.
            actual_lat_name: Actual latitude coordinate name in timeseries_df.
            actual_lon_name: Actual longitude coordinate name in timeseries_df.
            actual_time_name: Actual time coordinate name in timeseries_df.
        """
        if self.zarr_store is None:
            self.zarr_store = self._open_zarr_store()
        
        ds = self.zarr_store # For accessing attributes

        if var_name not in timeseries_df.columns:
            print(f"Warning: Variable '{var_name}' not found in the provided DataFrame. Cannot plot.")
            # Attempt to plot the first variable from retrieved_vars if available
            if timeseries_df.attrs.get('retrieved_vars'):
                var_name_to_plot = timeseries_df.attrs['retrieved_vars'][0]
                print(f"Attempting to plot the first available variable: '{var_name_to_plot}'")
            else:
                return # Cannot plot
        else:
            var_name_to_plot = var_name

        # The DataFrame should have columns: actual_time_name, actual_lat_name, actual_lon_name, and var_name_to_plot
        # Plotting
        plt.figure() # Create a new figure
        plt.plot(timeseries_df[actual_time_name], timeseries_df[var_name_to_plot])

        # Use actual lat/lon values from the DataFrame for the title, as 'nearest' might pick a slightly different point.
        plot_lat_val = timeseries_df[actual_lat_name].iloc[0] if not timeseries_df.empty else lat
        plot_lon_val = timeseries_df[actual_lon_name].iloc[0] if not timeseries_df.empty else lon
        
        norm_date_start = normalize_date(date_start)
        norm_date_end = normalize_date(date_end)

        # Get attributes from the original Zarr store for the plotted variable
        var_attrs = ds[var_name_to_plot].attrs if var_name_to_plot in ds else {}
        long_name = var_attrs.get('long_name', var_name_to_plot)
        units = var_attrs.get('units', 'unitless')

        plt.title(
            f"{long_name} at {actual_lat_name}={plot_lat_val:.2f}, "
            f"{actual_lon_name}={plot_lon_val:.2f} from {norm_date_start} to {norm_date_end}"
        )
        plt.xlabel(f"Time ({actual_time_name})")
        plt.ylabel(f"{long_name} ({units})")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    def get_metadata(self) -> dict:
        """Retrieves metadata from the Zarr store.

        Uses the global `get_zarr_metadata` function.

        Returns:
            A dictionary containing the Zarr store's metadata.
        """
        return get_zarr_metadata(self.dname)


class GetAodn:
    """Main class for discovering and accessing AODN cloud-optimised datasets."""
    def __init__(self):
        """Initialises GetAodn with default S3 bucket and prefix."""
        self.bucket_name = BUCKET_OPTIMISED_DEFAULT
        self.prefix = ROOT_PREFIX_CLOUD_OPTIMISED_PATH

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
        
        paginator = s3.get_paginator('list_objects_v2')
        datasets = []

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=s3_prefix, Delimiter="/"):
            for common_prefix in page.get("CommonPrefixes", []):
                folder_path = common_prefix["Prefix"]  # Full path from bucket root
                # Extract the folder name relative to s3_prefix
                relative_folder_path = folder_path
                if s3_prefix: # if s3_prefix is not empty
                    relative_folder_path = folder_path[len(s3_prefix):]
                
                # Remove trailing slash to get the dataset name
                dataset_name = relative_folder_path.rstrip('/')
                if dataset_name: # Ensure it's not an empty string if prefix itself was listed
                    datasets.append(dataset_name)
        
        return sorted(list(set(datasets))) # Sort and ensure uniqueness

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
            return ParquetDataSource(self.bucket_name, self.prefix, dataset_name_with_ext)
        elif dataset_name_with_ext.endswith(".zarr"):
            return ZarrDataSource(self.bucket_name, self.prefix, dataset_name_with_ext)
        else:
            raise ValueError(
                f"Unsupported dataset extension in '{dataset_name_with_ext}'. Must end with '.parquet' or '.zarr'."
            )

    def get_metadata(self) -> 'Metadata':
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
    def __init__(self, bucket_name: str, prefix: str):
        """Initialises the Metadata object.

        Args:
            bucket_name: The S3 bucket name containing the datasets.
            prefix: The S3 prefix (folder path) within the bucket where
                datasets reside.
        """
        # super().__init__()
        # initialise the class by calling the needed methods
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.catalog = self.metadata_catalog()

    def metadata_catalog_uncached(self) -> dict:
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
                metadata = get_schema_metadata(dname)
            except Exception as e:
                print(f"Error processing Parquet metadata from {dataset_path}: {e}")
                continue

            # Extract dataset_name from dataset_path (e.g., 'my_dataset' from 'my_dataset.parquet/')
            dataset_name = os.path.splitext(dataset_path.strip("/"))[0]
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
                metadata = get_zarr_metadata(dname)
            except Exception as e:
                print(f"Error processing Zarr metadata from {dataset_path}: {e}")
                continue

            dataset_name = os.path.splitext(dataset_path.strip("/"))[0]
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
            return self.metadata_catalog_uncached()

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
