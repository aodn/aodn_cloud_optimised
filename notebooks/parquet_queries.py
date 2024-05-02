"""
A currated list of functions used to facilitate reading AODN parquet files. These are used by the various Jupyter
Notebooks
"""
import json
import os
import re
from datetime import datetime

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from shapely import wkb
from shapely.geometry import Polygon, MultiPolygon


def query_unique_value(dataset: pq.ParquetDataset, partition: str) -> set:
    """Query the unique values of a specified partition name from a ParquetDataset.

     Args:
         dataset (pyarrow.parquet.ParquetDataset): The ParquetDataset to query.
         partition (str): The name of the partition to query on.

     Returns:
         set[str]: A set containing the unique values of the specified partition.
     """
    unique_values = set()
    pattern = re.compile(f'.*/{partition}=([^/]*)/')
    for p in dataset.fragments:
        value = re.match(pattern, p.path).group(1)
        unique_values.add(value)
    return unique_values


def get_temporal_extent(parquet_ds):
    """Calculate the temporal extent (start and end timestamps) of a Parquet dataset.

    This function determines the temporal extent of a Parquet dataset based on unique timestamps
    found in the 'timestamp' partition.

    Args:
        parquet_ds (pyarrow.parquet.ParquetDataset): The Parquet dataset to analyze.

    Returns:
        tuple: A tuple containing the temporal extent of the dataset.
               The first element is the datetime corresponding to the minimum timestamp value,
               and the second element is the datetime corresponding to the maximum timestamp value.
    """
    unique_timestamps = query_unique_value(parquet_ds, 'timestamp')
    unique_timestamps = np.array([np.int64(string) for string in unique_timestamps])
    unique_timestamps = np.sort(unique_timestamps)

    return datetime.fromtimestamp(unique_timestamps.min()), datetime.fromtimestamp(unique_timestamps.max())


def get_timestamps_boundary_values(parquet_ds: pq.ParquetDataset, date_start: str, date_end: str):
    """
    Get the boundary values of timestamps from a Parquet dataset based on the specified date range.

    Args:
        parquet_ds (pyarrow.parquet.ParquetDataset): The Parquet dataset.
        date_start (str): The start date in string format (e.g., "YYYY-MM-DD").
        date_end (str): The end date in string format (e.g., "YYYY-MM-DD").

    Returns:
        tuple: A tuple containing the boundary values of timestamps for the specified date range.
               The first element is the timestamp corresponding to the closest value before date_start,
               and the second element is the timestamp corresponding to the closest value after date_end.
    """

    # Get the unique partition values of timestamp available in the parquet dataset
    unique_timestamps = query_unique_value(parquet_ds, 'timestamp')
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


def create_bbox_filter(parquet_ds, **kwargs):
    """
    Create a filter expression to select data within a bounding box from a Parquet dataset.

    Args:
        parquet_ds (pyarrow.parquet.ParquetDataset): The Parquet dataset to filter.
        kwargs (dict): Keyword arguments specifying the bounding box coordinates:
            lon_min (float): The minimum longitude of the bounding box.
            lon_max (float): The maximum longitude of the bounding box.
            lat_min (float): The minimum latitude of the bounding box.
            lat_max (float): The maximum latitude of the bounding box.
            lat_varname (str, optional): The latitude variable name.
            lon_varname (str, optional): The longitude variable name.

    Returns:
        pyarrow.compute.Expression: A filter expression for selecting data within the specified bounding box.

    Example:
        filter_expr = create_bbox_filter(parquet_ds, lon_min=-180, lon_max=180, lat_min=-90, lat_max=90)
    """
    lon_min = kwargs.get('lon_min')
    lon_max = kwargs.get('lon_max')
    lat_min = kwargs.get('lat_min')
    lat_max = kwargs.get('lat_max')

    lat_varname = kwargs.get('lat_varname', "LATITUDE")
    lon_varname = kwargs.get('lon_varname', "LONGITUDE")

    if None in (lon_min, lon_max, lat_min, lat_max):
        raise ValueError("Bounding box coordinates must be provided.")

    bounding_box = [(lon_min, lat_max), (lon_max, lat_max), (lon_max, lat_min), (lon_min, lat_min)]
    bounding_box_polygon = Polygon(bounding_box)

    polygon_partitions = query_unique_value(parquet_ds, 'polygon')
    wkb_list = list(polygon_partitions)

    polygon_set = set(map(lambda x: wkb.loads(bytes.fromhex(x)), wkb_list))
    polygon_array_partitions = np.array(list(polygon_set))

    results = [polygon.intersects(bounding_box_polygon) for polygon in polygon_array_partitions]

    # Filter polygon_array_partitions based on results
    intersecting_polygons = [polygon for polygon, result in zip(polygon_array_partitions, results) if result]

    if intersecting_polygons == []:
        raise ValueError("No data for given bounding box. Amend lat/lon values ")

    # Convert intersecting polygons to WKB hexadecimal strings
    wkb_list = [polygon.wkb_hex for polygon in intersecting_polygons]

    expression = None
    for wkb_polygon in wkb_list:
        sub_expr = pc.field('polygon') == wkb_polygon
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


def create_time_filter(parquet_ds, **kwargs):
    """
    Create a filter expression to select data within a time range from a Parquet dataset.

    Args:
        parquet_ds (pyarrow.parquet.ParquetDataset): The Parquet dataset to filter.
        kwargs (dict): Keyword arguments specifying the time range:
            date_start (str, optional): The start date in the format 'YYYY-MM-DD'.
            date_end (str, optional): The end date in the format 'YYYY-MM-DD'.
            time_varname (str, optional): The time variable.


    Returns:
        pyarrow.compute.Expression: A filter expression for selecting data within the specified time range.

    Example:
        filter_expr = create_time_filter(parquet_ds, date_start='2023-01-01', date_end='2023-12-31')
    """
    date_start = kwargs.get('date_start')
    date_end = kwargs.get('date_end')
    time_varname = kwargs.get('time_varname', "TIME")

    if None in (date_start, date_end):
        raise ValueError("Start and end dates must be provided.")

    timestamp_start, timestamp_end = get_timestamps_boundary_values(parquet_ds, date_start, date_end)

    expr1 = pc.field('timestamp') >= np.int64(timestamp_start)
    expr2 = pc.field('timestamp') <= np.int64(timestamp_end)

    # ARGO Specifiq:
    if "TIME" in parquet_ds.schema.names:
        time_varname = 'TIME'
    elif "JULD" in parquet_ds.schema.names:
        time_varname = 'JULD'

    expr3 = pc.field(time_varname) >= pd.to_datetime(date_start)
    expr4 = pc.field(time_varname) <= pd.to_datetime(date_end)

    expression = expr1 & expr2 & expr3 & expr4
    return expression


def get_spatial_extent(parquet_ds: pq.ParquetDataset) -> MultiPolygon:
    """Retrieve the spatial extent (multi-polygon) from a Parquet dataset.

    This function retrieves the spatial extent (multi-polygon) represented by unique polygons
    found in the 'polygon' partition of the given Parquet dataset.

    Args:
        parquet_ds (pyarrow.parquet.ParquetDataset): The Parquet dataset containing polygon partitions.

    Returns:
        shapely.geometry.MultiPolygon: A multi-polygon representing the spatial extent.
    """
    # Retrieve unique polygon partitions
    polygon_partitions = query_unique_value(parquet_ds, 'polygon')

    # Convert WKB hex strings to Shapely geometries and create a set of unique polygons
    wkb_list = list(polygon_partitions)
    polygon_set = set(map(lambda x: wkb.loads(bytes.fromhex(x)), wkb_list))

    # Convert the set of polygons to a numpy array
    polygon_array_partitions = np.array(list(polygon_set))

    # Create a MultiPolygon from the array of polygons
    multi_polygon = MultiPolygon(polygon_array_partitions.tolist())

    return multi_polygon


def plot_spatial_extent(parquet_ds):
    """Retrieve the spatial extent (multi-polygon) from a Parquet dataset.

    This function retrieves the spatial extent (multi-polygon) represented by unique polygons
    found in the 'polygon' partition of the given Parquet dataset.

    Args:
        parquet_ds (pyarrow.parquet.ParquetDataset): The Parquet dataset containing polygon partitions.

    Returns:
        shapely.geometry.MultiPolygon: A multi-polygon representing the spatial extent.
    """
    multi_polygon = get_spatial_extent(parquet_ds)

    #%config InlineBackend.figure_format = 'retina'
    #%config InlineBackend.rc = {'figure.figsize': (10.0, 8.0)}

    # Assuming 'multi_polygon' is your MultiPolygon object
    gdf = gpd.GeoDataFrame(geometry=[multi_polygon])

    # Plot the MultiPolygon with customized color and transparency
    gdf.plot(color='red', alpha=0.5)  # Adjust color and alpha as needed

    # Show the plot
    plt.show()


def get_schema_metadata(dname):
    """Retrieve pyarrow_schema metadata from a Parquet dataset directory.

    This function reads the pyarrow_schema metadata from the common metadata file
    associated with a Parquet dataset directory.

    Args:
        dname (str): The S3 path of the Parquet dataset (without '_common_metadata').

    Returns:
        dict: A dictionary containing the decoded pyarrow_schema metadata.
            The keys are metadata keys (decoded from bytes to UTF-8 strings),
            and the values are metadata values (parsed from JSON strings to Python objects).
    """
    parquet_meta = pa.parquet.read_schema(os.path.join(dname, '_common_metadata'))
    # horrible ... but got to be done. The dictionary of metadata has to be a dictionnary with byte keys and byte values.
    # meaning that we can't have nested dictionaries ...
    decoded_meta = {key.decode('utf-8'): json.loads(value.decode('utf-8').replace("'", '"')) for key, value in
                    parquet_meta.metadata.items()}
    return decoded_meta