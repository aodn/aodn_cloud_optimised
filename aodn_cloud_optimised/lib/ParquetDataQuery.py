"""
A currated list of functions used to facilitate reading AODN parquet files. These are used by the various Jupyter
Notebooks
"""
import json
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Final

import boto3
import geopandas as gpd
import gsw  # TEOS-10 library
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pyarrow.parquet as pq
from botocore import UNSIGNED
from botocore.client import Config
from fuzzywuzzy import fuzz
from s3path import PureS3Path
from shapely import wkb
from shapely.geometry import Polygon, MultiPolygon

REGION: Final[str] = "ap-southeast-2"
ENDPOINT_URL = f"https://s3.ap-southeast-2.amazonaws.com"
BUCKET_OPTIMISED_DEFAULT = "aodn-cloud-optimised"
ROOT_PREFIX_CLOUD_OPTIMISED_PATH = ""


def query_unique_value(dataset: pq.ParquetDataset, partition: str) -> set:
    """Query the unique values of a specified partition name from a ParquetDataset.

    Args:
        dataset (pyarrow.parquet.ParquetDataset): The ParquetDataset to query.
        partition (str): The name of the partition to query on.

    Returns:
        set[str]: A set containing the unique values of the specified partition.
    """
    unique_values = set()
    pattern = re.compile(f".*/{partition}=([^/]*)/")
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
    unique_timestamps = query_unique_value(parquet_ds, "timestamp")
    unique_timestamps = np.array([np.int64(string) for string in unique_timestamps])
    unique_timestamps = np.sort(unique_timestamps)

    return (
        datetime.fromtimestamp(unique_timestamps.min(), tz=timezone.utc),
        datetime.fromtimestamp(unique_timestamps.max(), tz=timezone.utc),
    )


def get_timestamps_boundary_values(
    parquet_ds: pq.ParquetDataset, date_start: str, date_end: str
):
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
    """Retrieve the spatial extent (multi-polygon) from a Parquet dataset.

    This function retrieves the spatial extent (multi-polygon) represented by unique polygons
    found in the 'polygon' partition of the given Parquet dataset.

    Args:
        parquet_ds (pyarrow.parquet.ParquetDataset): The Parquet dataset containing polygon partitions.

    Returns:
        shapely.geometry.MultiPolygon: A multi-polygon representing the spatial extent.
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
    gdf.plot(color="red", alpha=0.5)  # Adjust color and alpha as needed

    # Show the plot
    plt.show()


def plot_ts_diagram(df, temp_col="TEMP", psal_col="PSAL", depth_col="DEPTH"):
    """
    Plots a T-S (Temperature-Salinity) diagram with density contours.

    Args:
        df (pd.DataFrame): DataFrame containing temperature, salinity, and depth columns.
        temp_col (str): Column name for temperature data (default is 'TEMP').
        psal_col (str): Column name for salinity data (default is 'PSAL').
        depth_col (str): Column name for depth data (default is 'DEPTH').
    """
    # Filter data where PSAL >= 25
    filtered_df = df[(df[psal_col] >= 25)]

    fig, ax = plt.subplots(figsize=(10, 6))

    # Define the colormap for depth
    depths = filtered_df[depth_col]
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
    cbar = plt.colorbar(sc, ax=ax, label="Depth (m)", orientation="vertical")

    ax.set_xlabel("Salinity (PSAL)")
    ax.set_ylabel("Temperature (TEMP)")
    ax.set_title("T-S Diagram with Density Contours")

    plt.xticks(rotation=45)

    plt.tight_layout()
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


def decode_and_load_json(metadata):
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


####################################################################################################################
# Work done during IMOS HACKATHON 2024
# https://github.com/aodn/IMOS-hackathon/blob/main/2024/Projects/CARSv2/notebooks/get_aodn_example_hackathon.ipynb
###################################################################################################################
class GetAodn:
    def __init__(self):
        self.bucket_name = BUCKET_OPTIMISED_DEFAULT
        self.prefix = ROOT_PREFIX_CLOUD_OPTIMISED_PATH

    def get_dataset(self, dataset_name):
        return Dataset(self.bucket_name, self.prefix, dataset_name)

    def get_metadata(self):
        return Metadata(self.bucket_name, self.prefix)


class Dataset:
    def __init__(self, bucket_name, prefix, dataset_name):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.dataset_name = dataset_name

        # creating path with PureS3Path to handle windows, and handle empty self.prefix
        self.dname = (
            PureS3Path.from_uri(f"s3://anonymous@{self.bucket_name}/{self.prefix}/")
            .joinpath(f"{self.dataset_name}.parquet/")
            .as_uri()
        )
        self.dname = self.dname.replace("s3://anonymous%40", "")

        self.parquet_ds = self._create_parquet_dataset()

    def _create_parquet_dataset(self, filters=None):

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

    def partition_keys_list(self):
        dataset = self._create_parquet_dataset()
        partition_keys = dataset.partitioning.schema
        return partition_keys

    def get_spatial_extent(self):
        return get_spatial_extent(self.parquet_ds)

    def plot_spatial_extent(self):
        return plot_spatial_extent(self.parquet_ds)

    def get_temporal_extent(self):
        return get_temporal_extent(self.parquet_ds)

    def get_data(
        self,
        date_start=None,
        date_end=None,
        lat_min=None,
        lat_max=None,
        lon_min=None,
        lon_max=None,
        scalar_filter=None,
    ):
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
            filesystem=fs.S3FileSystem(
                region=REGION, endpoint_override=ENDPOINT_URL, anonymous=True
            ),
        )

        return df

    def get_metadata(self):
        return get_schema_metadata(self.dname)


class Metadata:
    def __init__(self, bucket_name, prefix):
        # super().__init__()
        # initialise the class by calling the needed methods
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.catalog = self.metadata_catalog()

    def metadata_catalog_uncached(self):
        # print('Running metadata_catalog_uncached...')  # Debug output

        folders_with_parquet = self.list_folders_with_parquet()
        catalog = {}

        for dataset in folders_with_parquet:
            dname = (
                PureS3Path.from_uri(f"s3://anonymous@{self.bucket_name}/{self.prefix}/")
                .joinpath(f"{dataset}")
                .as_uri()
            )
            dname = dname.replace("s3://anonymous%40", "")

            try:
                metadata = get_schema_metadata(dname)  # schema metadata
            except Exception as e:
                print(f"Error processing metadata from {dataset}, {e}")
                continue

            path_parts = dataset.strip("/").split("/")
            last_folder_with_extension = path_parts[-1]
            dataset_name = os.path.splitext(last_folder_with_extension)[0]

            catalog[dataset_name] = metadata

        return catalog

    @lru_cache(maxsize=None)
    def metadata_catalog(self):

        # print('Running metadata_catalog...')  # Debug output
        if "catalog" in self.__dict__:
            return self.catalog
        else:
            return self.metadata_catalog_uncached()

    def list_folders_with_parquet(self):
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        prefix = self.prefix

        # if (prefix is not None) and (not prefix.endswith("/")):
        #    prefix += "/"

        response = s3.list_objects_v2(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter="/"
        )

        folders = []

        for prefix in response.get("CommonPrefixes", []):
            folder_path = prefix["Prefix"]
            if folder_path.endswith(".parquet/"):
                folder_name = folder_path[len(prefix) - 1 :]
                folders.append(folder_name)

        return folders

    def find_datasets_with_attribute(
        self, target_value, target_key="standard_name", data_dict=None, threshold=80
    ):

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
