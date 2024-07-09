# AODN Cloud Optimised Conversion

![Build Status](https://github.com/aodn/aodn_cloud_optimised/actions/workflows/build.yml/badge.svg)
![Test Status](https://github.com/aodn/aodn_cloud_optimised/actions/workflows/test-mamba.yml/badge.svg)
![Release](https://img.shields.io/github/v/release/aodn/aodn_cloud_optimised.svg)
[![codecov](https://codecov.io/gh/aodn/aodn_cloud_optimised/branch/main/graph/badge.svg)](https://codecov.io/gh/aodn/aodn_cloud_optimised/branch/main)

A tool designed to convert IMOS NetCDF and CSV files into Cloud Optimised formats such as Zarr and Parquet

## Key Features

* Conversion of CSV/NetCDF to Cloud Optimised format (Zarr/Parquet)
  * YAML configuration approach with parent and child YAML configuration if multiple dataset are very similar (i.e. Radar ACORN, GHRSST, see [config](https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/config/dataset))
  * Generic handlers for most dataset ([GenericParquetHandler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/GenericParquetHandler.py), [GenericZarrHandler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/GenericZarrHandler.py)).
  * Specific handlers can be written and inherits methods from a generic handler ([Argo handler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/ArgoHandler.py), [Mooring Timseries Handler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/AnmnHourlyTsHandler.py))
* Clustering capability:
  * Local dask cluster
  * Remote Coiled cluster
  * driven by configuration/can be easily overwritten
  * Zarr: gridded dataset are done in batch and in parallel with xarray.open_mfdataset
  * Parquet: tabular files are done in batch and in parallel as independent task, done with future
* Reprocessing:
  * Zarr,: reprocessing is achieved by writting to specific regions with slices. Non-contigous regions are handled
  * Parquet: reprocessing is done via pyarrow internal overwritting function, but can also be forced in case an input file has significantly changed
* Chunking:
  * Parquet: to facilitate the query of geospatial data, polygon and timestamp slices are created as partitions
  * Zarr: done via dataset configuration
* Metadata:
  * Parquet: Metadata is created as a sidecar _metadata.parquet file
* Unittesting of module: Very close to integration testing, local cluster is used to create cloud optimised files


# Installation
## Users
Requirements:
* python >= 3.10.14

### automatic installation of latest wheel release
```bash
curl -s https://raw.githubusercontent.com/aodn/aodn_cloud_optimised/main/install.sh | bash
```

Otherwise, go to the [release](http://github.com/aodn/aodn_cloud_optimised/releases/latest) page.

## Development

### Option 1: Install with Mamba/Conda
Requirements:
* Mamba from [miniforge3](https://github.com/conda-forge/miniforge)

```bash
mamba env create --file=environment.yml
mamba activate CloudOptimisedParquet

poetry install --with dev
```

### Option 2: Create a virtual environment of your choice

Create a virtual environment of your choice and activate it
```bash
pip install poetry
poetry install --with dev
#pip install -r requirements.txt  #
#pre-commit install  # should be done by poetry install
```

### Update dependencies

1. Update manually the pyproject.toml file with the required package versions
2. run
```bash
poetry update
```
to update and commit the changes to ```poetry.lock```

To update the ```requirements.txt```, run
```bash
poetry export -f requirements.txt --without-hashes -o requirements.txt
```

# Requirements
AWS SSO to push files to S3


# Usage

## As a standalone bash script
```bash
generic_cloud_optimised_creation -h
usage: generic_cloud_optimised_creation [-h] --paths PATHS [PATHS ...] [--filters [FILTERS ...]] [--suffix SUFFIX] --dataset-config DATASET_CONFIG
                                        [--clear-existing-data] [--force-previous-parquet-deletion] [--cluster-mode {local,remote}]
                                        [--optimised-bucket-name OPTIMISED_BUCKET_NAME] [--root_prefix-cloud-optimised-path ROOT_PREFIX_CLOUD_OPTIMISED_PATH]
                                        [--bucket-raw BUCKET_RAW]

Process S3 paths and create cloud-optimized datasets.

options:
  -h, --help            show this help message and exit
  --paths PATHS [PATHS ...]
                        List of S3 paths to process. Example: 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA'
  --filters [FILTERS ...]
                        Optional filter strings to apply on the S3 paths. Example: '_hourly-timeseries_' 'FV02'
  --suffix SUFFIX       Optional suffix used by s3_ls to filter S3 objects. Default is .nc. Example: '.nc'
  --dataset-config DATASET_CONFIG
                        Path to the dataset config JSON file. Example: 'mooring_hourly_timeseries_delayed_qc.json'
  --clear-existing-data
                        Flag to clear existing data. Default is False.
  --force-previous-parquet-deletion
                        Flag to force the search of previous equivalent parquet file created. Much slower. Default is False.Only for Parquet processing.
  --cluster-mode {local,remote}
                        Cluster mode to use. Options: 'local' or 'remote'. Default is 'local'.
  --optimised-bucket-name OPTIMISED_BUCKET_NAME
                        Bucket name where cloud optimised object will be created. Default is 'imos-data-lab-optimised'
  --root-prefix-cloud-optimised-path ROOT_PREFIX_CLOUD_OPTIMISED_PATH
                        Prefix value for the root location of the cloud optimised objects, such as s3://optimised-bucket-name/root-prefix-cloud-optimised-path/... Default is 'cloud_optimised/cluster_testing'
  --bucket-raw BUCKET_RAW
                        Bucket name where input object files will be searched for. Default is 'imos-data'

Examples:
  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA' --filters '_hourly-timeseries_' 'FV02' --dataset-config 'mooring_hourly_timeseries_delayed_qc.json' --clear-existing-data --cluster-mode 'remote'
  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/QLD' --dataset-config 'anmn_ctd_ts_fv01.json'
  generic_cloud_optimised_creation --paths 'IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024' --dataset-config 'radar_TurquoiseCoast_velocity_hourly_average_delayed_qc.json' --clear-existing-data --cluster-mode 'remote'
```

## As a python module

```python
from importlib.resources import files

from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.config import (
    load_variable_from_config,
    load_dataset_config,
)
from aodn_cloud_optimised.lib.s3Tools import s3_ls


def main():
    BUCKET_RAW_DEFAULT = load_variable_from_config("BUCKET_RAW_DEFAULT")
    nc_obj_ls = s3_ls(BUCKET_RAW_DEFAULT, "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2024")

    dataset_config = load_dataset_config(
        str(files("aodn_cloud_optimised").joinpath("config").joinpath("dataset").joinpath("satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.json")
            )
        )

    cloud_optimised_creation(
       nc_obj_ls,
       dataset_config=dataset_config,
       # clear_existing_data=True,  # this will delete existing data, be cautious! If testing change the paths below
       # optimised_bucket_name="imos-data-lab-optimised",  # optional, default value in config/common.json
       # root_prefix_cloud_optimised_path="cloud_optimised/cluster_testing",  # optional, default value in config/common.json
       cluster_mode='remote'
    )


if __name__ == "__main__":
    main()
```


# Parquet GenericHandler - handler steps
The conversion process in GenericHandler is broken down into a series of ordered steps, each responsible for a specific task. These steps include:

1. **delete_existing_matching_parquet**: Deletes existing Parquet files that match the current processing criteria.

2. **preprocess_data**: Generates a DataFrame and Dataset from the input NetCDF file.

3. **publish_cloud_optimised**: Creates Parquet files containing the processed data.
   - **_add_timestamp_df**: Adds timestamp information to the DataFrame. Useful for partitioning.
   - **_add_columns_df**: Adds generic columns such as site_code and filename to the DataFrame.
   - **_add_columns_df_custom** Adds custom columns (useful for specific handlers)
   - **_rm_bad_timestamp_df**: Removes rows with bad timestamps from the DataFrame.
   - **_add_metadata_sidecar**: Adds metadata from the PyArrow table to the xarray dataset as sidecar attributes.

4. **postprocess**: Cleans up resources used during data processing.



# Dataset configuration

Every dataset should be configured with a config JSON file. A template exists at ```aodn_cloud_optimised/config/dataset/dataset_template.json```

See [documentation](README_add_new_dataset.md) to learn how to add a new dataset


# Notebooks

Notebooks exist under
https://github.com/aodn/aodn_cloud_optimised/blob/main/notebooks/

For each new dataset, it is a good practice to use the provided template ```notebooks/template.ipynb```
and create a new notebook.

These notebooks use a common library of python functions to help with creating the geo-spatial filters:
```notebooks/parquet_queries.py```
