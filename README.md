# AODN Cloud Optimised Conversion

A tool to convert IMOS NetCDF files and CSV into Cloud Optimised format (Zarr/Parquet)



# Installation
## Users
Requirements:
* python >= 3.10.14

```bash
curl -s https://raw.githubusercontent.com/aodn/aodn_cloud_optimised/main/install.sh | bash
```

## Development
Requirements:
* Mamba from miniforge3: https://github.com/conda-forge/miniforge

```bash
mamba env create --file=environment.yml
mamba activate CloudOptimisedParquet

poetry install
```
# Requirements
AWS SSO to push files to S3

# Features List

## Parquet Features
| Feature                                                                                        | Status | Comment                                                                            |
|------------------------------------------------------------------------------------------------|--------|------------------------------------------------------------------------------------|
| Process IMOS tabular NetCDF to Parquet with GenericHandler                                     | Done   | Converts NetCDF files to Parquet format using a generic handler.                   |
| Process CSV to Parquet with GenericHandler                                                     | Done   | Converts CSV files to Parquet format using a generic handler.                      |
| Specific Handlers inherit all methods from GenericHandler with super()                         | Done   | Simplifies the creation of new handlers by inheriting methods.                     |
| Unittests implemented                                                                          | Done   | Tests to ensure functionality and reliability.                                     |
| Reprocessing of files already converted to Parquet                                             | Done   | Reprocessing of NetCDF files; original method can be slow for large datasets.      |
| Metadata variable attributes in sidecar parquet file                                           | Done   | Metadata attributes available in dataset sidecars.                                 |
| Add new variables to dataset                                                                   | Done   | Addition of new variables such as site_code, deployment_code, filename attributes. |
| Add timestamp variable for partition key                                                       | Done   | Enhances query performance by adding a timestamp variable.                         |
| Remove NaN timestamp when NetCDF not CF compliant                                              | Done   | Eliminates NaN timestamps, particularly for non CF compliant data like Argo.       |
| Create dataset Schema                                                                          | Done   | Creation of a schema for the dataset.                                              |
| Create missing variables available in Schema                                                   | Done   | Ensures dataset consistency by adding missing variables from the schema.           |
| Warning when new variable from NetCDF is missing from Schema                                   | Done   | Alerts when a new variable from NetCDF is absent in the schema.                    |
| Creating metadata parquet sidecar                                                              | Done   |                                                                                    |
| Create AWS OpenData Registry Yaml                                                              | Done   |
| Config file JSON validation against schema                                                     | Done   |
| Create polygon variable to facilite geometry queries | Done   |

## Zarr Features
| Feature                                                                | Status | Comment                                                                            |
|------------------------------------------------------------------------|--------|------------------------------------------------------------------------------------|
| Process IMOS Gridded NetCDF to Zarr with GenericHandler                | Done   | Converts NetCDF files to Parquet format using a generic handler.                   |
| Specific Handlers inherit all methods from GenericHandler with super() | Done   | Simplifies the creation of new handlers by inheriting methods.                     |



# Usage

## Parquet
The GenericHandler for parquet dataset creation is designed to be used either as a standalone class or as a base class for more specialised handler implementations. Here's a basic usage example:

```python
# Read the content of the dataset template JSON file (with comments)
#import commentjson
#with open('aodn_cloud_optimised/config/dataset/dataset_template.json', 'r') as file:
#   json_with_comments = file.read()
#dataset_config = commentjson.loads(json_with_comments)

import importlib.resources
from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation

dataset_config = load_dataset_config(str(importlib.resources.path("aodn_cloud_optimised.config.dataset", "anfog_slocum_glider.json")))

cloud_optimised_creation('object/path/netcdf_file.nc',
                          dataset_config=dataset_config
                         )
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
