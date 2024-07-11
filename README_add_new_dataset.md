# Creating a dataset configuration file


This module aims to be generic enough so that adding a new IMOS dataset is driven through a json config file.
Examples of dataset configuration can be found at [config](https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/config/dataset).

For more complicated dataset, such as Argo for example, it's also possible to create a specific handler which would
inherit with ```Super()``` all of the methods for the ```aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler``` class.

The main choice left to create a cloud optimised dataset with this module is to decide to either use the **Apache Parquet**
format vs the **Zarr** format.

As a rule of thumbs, for:
* **tabular** dataset (NetCDF, CSV) -> **Parquet**
* **gridded** dataset (NetCDF) -> **Zarr**

## Creating a Parquet dataset for NetCDF
As an example, we'll explain the ```aodn_cloud_optimised.config.slocum_glider_delayed_qc.json``` config file.

### The Basics
The first sections to add are

```json
{
  "dataset_name": "slocum_glider_delayed_qc",
  "logger_name": "slocum_glider_delayed_qc",
  "cloud_optimised_format": "parquet",
  "metadata_uuid": "a681fdba-c6d9-44ab-90b9-113b0ed03536",
  ...
}
```
* dataset_name: the name as it will appear on AWS S3 storage
* cloud_optimised_format key is important as this will allow the code to either choose the Parquet handler or the zarr handler
* metadata_uuid: the Geonetwork uuid metadata record. This value will be written in the parquet sidecar file

### Creating the Schema
While developing the aodn_cloud_optimised, it became clear that for both zarr and parquet format, consistency of the dataset was key.

In this section, we're demonstrating how to create the full schema from a NetCDF file as an example, so that each variable
is defined, with its variable attributes and the type.

The following snippet creates the required schema from a random NetCDF. ```generate_json_schema_from_s3_netcdf``` will output the schema into a json file in a temporary file.

```python
import os
from aodn_cloud_optimised.lib.config import load_variable_from_config
from aodn_cloud_optimised.lib.schema import generate_json_schema_from_s3_netcdf

BUCKET_RAW_DEFAULT = load_variable_from_config('BUCKET_RAW_DEFAULT')
obj_key = 'IMOS/ANFOG/slocum_glider/AIMS20151021/IMOS_ANFOG_BCEOPSTUVN_20151021T035731Z_SL416_FV01_timeseries_END-20151027T015319Z.nc'
nc_file = os.path.join('s3://', BUCKET_RAW_DEFAULT, obj_key)

generate_json_schema_from_s3_netcdf(nc_file)
```

The output will be such as:
```json
{
  "PLATFORM": {
    "type": "string",
    "trans_system_id": "Irridium",
    "positioning_system": "GPS",
    "platform_type": "Slocum G2",
    "platform_maker": "Teledyne Webb Research",
    "firmware_version_navigation": 7.1,
    "firmware_version_science": 7.1,
    "glider_serial_no": "416",
    "battery_type": "Alkaline",
    "glider_owner": "CSIRO",
    "operating_institution": "ANFOG",
    "long_name": "platform informations"
  },
  "DEPLOYMENT": {
    "type": "string",
    "deployment_start_date": "2015-10-21-T05:00:02Z",
    "deployment_start_latitude": -18.9373,
    "deployment_start_longitude": 146.881,
    "deployment_start_technician": "Gregor, Rob",
    "deployment_end_date": "2015-10-27-T01:56:23Z",
    "deployment_end_latitude": -19.2358,
    "deployment_end_longitude": 147.5188,
    "deployment_end_status": "recovered",
    "deployment_pilot": "pilot, CSIRO",
    "long_name": "deployment informations"
  },
  "SENSOR1": {
    "type": "string",
    "sensor_type": "CTD",
    "sensor_maker": "Seabird",
    "sensor_model": "GPCTD",
    "sensor_serial_no": "9117",
    "sensor_calibration_date": "2013-09-17",
    "sensor_parameters": "TEMP, CNDC, PRES, PSAL",
    "long_name": "sensor1 informations"
  },
```

Simply copy this into the ```schema``` key of the dataset config, so that:

```json
{
  "dataset_name": "slocum_glider_delayed_qc",
  "logger_name": "slocum_glider_delayed_qc",
  "cloud_optimised_format": "parquet",
  "metadata_uuid": "a681fdba-c6d9-44ab-90b9-113b0ed03536",
  "schema": {
  "PLATFORM": {
    "type": "string",
    "trans_system_id": "Irridium",
    "positioning_system": "GPS",
    "platform_type": "Slocum G2",
    "platform_maker": "Teledyne Webb Research",
    "firmware_version_navigation": 7.1,
    "firmware_version_science": 7.1,
    "glider_serial_no": "416",
    "battery_type": "Alkaline",
    "glider_owner": "CSIRO",
    "operating_institution": "ANFOG",
    "long_name": "platform informations"
  },
  "DEPLOYMENT": {
    "type": "string",
    "deployment_start_date": "2015-10-21-T05:00:02Z",
    "deployment_start_latitude": -18.9373,
    "deployment_start_longitude": 146.881,
    "deployment_start_technician": "Gregor, Rob",
    "deployment_end_date": "2015-10-27-T01:56:23Z",
    "deployment_end_latitude": -19.2358,
    "deployment_end_longitude": 147.5188,
    "deployment_end_status": "recovered",
    "deployment_pilot": "pilot, CSIRO",
    "long_name": "deployment informations"
  },
  "SENSOR1": {
    "type": "string",
    "sensor_type": "CTD",
    "sensor_maker": "Seabird",
    "sensor_model": "GPCTD",
    "sensor_serial_no": "9117",
    "sensor_calibration_date": "2013-09-17",
    "sensor_parameters": "TEMP, CNDC, PRES, PSAL",
    "long_name": "sensor1 informations"
  },
 ...
```

#### **Note**:
The chosen NetCDF may not contain all of the variables that will exist in the dataset. In order to add them, while creating
the parquet dataset, the logs will output the json info to be added into the config for each missing variable.

### Global attributes as variables
Some NetCDF global attributes may have to be converted into variables so that users/API can filter the data based on these
values.

In the following example, ```deployment_code``` is a global attribute that we want to have as a variable. It is then added
in the ```gattrs_to_variables```. **However**, this needs to also be present in the schema definition
so that:

```json
...
  "gattrs_to_variables": [
    "deployment_code"
  ],
 "schema": {
...
    "deployment_code": {
      "type": "string"
    }
 }
```

### Filename as variable
The IMOS/AODN data (re)processing is very file oriented. In order to reprocess data and delete the old matching data,
the original filename is stored as a variable. It is required to add it in the schema definition:

```json
 "schema": {
...
    "filename": {
      "type": "string"
    },
...
```

### Choosing the Partition keys
Any variable available in the schema definition could be used as a partition.

#### Timestamp partition
To add efficient time filtering, a timestamp variable is created.
```partition_timestamp_period``` is the period to choose (`M` for month, `Y` for year, `Q` for quarterly, etc.).

The following information needs to be added in the relevant sections:
```json
  "partition_keys": [
    "timestamp",
    ...
  ],
  "time_extent": {
    "time": "TIME",
    "partition_timestamp_period": "Q"
  },
  "schema":
    ...
    "timestamp": {
      "type": "int64"
    },
    ...
```


#### Geospatial Partition
To add efficient geospatial filtering, a polygon variable is created.

The following information needs to be added in the relevant sections:

```json
  "partition_keys": [
     ...
    "polygon"
  ],
  "spatial_extent": {
    "lat": "LATITUDE",
    "lon": "LONGITUDE",
    "spatial_resolution": 5
  },
  "schema":
    ...
    "polygon": {
      "type": "string"
    },
    ...

```

### Global Attributes

To add common global attributes to the metadata parquet sidecar, add:

```json
  "dataset_gattrs": {
    "title": "ANFOG glider"
  },
```

### Force search and deletion of previous parquet files
Force search for existing parquet files to delete when creating new ones. This can end up being really slow if there are a lot of objets (for example Argo)
```json
  "force_previous_parquet_deletion": true
```





## Creating a Parquet dataset for CSV
To create a parquet dataset from CSV files, all of the above is relevant. However, there are some special config to deal
with various CSV formats.

As an example, we will use the  ```aodn_cloud_optimised.config.aatams_acoustic_tagging.json``` config file.

The config is based on the ```pandas.read_csv``` documentation. Below is only a short list of optional arguments. Any options
from the ```pandas.read_csv``` could be used

```json
  "pandas_read_csv_config": {
    "delimiter": ";",
    "header": 0,
    "index_col": "detection_timestamp",
    "parse_dates": [
      "detection_timestamp"
    ],
    "na_values": [
      "N/A",
      "NaN"
    ],
    "encoding": "utf-8"
  },
```

## Creating a Zarr dataset

As an example, we'll explain the ```aodn_cloud_optimised.config.radar_velocity_hourly_average_delayed_qc_main.json``` config file.
Please note that it is possible to have a main config and a child config to avoid duplication. This is especially useful for IMOS Zarr dataset
such as the ones from the Radar and GHRSST dataset which are similar in terms of metadata.

See for example the two configurations files which are related:
https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/config/dataset/radar_velocity_hourly_average_delayed_qc_main.json
https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/config/dataset/radar_TurquoiseCoast_velocity_hourly_average_delayed_qc.json

### The Basics
The first section to add is

```json
{
  "dataset_name": "radar_velocity_hourly_average_delayed_qc_main",
  "logger_name": "radar_velocity_hourly_average_delayed_qc_main",
  "cloud_optimised_format": "zarr",
  "metadata_uuid": "a681fdba-c6d9-44ab-90b9-113b0ed03536",
  ...
}
```
* dataset_name: the name as it will appear on AWS S3 storage
* cloud_optimised_format key is important as this will allow the code to either choose the Parquet handler or the zarr handler
* metadata_uuid: the GeoNetwork uuid metadata record. This value will be written in the parquet sidecar file

### The chunks

```json
    "dimensions": {
        "time": {"name": "TIME",
                 "chunk": 1500,
                 "rechunk": true},
        "latitude": {"name": "J",
                     "chunk": 60},
        "longitude": {"name": "I",
                      "chunk": 59}
    },
```
### Variable Template

The name of a variable which will be used as a template to create missing variables from the dataset and have similar shape

```json
    "var_template_shape": "UCUR",

```

### Variables to drop
when setting `region` explicitly in to_zarr() method, all variables in the dataset to write must have at least one
dimension in common with the region's dimensions ['TIME'].
We need to remove the variables from the dataset which fall into this condition:
```json
    "vars_to_drop_no_common_dimension": ["I", "J", "LATITUDE", "LONGITUDE", "GDOP"],

```

Also, when a dataset to be converted to ZARR has some variables which aren't always in the dataset,
it is at the moment (July 2024) good practice to drop them:
```json
    "wind_speed_dtime_from_sst": {
      "type": "float32",
      "drop_vars": true
    },
```

Ideally we should create an empty variable. This is implemented in the zarr handler with the preprocess function to be used
by xarray_open_mfdataset. However, this function is currently not used, as it leads to some issues when running in a cluster.

After multiple testing of this preprocess function, which is very hard to serialise, it needs to be outside of the handler class.
But even outised of the class, even with an empty preprocess function, calling it with mfdataset will send the data back
to the machine creating the coiled cluster!! Insane behaviour, but it happens. So far, the hack has been to call
the preprocess function after mfdataset and keeping it "simple".


### Creating the Schema

See same section above. As for parquet

### AWS OpenData registry
See same section above. As for parquet


## Cluster options
In order to create the dataset on a remote cluster (Coiled), the following configuration needs to be added:
```json
  "cluster_options" : {
    "n_workers": [2, 20],
    "scheduler_vm_types": "t3.small",
    "worker_vm_types": "t3.large",
    "allow_ingress_from": "me",
    "compute_purchase_option": "spot_with_fallback",
    "worker_options": {
      "nthreads": 8,
      "memory_limit": "16GB" }
  },
  "batch_size": 1000,

```

See [coiled documentation](https://docs.coiled.io/user_guide/clusters/index.html)

Every dataset is different, and so will be the configuration above. The values of the ```batch_size```,
```number of n_workers```, ```scheduler_vm_types``` and  ```worker_vm_types``` are all intertwined.

It is necessary to understand the dataset, how big are the input files, and to run some tests on the coiled cluster to
find the best cluster options to process input files as quickly and cheaply as possible.

Too big of a ```batch_size``` with a too small of a ```worker_vm_types``` will lead to out of memory issues, and higher Global Interpreter Lock (GIL)



## AWS OpenData registry
In order to publicise the dataset on the OpenData Registry, the following needs to be added to every dataset configuration file.
Once populated, the registry files needed by AWS can be created by the script below,
and then added to the AWS OpenData Github repository https://github.com/awslabs/open-data-registry


```json
  "aws_opendata_registry": {
    "Name": "",
    "Description": "",
    "Documentation": "",
    "Contact": "",
    "ManagedBy": "",
    "UpdateFrequency": "",
    "Tags": [],
    "License": "",
    "Resources": [
      {
        "Description": "",
        "ARN": "",
        "Region": "",
        "Type": "",
        "Explore": []
      },
      {
        "Description": "",
        "ARN": "",
        "Region": "",
        "Type": ""
      },
      {
        "Description": "",
        "ARN": "",
        "Region": "",
        "Type": ""
      },
      {
        "Description": "",
        "ARN": "",
        "Region": "",
        "Type": ""
      }
    ],
    "DataAtWork": {
      "Tutorials": [
        {
          "Title": "",
          "URL": "",
          "Services": "",
          "AuthorName": "",
          "AuthorURL": ""
        },
        {
          "Title": "",
          "URL": "",
          "AuthorName": "",
          "AuthorURL": ""
        },
        {
          "Title": "",
          "URL": "",
          "AuthorName": "",
          "AuthorURL": ""
        }
      ],
      "Tools & Applications": [
        {
          "Title": "",
          "URL": "",
          "AuthorName": "",
          "AuthorURL": ""
        },
        {
          "Title": "",
          "URL": "",
          "AuthorName": "",
          "AuthorURL": ""
        }
      ],
      "Publications": [
        {
          "Title": "",
          "URL": "",
          "AuthorName": ""
        },
        {
          "Title": "",
          "URL": "",
          "AuthorName": ""
        }
      ]
    }
  }
}

```

A script exists to facilitate the creation of all registry entries:
```shell
cloud_optimised_create_aws_registry_dataset -h
usage: cloud_optimised_create_aws_registry_dataset [-h] [-f FILE] [-d DIRECTORY] [-a]

        Create AWS OpenData Registry YAML files from the dataset configuration, ready to be added to the OpenData Github
        repository.
        The script can be run in three ways:
            1. Convert a specific JSON file to YAML using '-f' or '--file'.
            2. Convert all JSON files in the directory using '-a' or '--all'.
            3. Run interactively to list all available JSON files and prompt
               the user to choose one to convert.


options:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  Name of a specific JSON file to convert.
  -d DIRECTORY, --directory DIRECTORY
                        Output directory to save converted YAML files.
  -a, --all             Convert all JSON files in the directory.
```
