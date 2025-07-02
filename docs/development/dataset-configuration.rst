.. _dataset-config-doc:


Dataset Configuration
=====================
Every dataset should be configured with a JSON file. A template exists at `config <https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/config/dataset/dataset_template.json>`__.

This module aims to be generic enough so that adding a new IMOS dataset
should be driven only through a json configuration file.

Examples of dataset configuration can be found at:

- `config <https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/config/dataset>`__.


The main choice left to create a cloud optimised dataset with this
module is to decide either to use the **Apache Parquet** format or the
**Zarr** format.

As a rule of thumb, for:

- *tabular* dataset (NetCDF, CSV) -> **Parquet**
- *gridded* dataset (NetCDF) -> **Zarr**


All dataset configuration files are placed under: https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/config/dataset/

Create Dataset Configuration (semi-automatic)
---------------------------------------------
See section :ref:`dataset-config-script` to help creating a dataset configuration.



Parquet Configuration from NetCDF file
--------------------------------------

.. note:: Important Note
   :class: custom-note
   :name: non-generic-handler

    Some dataset format are more complicated and can't use the ``GenericParquetHandler``.

    This is the case with Argo data for example. In this case, it is possible to create a specific handler which would inherit with
    ``Super()`` all of the methods from the ``aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler`` class.



As an example, we’ll explain the
``aodn_cloud_optimised.config.slocum_glider_delayed_qc.json`` config
file.

https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/config/dataset/slocum_glider_delayed_qc.json


The Basics
~~~~~~~~~~

The first sections to add are

.. code:: json

   {
     "dataset_name": "slocum_glider_delayed_qc",
     "logger_name": "slocum_glider_delayed_qc",
     "cloud_optimised_format": "parquet",
     "metadata_uuid": "a681fdba-c6d9-44ab-90b9-113b0ed03536",
     ...
   }

-  dataset_name: the dataset name as it will appear on AWS S3 storage (minus its format, ie: `slocum_glider_delayed_qc.parquet`)
-  cloud_optimised_format key is important as this will allow the code
   to either choose the Parquet handler or the zarr handler
-  metadata_uuid: the Geonetwork uuid metadata record. This value will
   be written in the parquet sidecar file

.. _creating_the_schema:

Creating the Schema
~~~~~~~~~~~~~~~~~~~

For both zarr and parquet format, consistency of the dataset is essential.

In this section, we’re demonstrating how to create the full schema definition from an input NetCDF file.
Each variable will be defined, with its respective variable attributes the type.


.. note:: Important Note
   :class: custom-note
   :name: var-attributes

    Currently, when processing a new NetCDF which has different attributes values (for example "Degrees" vs "degree"),
    a warning/error message will be displayed without consequences.

    On a previous version, files were not processed, but this lead to too many files failing



The following snippet creates the required schema from a random NetCDF.
``generate_json_schema_from_s3_netcdf`` will output the schema into a
json file in a temporary file.

.. code:: python

   import os
   from aodn_cloud_optimised.lib.config import load_variable_from_config
   from aodn_cloud_optimised.lib.schema import generate_json_schema_from_s3_netcdf

   BUCKET_RAW_DEFAULT = load_variable_from_config('BUCKET_RAW_DEFAULT')
   obj_key = 'IMOS/ANFOG/slocum_glider/AIMS20151021/IMOS_ANFOG_BCEOPSTUVN_20151021T035731Z_SL416_FV01_timeseries_END-20151027T015319Z.nc'
   nc_file = os.path.join('s3://', BUCKET_RAW_DEFAULT, obj_key)

   generate_json_schema_from_s3_netcdf(nc_file)

The output will be such as:

.. code:: json

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

Simply copy this into the ``schema`` key of the dataset config, so that we have:

.. code:: json

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


.. note:: Important Note
   :class: custom-note
   :name: my-note

    The chosen NetCDF may not contain all of the variables that will exist
    in the dataset.

    In order to add them, it is advised to create a first pass of the dataset. The module will log the json info to be
    added into the config for each missing variable, which can simply be paste.

Global attributes as variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some NetCDF global attributes may have to be converted into variables so
that users/API can filter the data based on these values.

In the following example, ``deployment_code`` is a global attribute that
we want to have as a variable. It is then added in the
``gattrs_to_variables``. **However**, this needs to also be present in
the schema definition:

.. code:: json

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

Object key path as variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section explains the configuration for extracting information from object keys when some information are missing from the
NetCDF files but available from the filepath.

The following JSON snippet illustrates how to specify the `object_key_info` in your configuration:

.. code:: json

   ...
      "object_key_info": {
        "key_pattern": ".*/IMOS/{campaign_name}/{dive_name}/hydro_netcdf/{filename}",
        "extraction_code": "def extract_info_from_key(key):\n    parts = key.split('/')\n    return {'campaign_name': parts[-4], 'dive_name': parts[-3]}"
        },

In this example, `campaign_name` and `dive_name` are extracted from the path. The `extraction_code` returns a dictionary of variables.

After extraction, `campaign_name` and `dive_name` should be added as string variables, and they can also be included in the `partition_keys`.

The following snippet shows how to define these keys in your JSON configuration:

.. code:: json

   ...
     "partition_keys": [
       "campaign_name"
     ],
    "schema": {
   ...
       "campaign_name": {
         "type": "string"
       }
    }


The ``extract_info_from_key`` function should always be structured as follows:

.. code:: python

    def extract_info_from_key(key):
        """
        Extract information from a key string while ensuring a consistent return structure.

        Args:
            key (str): The input string containing information formatted with slashes.

        Returns:
            dict: A dictionary with the extracted information, including predefined keys
                  with default values if information is not available.
        """
        parts = key.split('/')

        return {
            'campaign_name': parts[-4] if len(parts) > 3 else None,
            'dive_name': parts[-3] if len(parts) > 2 else None,
            'project_id': parts[-2] if len(parts) > 1 else None,
            'file_type': parts[-1] if len(parts) > 0 else None,
            'timestamp': None  # This could be added or derived if applicable
        }


Filename as variable
~~~~~~~~~~~~~~~~~~~~

The IMOS/AODN data (re)processing is very file oriented. In order to
reprocess data and delete the old matching data, the original filename
is stored as a variable. It is required to add it in the schema
definition:

.. code:: json

    "schema": {
   ...
       "filename": {
         "type": "string"
       },
   ...

Choosing the Partition keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any variable available in the schema definition could be used as a
partition.

Timestamp partition
^^^^^^^^^^^^^^^^^^^

To add efficient time filtering, a timestamp variable is created.
``partition_timestamp_period`` is the period to choose (``M`` for month,
``Y`` for year, ``Q`` for quarterly, etc.). Refer to https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-period-aliases

.. note:: Important Note
   :class: custom-note
   :name: timestamp-partition

    Choose the period wisely though testing! A finer period, such as day, will create a lot more objects or chunks
    and will slow considerably data queries

The following information needs to be added in the relevant sections:

.. code:: json

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

Geospatial Partition
^^^^^^^^^^^^^^^^^^^^

To add efficient geospatial filtering, a polygon variable is created.

The following information needs to be added in the relevant sections:

.. code:: json

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

.. note:: Important Note
   :class: custom-note
   :name: polygon-partition

    Choose the spatial_resolution wisely though testing! Similarly to the ``partition_timestamp_period`` above, a smaller
    value will lead to more objects.

Global Attributes
~~~~~~~~~~~~~~~~~

To add common global attributes to the metadata parquet sidecar, add:

.. code:: json

     "dataset_gattrs": {
       "title": "ANFOG glider"
     },

Force search and deletion of previous parquet files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Force search for existing parquet files to delete when creating new
ones. This can end up being really slow if there are a lot of objets
(for example Argo)

.. code:: json

     "force_previous_parquet_deletion": true

Parquet Configuration from CSV file
-----------------------------------

To create a parquet dataset from CSV files, all of the above is
relevant. However, there are some special config to deal with various
CSV formats.

As an example, we will use the
``aodn_cloud_optimised.config.aatams_acoustic_tagging.json`` config
file.

The config is based on the ``pandas.read_csv`` documentation. Below is
only a short list of optional arguments. Any options from the
``pandas.read_csv`` could be used

.. code:: json

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

See the official pandas documentation:
`pandas.read_csv <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`_.


Zarr Configuration from NetCDF
------------------------------

As an example, we’ll explain the
``aodn_cloud_optimised.config.radar_velocity_hourly_averaged_delayed_qc_main.json``
config file.
https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/config/dataset/radar_velocity_hourly_averaged_delayed_qc_main.json

.. note:: Important Note
   :class: custom-note
   :name: child-parent-config

   It is possible to have a main config and a child config to avoid duplication. This is especially useful for IMOS Zarr datasets such as the ones from the Radar and GHRSST datasets, which are similar in terms of metadata.

   See for example the two related configuration files:
   `radar_velocity_hourly_averaged_delayed_qc_main.json <https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/config/dataset/radar_velocity_hourly_averaged_delayed_qc_main.json>`__
   `radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json <https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/config/dataset/radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json>`__

.. _the-basics-1:

The Basics
~~~~~~~~~~

The first section to add is

.. code:: json

   {
     "dataset_name": "radar_velocity_hourly_averaged_delayed_qc_main",
     "logger_name": "radar_velocity_hourly_averaged_delayed_qc_main",
     "cloud_optimised_format": "zarr",
     "metadata_uuid": "a681fdba-c6d9-44ab-90b9-113b0ed03536",
     ...
   }

-  dataset_name: the name as it will appear on AWS S3 storage
-  cloud_optimised_format key is important as this will allow the code
   to either choose the Parquet handler or the zarr handler
-  metadata_uuid: the GeoNetwork uuid metadata record. This value will
   be written in the parquet sidecar file

The chunks
~~~~~~~~~~

.. code:: json

       "dimensions": {
           "time": {"name": "TIME",
                    "chunk": 1500,
                    "rechunk": true,
                    "append_dim": true},
           "latitude": {"name": "J",
                        "chunk": 60},
           "longitude": {"name": "I",
                         "chunk": 59}
       },

Variable Template
~~~~~~~~~~~~~~~~~

The name of a variable which will be used as a template to create
missing variables from the dataset and have similar shape

.. code:: json

       "var_template_shape": "UCUR",

Variables to drop
~~~~~~~~~~~~~~~~~

when setting ``region`` explicitly in to_zarr() method, all variables in
the dataset to write must have at least one dimension in common with the
region’s dimensions [‘TIME’]. We need to remove the variables from the
dataset which fall into this condition:

.. code:: json

       "vars_incompatible_with_region": ["I", "J", "LATITUDE", "LONGITUDE", "GDOP"],

Also, when a dataset to be converted to ZARR has some variables which
aren’t always in the dataset, it is at the moment (July 2024) good
practice to drop them:

.. code:: json

       "wind_speed_dtime_from_sst": {
         "type": "float32",
         "drop_var": true
       },

.. note:: Important Note
   :class: custom-note
   :name: zarr-preprocess

    Ideally, when a variable is missing from an input NetCDF file but exists in the schema definition, an empty variable
    should be created. This functionality is implemented in the Zarr handler through the ``preprocess`` function. The
    intended usage is for ``xarray.open_mfdataset`` to utilise this ``preprocess`` function. However, due to issues
    encountered when running on a remote cluster, this function is currently not in use.

    After extensive testing of the ``preprocess`` function, which is challenging to serialise, it was found necessary to
    move it outside the handler class. But even with an empty ``preprocess`` function simply returning the input dataframe,
    and defined outside the class, once called with ``xarray.open_mfdataset``, data being sent back to the machine
    creating the Coiled cluster for some processing.

    This behavior has been managed by simplifying the `preprocess` function and calling it post ``mfdataset`` call.

    However, we're currently dropping **ALL** variables which aren't present across all the NetCDF files. This is done by
    adding ``"drop_var": true`` in the schema definition for any variable to drop. In the future, this should be fixed!


Creating the Schema
~~~~~~~~~~~~~~~~~~~

See :ref:`creating_the_schema` section above. As for Parquet...


Global Attributes to drop
~~~~~~~~~~~~~~~~~

.. code:: json

  "gattrs_to_delete": [
    "Voyage_number",
    "platform_code",
    "geospatial_lat_max",
    "geospatial_lat_min",
    "geospatial_lon_max",
    "geospatial_lon_min",
    "date_created"
  ],

Global Attributes to variables
~~~~~~~~~~~~~~~~~

.. code:: json

   "gattrs_to_variable": {
      "file_version": {
        "destination_name": "quality_control_version",
        "dimensions": "TIME",
        "length": 49
      },
      "platform_code": {
        "destination_name": "platform_code",
        "dimensions": "TIME",
        "length": 7
      },
      "voyage_number": {
        "destination_name": "voyage_number",
        "dimensions": "TIME",
        "length": 10
      }
   },



Run Settings Options
---------------

Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: json

    "run_settings": {
      "coiled_cluster_options": {
        "n_workers": [
          40,
          50
        ],
        "scheduler_vm_types": "m7i.large",
        "worker_vm_types": "m7i.large",
        "allow_ingress_from": "me",
        "compute_purchase_option": "spot_with_fallback",
        "worker_options": {
          "nthreads": 8,
          "memory_limit": "16GB"
        }
      },
      "batch_size": 60,
      "cluster": {
        "mode": "coiled",
        "restart_every_path": false
      },
      "paths": [
        {
          "s3_uri": "s3://imos-data/IMOS/AATAMS/satellite_tagging/MEOP_QC_CTD/",
          "filter": [
            ".*\\.nc$"
          ],
          "year_range": []
        }
      ],
      "clear_existing_data": true,
      "raise_error": false,
      "force_previous_parquet_deletion": true
    }


.. note:: Important Note
   :class: custom-note
   :name: cluster-config

    * If cluster.mode is set to "coiled", the coiled_cluster_options need to be set.
    * If cluster.mode is set to "ec2", the ec2_cluster_options and ec2_adapt_options need to be set.
    * cluster.mode can be also set to "local" or null



In order to create the dataset on a remote cluster (ec2/coiled), the
following configuration needs to be added within the run_settings:

Coiled Cluster configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For a coiled cluster, simply put this in the ``run_settings`` config

.. code:: json

     "coiled_cluster_options" : {
       "n_workers": [2, 20],
       "scheduler_vm_types": "m7i-flex.large",
       "worker_vm_types": "m7i-flex.large",
       "allow_ingress_from": "me",
       "compute_purchase_option": "spot_with_fallback",
       "worker_options": {
         "nthreads": 8,
         "memory_limit": "16GB" }
     },
     "cluster": {
      "mode": "coiled",
      "restart_every_path": false
    },

See `coiled documentation <https://docs.coiled.io/user_guide/clusters/index.html>`__

.. note:: Important Note
   :class: custom-note
   :name: coiled-config

    Every dataset is different, and so will be the configuration above. The
    values of the ``batch_size``, ``number of n_workers``,
    ``scheduler_vm_types`` and ``worker_vm_types`` are all intertwined.

    It is necessary to understand the dataset, how big are the input files.

    It is advised run some tests on the coiled cluster and look at the graph outputs to find the best cluster
    configuration to process input files as quickly and cheaply as possible.

    Too big of a ``batch_size`` with a too small of a ``worker_vm_types``
    will lead to out of memory issues, and higher Global Interpreter Lock
    (GIL)


EC2 Cluster configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
As for above, in the EC2 cluster is to be chosen, simply put this in the ``run_settings`` config

.. code:: json

  "ec2_cluster_options": {
    "n_workers": 1,
    "scheduler_instance_type": "m7i-flex.xlarge",
    "worker_instance_type": "m7i-flex.2xlarge",
    "security": false,
    "docker_image": "ghcr.io/aodn/aodn_cloud_optimised:latest"
  },
  "ec2_adapt_options": {
    "minimum": 1,
    "maximum": 120
  },
  "cluster": {
      "mode": "ec2",
      "restart_every_path": false
    },

.. _aws-opendata-registry-1:

AWS OpenData registry
---------------------

In order to publicise the dataset on the OpenData Registry, the
following needs to be added to every dataset configuration file.

Once populated, the registry files needed by AWS can be created by the script
below, and then added to the AWS OpenData Github repository:
`AWS Open Data Registry <https://github.com/awslabs/open-data-registry>`_.


.. code:: json

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

A script, automatically installed with the module, exists to facilitate the creation of all registry entries.

.. code:: shell

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

This script can be run in a few different ways:

- ``cloud_optimised_create_aws_registry_dataset`` -> will trigger an interactive mode
- ``cloud_optimised_create_aws_registry_dataset -a`` -> will output all dataset metadata
- ``cloud_optimised_create_aws_registry_dataset -f slocum_glider_delayed_qc.json`` -> for a specific dataset



Adding the dataset to pyproject.toml
====================================
TODO:
- Explain pyproject.toml
- individual scripts for full reprocessing

.. note:: Important Note
   :class: custom-note
   :name: non-generic-handler

    In order to test the new configuration, the newly created script needs to be installed in the environment.

    1) Activate the virtual env

    2) run    ```poetry install --with dev```

    3) re-activate the virtual env

    The new script will be available
