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
^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^

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

Parquet Schema Transformation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Adding Variables Dynamically
""""""""""""""""""""""""""""

You can define new variables to add to the dataset using the following `source` types:

**@filename** (required)
~~~~~~~~~~~~~~~~~~~~~~~~

Adds the original file name as a variable. This is required for traceability and to safely overwrite old data.

The IMOS/AODN processing is very file-oriented. To reprocess data and delete previously generated output, we need to keep track of the original filename. That’s why it must be included in the schema definition:

.. code-block:: json

   "filename": {
     "source": "@filename",
     "schema": {
       "type": "string",
       "units": "1",
       "long_name": "Filename of the source file"
     }
   }

**@partitioning** (required)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Generates time and space partitioning variables (`timestamp`, `polygon`) for optimised cloud access.

.. code-block:: json

   "timestamp": {
     "source": "@partitioning:time_extent",
     "schema": {
       "type": "int64",
       "units": "1",
       "long_name": "Partition timestamp"
     }
   },
   "polygon": {
     "source": "@partitioning:spatial_extent",
     "schema": {
       "type": "string",
       "units": "1",
       "long_name": "Spatial partition polygon"
     }
   }

The above requires a corresponding `partitioning` section:

.. code-block:: json

   "partitioning": [
     {
       "source_variable": "timestamp",
       "type": "time_extent",
       "time_extent": {
         "time_varname": "TIME",
         "partition_period": "M"
       }
     },
     {
       "source_variable": "polygon",
       "type": "spatial_extent",
       "spatial_extent": {
         "lat_varname": "LATITUDE",
         "lon_varname": "LONGITUDE",
         "spatial_resolution": 5
       }
     }
   ]


**@global_attribute:<name>**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Copies the value from a NetCDF global attribute into a new variable.

.. code-block:: json

   "vessel_name": {
     "source": "@global_attribute:vessel_name",
     "schema": {
       "type": "string",
       "units": "1",
       "_FillValue": "",
       "long_name": "vessel name"
     }
   }

**@variable_attribute:<varname>.<varatt>**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Extracting specific variable attributes and promoting them to variables.

**Example:**

.. code-block:: json

   "instrument_identifgier": {
     "source": "@variable_attribute:TEMP.instrument_id",
     "schema": {
       "type": "string",
       "units": "1",
       "_FillValue": "",
       "long_name": "my instrument id"
     }
   }

**@function:<function_name>** to Extract from File Paths
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Custom logic can be applied to the object key (e.g. S3 path) using the `@function:<name>` syntax in `add_variables`. These require a corresponding function definition in the `functions` block.

This is helpful when useful information are missing from the NetCDF files but available from the filepath.

**Example:**

.. code-block:: json

   "campaign_name": {
     "source": "@function:campaign_name_extract",
     "schema": {
       "type": "string",
       "units": "1",
       "_FillValue": "",
       "long_name": "voyage identifier"
     }
   }

And the function definition:

.. code-block:: json

   "functions": {
     "campaign_name_extract": {
       "extract_method": "object_key",
       "method": {
         "key_pattern": ".*/IMOS/AUV/{campaign_name}/{dive_name}/hydro_netcdf/{filename}",
         "extraction_code": "def extract_info_from_key(key):\n    parts = key.split('/')\n    return {'campaign_name': parts[-4]}"
       }
     }
   }

You may define multiple functions this way. They are applied to every input path at runtime.


Global Attributes
""""""""""""""""""

The `global_attributes` section allows you to delete or override global attributes on the output dataset.

**Delete global attributes**

.. code-block:: json

   "global_attributes": {
     "delete": [
       "geospatial_lat_max",
       "geospatial_lat_min",
       "geospatial_lon_max",
       "geospatial_lon_min",
       "date_created"
     ]
   }

**Set or override global attributes**

.. code-block:: json

   "global_attributes": {
     "set": {
       "title": "IMOS Underway CO2 dataset measured",
       "featureType": "trajectory",
       "principal_investigator": "",
       "principal_investigator_email": ""
     }
   }


Choosing the Partition keys
""""""""""""""""""""""""""""

Any variable available in the schema definition could be used as a
partition.

Partition keys are defined through the ``schema_transformation.partitioning`` section.

Each partitioning variable must exist, either in the original ``schema`` section, or added in ``add_variables``.

Timestamp partition
~~~~~~~~~~~~~~~~~~~

To enable time-based partitioning, a variable like ``timestamp`` is added using:

.. code-block:: json

    "timestamp": {
      "source": "@partitioning:time_extent",
      "schema": {
        "type": "int64",
        "units": "1",
        "long_name": "Partition timestamp"
      }
    }

This must be matched with a partitioning definition like:

.. code-block:: json

    {
      "source_variable": "timestamp",
      "type": "time_extent",
      "time_extent": {
        "time_varname": "TIME",
        "partition_period": "Q"
      }
    }

``partition_period`` controls how time is grouped: ``M`` (monthly), ``Y`` (yearly), ``Q`` (quarterly), etc. See the full list of supported values at:
https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-period-aliases

.. note:: Important Note
   :class: custom-note
   :name: timestamp-partition

    Choose the period wisely though testing! A finer period, such as day, will create a lot more objects or chunks
    and will slow considerably data queries


Geospatial Partition
~~~~~~~~~~~~~~~~~~~~

To add spatial filtering, define a variable like ``polygon``:

.. code-block:: json

    "polygon": {
      "source": "@partitioning:spatial_extent",
      "schema": {
        "type": "string",
        "units": "1",
        "long_name": "Spatial partition polygon"
      }
    }

Then define how it's calculated from the coordinates:

.. code-block:: json

    {
      "source_variable": "polygon",
      "type": "spatial_extent",
      "spatial_extent": {
        "lat_varname": "LATITUDE",
        "lon_varname": "LONGITUDE",
        "spatial_resolution": 5
      }
    }

``spatial_resolution`` controls the grid size in degrees. Smaller values give finer granularity.


.. note:: Important Note
   :class: custom-note
   :name: polygon-partition

    Choose the spatial_resolution wisely though testing! Similarly to the ``partition_timestamp_period`` above, a smaller
    value will lead to more objects.


Partition Key Summary
~~~~~~~~~~~~~~~~~~~~~

All partition keys must also be listed under the ``partitioning`` config, the order will matter. For example:

.. code-block:: json

    "partitioning": [
      {
        "source_variable": "timestamp",
        "type": "time_extent",
        "time_extent": {
          "time_varname": "TIME",
          "partition_period": "M"
        }
      },
      {
        "source_variable": "polygon",
        "type": "spatial_extent",
        "spatial_extent": {
          "lat_varname": "LATITUDE",
          "lon_varname": "LONGITUDE",
          "spatial_resolution": 5
        }
      },
      {
        "source_variable": "platform_code"
      }
    ],


In this example, the order of partitions will be ``timestamp`` -> ``polygon`` -> ``platform_code``

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

Add the following to the "schema_transformation" section

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


Global Attributes to drop and set
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Similar to Parquet. Add the following under the "schema_transformation" section

.. code:: json

  "global_attributes": {
      "set": {
        "title": ""
      },
      "delete": [
        "Voyage_number",
        "platform_code",
        "geospatial_lat_max",
        "geospatial_lat_min",
        "geospatial_lon_max",
        "geospatial_lon_min",
        "date_created"
      ]
    }


Global Attributes to variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Similar to Parquet. Add the following to the "schema_transformation" section.

.. note:: Important Note
   :class: custom-note
   :name: string_vars

     Due to some zarr/xarray bugs, only string variables are supported


.. code:: json

  "add_variables": {
      "quality_control_version": {
        "source": "@global_attribute:file_version",
        "schema": {
          "type": "<U49",
          "units": "1",
          "dimensions": "TIME"
        }
      },
      "platform_code": {
        "source": "@global_attribute:platform_code",
        "schema": {
          "type": "<U7",
          "units": "1",
          "dimensions": "TIME"
        }
      },
      "voyage_number": {
        "source": "@global_attribute:Voyage_number",
        "schema": {
          "type": "<U10",
          "units": "1",
          "dimensions": "TIME"
        }
      }
    }


Run Settings Options
---------------

Example
^^^^^^^^^

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
    * force_previous_parquet_deletion forces the search for existing parquet files to delete matching the new one to ingest. This can end up being really slow if there are a lot of objets
(for example Argo)



In order to create the dataset on a remote cluster (ec2/coiled), the
following configuration needs to be added within the run_settings:

Coiled Cluster configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For a coiled cluster, simply put this in the ``run_settings`` config

.. code-block:: json

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
^^^^^^^^^^^^^^^^^^^^^^^^^

As for above, in the EC2 cluster is to be chosen, simply put this in the ``run_settings`` config

.. code-block:: json

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

Local Development with MinIO (S3-Compatible Bucket)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


If you want to develop locally without connecting to AWS S3, you can use
`MinIO <https://min.io>`_, an S3-compatible object store that can run in Docker.

This allows you to test features such as bucket creation, file uploads, and
`s3fs` integration without needing real S3 credentials.

Running MinIO with Docker Compose
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Save the following as ``docker-compose.yml`` in your project root:

.. code-block:: yaml

    services:
      minio:
        image: quay.io/minio/minio:latest
        container_name: minio
        ports:
          - "9000:9000"  # S3 API
          - "9001:9001"  # Web console
        environment:
          MINIO_ROOT_USER: minioadmin
          MINIO_ROOT_PASSWORD: minioadmin
        command: server /data --console-address ":9001"
        volumes:
          - minio_data:/data

    volumes:
      minio_data:

Start MinIO with:

.. code-block:: bash

    docker compose up -d

Access the MinIO web console at: http://localhost:9001
(Default login: ``minioadmin / minioadmin``).

Creating a Bucket
~~~~~~~~~~~~~~~~~

Once MinIO is running, create a bucket (for example ``test-bucket``) either via
the web console or with the ``mc`` (MinIO client) CLI:

.. code-block:: bash

    docker run --rm -it \
      --network host \
      quay.io/minio/mc alias set local http://localhost:9000 minioadmin minioadmin

    docker run --rm -it \
      --network host \
      quay.io/minio/mc mb local/test-bucket


S3FS bucket endpoint patching for MinIO
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Two new optional settings allow to configure access to S3 (or S3-compatible) storage for **input** and **output** datasets.
These settings control authentication and client configuration used by ``s3fs`` / ``boto3``.

.. code-block:: json

  "s3_fs_common_opts": {
    "key": "minioadmin",
    "secret": "minioadmin",
    "client_kwargs": {
      "endpoint_url": "http://localhost:9000"
    }
  },
  "s3_bucket_opts": {
    "input_data": {
      "bucket": "imos-data",
      "s3_fs_opts": {
        "key": "minioadmin",
        "secret": "minioadmin",
        "client_kwargs": {
          "endpoint_url": "http://localhost:9000"
        }
      }
    },
    "output_data": {
      "bucket": "aodn-cloud-optimised",
      "s3_fs_opts": {
        "key": "minioadmin",
        "secret": "minioadmin",
          "client_kwargs": {
          "endpoint_url": "http://localhost:9000"
        }
      }
    }
  }

**Explanation**
~~~~~~~~~~~~~~~

- ``s3_fs_common_opts``
  Defines the **default connection options** shared by both input and output S3 clients (e.g. access keys, endpoint).
  If set, these options are used unless explicitly overridden by per-bucket configuration.

- ``s3_bucket_opts``
  Allows configuring **per-bucket overrides** for input and output datasets.
  Each section may define:
  - ``bucket`` → the bucket name to use for reading/writing data
  - ``s3_fs_opts`` → optional overrides to connection options defined in ``s3_fs_common_opts``

- Both ``input_data`` and ``output_data`` are optional.
  If not specified, the system falls back to default bucket names (``bucket_raw_default_name`` and ``optimised_bucket_name``) or environment variables.

**Precedence Rules**
~~~~~~~~~~~~~~~~~~~~

1. If ``s3_bucket_opts.<input_data|output_data>.s3_fs_opts`` is defined → it takes priority.
2. Otherwise, ``s3_fs_common_opts`` is used.
3. If neither is defined → the default global configuration is used.

.. note:: Important Note
   :class: custom-note
   :name: s3-config

   * ``s3_fs_common_opts`` and ``s3_bucket_opts`` are optional — they are mainly useful when pointing to **non-AWS endpoints** (e.g. MinIO, localstack) or when input and output buckets require different credentials.
   * If ``s3_fs_common_opts`` is provided, both a valid ``s3fs.S3FileSystem`` session and corresponding ``boto3`` client will be created automatically.
   * If you provide only one of (``s3_fs_common_opts`` or its corresponding boto client options), a validation error will be raised.

---

AWS OpenData registry
---------------------

In order to publicise the dataset on the OpenData Registry, the
following needs to be added to every dataset configuration file.

Once populated, the registry files needed by AWS can be created by the script
below, and then added to the AWS OpenData Github repository:
`AWS Open Data Registry <https://github.com/awslabs/open-data-registry>`_.


.. code-block:: json

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
