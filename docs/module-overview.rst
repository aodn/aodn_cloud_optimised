Module Overview
===============

Generic Cloud Optimised Creation
--------------------------------
The ``generic_cloud_optimised_creation`` script is installed as part of the `aodn_cloud_optimised` package.


.. automodule:: aodn_cloud_optimised.bin.generic_cloud_optimised_creation
    :members:
    :undoc-members:

.. _dataset-config-script:

Create Dataset Configuration (semi-automatic)
---------------------------------------------
The ``cloud_optimised_create_dataset_config`` script is installed as part of the `aodn_cloud_optimised` package and
helps greatly in the creation of a full dataset configuration. The file will need to be modified manually, see
full documentation at :ref:`dataset-config-doc`


.. autofunction:: aodn_cloud_optimised.bin.create_dataset_config.main


Create AWS Registry dataset entry
---------------------------------
The ``cloud_optimised_create_aws_registry_dataset`` script is installed as part of the `aodn_cloud_optimised` package.


Usage Example
~~~~~~~~~~~~~
.. asciinema:: _static/recordings/create_aws_registry.cast
   :preload: 1
   :theme: solarized-dark
   :autoplay: true
   :speed: 0.80
   :cols: 100
   :rows: 30


Definition
~~~~~~~~~~

.. autofunction:: aodn_cloud_optimised.bin.create_aws_registry_dataset.main


Common Handler
--------------
.. automodule:: aodn_cloud_optimised.lib.CommonHandler
    :members:


Parquet Handlers
----------------

handler steps
~~~~~~~~~~~~~
The conversion process is broken down into a series of ordered steps, each responsible for a specific task. These steps include:

1. **delete_existing_matching_parquet**: Deletes existing Parquet files that match the current processing criteria.

2. **preprocess_data**: Generates a DataFrame and Dataset from the input NetCDF file.

3. **publish_cloud_optimised**: Creates Parquet files containing the processed data.
   - **_add_timestamp_df**: Adds timestamp information to the DataFrame. Useful for partitioning.
   - **_add_columns_df**: Adds generic columns such as site_code and filename to the DataFrame.
   - **_add_columns_df_custom**: Adds custom columns (useful for specific handlers).
   - **_rm_bad_timestamp_df**: Removes rows with bad timestamps from the DataFrame.
   - **_add_metadata_sidecar**: Adds metadata from the PyArrow table to the xarray dataset as sidecar attributes.

4. **postprocess**: Cleans up resources used during data processing.


Generic Parquet Handler definition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automodule:: aodn_cloud_optimised.lib.GenericParquetHandler
   :members:
   :show-inheritance:

.. inheritance-diagram:: aodn_cloud_optimised.lib.GenericParquetHandler


Argo Parquet Handler
~~~~~~~~~~~~~~~~~~~~

.. automodule:: aodn_cloud_optimised.lib.ArgoHandler
   :members:
   :show-inheritance:

.. inheritance-diagram:: aodn_cloud_optimised.lib.ArgoHandler


Mooring Hourly Timeseries Parquet Handler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: aodn_cloud_optimised.lib.AnmnHourlyTsHandler
   :members:
   :show-inheritance:

.. inheritance-diagram:: aodn_cloud_optimised.lib.AnmnHourlyTsHandler


Zarr Handler
------------
handler steps
~~~~~~~~~~~~~

Handler definition
~~~~~~~~~~~~~~~~~~
.. automodule:: aodn_cloud_optimised.lib.GenericZarrHandler
   :members:

.. inheritance-diagram:: aodn_cloud_optimised.lib.GenericZarrHandler
