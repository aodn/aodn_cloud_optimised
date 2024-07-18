Module Overview
===============

Generic Cloud Optimised Creation
--------------------------------
.. automodule:: aodn_cloud_optimised.bin.generic_cloud_optimised_creation
    :members:
    :undoc-members:

Create AWS Registry dataset entry
---------------------------------
.. automodule:: aodn_cloud_optimised.bin.create_aws_registry_dataset
    :members:
    :undoc-members:

Terminal Recording
~~~~~~~~~~~~~~~~~~
.. asciinema:: _static/recordings/create_aws_registry.cast
   :preload: 1
   :theme: solarized-dark


Common Handler
--------------
.. automodule:: aodn_cloud_optimised.lib.CommonHandler
    :members:


Parquet Handler
---------------

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

Handler definition
~~~~~~~~~~~~~~~~~~
.. automodule:: aodn_cloud_optimised.lib.GenericParquetHandler
   :members:
   :show-inheritance:



Zarr Handler
------------
handler steps
~~~~~~~~~~~~~

Handler definition
~~~~~~~~~~~~~~~~~~
.. automodule:: aodn_cloud_optimised.lib.GenericZarrHandler
   :members:
