
.. _update-metadata-doc:

Update All Metadata Script
==========================

``cloud_optimised_update_all_metadata`` is a command-line utility for validating dataset configuration files and updating metadata directly in cloud-optimised formats (Zarr or Parquet) without having to process new data.

Usage
-----

.. code-block:: bash

    cloud_optimised_update_all_metadata

This script will:

1. Validate ALL JSON configuration files.
2. Load the corresponding dataset configuration.
3. Detect the output format (Zarr or Parquet).
4. Use the appropriate handler (zarr/parquet) to update global and variable metadata.

By default, it processes all files in:

.. code-block:: text

    aodn_cloud_optimised/config/dataset/

To specify a subset of JSON files, modify the `json_files` argument in the script.

Output
------

- Logs validation errors or metadata update failures.
- Applies changes directly to the cloud-optimised datasets without rewriting the entire store.

.. code-block:: bash

    [DEBUG] Initializing logger: animal_acoustic_tracking_delayed_qc
    [DEBUG] Adding StreamHandler...
    [DEBUG] StreamHandler added.
    [DEBUG] Adding FileHandler...
    [DEBUG] FileHandler added. Log file path: /tmp/cloud_optimised_animal_acoustic_tracking_delayed_qc_2025-08-04.log
    2025-08-04 17:33:44,717 - INFO - CommonHandler.py:343 - validate_json - Successfully validated JSON configuration for dataset animal_acoustic_tracking_delayed_qc against schema_validation_parquet.json.
    2025-08-04 17:33:48,630 - INFO - GenericParquetHandler.py:897 - _add_metadata_sidecar - None: Existing Parquet store found at s3://imos-data-lab-optimised/animal_acoustic_tracking_delayed_qc.parquet. Updating Metadata
    2025-08-04 17:33:50,947 - INFO - GenericParquetHandler.py:975 - _add_metadata_sidecar - None: Parquet metadata file successfully published to s3://imos-data-lab-optimised/animal_acoustic_tracking_delayed_qc.parquet/_common_metadata

    [DEBUG] Initializing logger: animal_ctd_satellite_relay_tagging_delayed_qc
    [DEBUG] Adding StreamHandler...
    [DEBUG] StreamHandler added.
    [DEBUG] Adding FileHandler...
    [DEBUG] FileHandler added. Log file path: /tmp/cloud_optimised_animal_ctd_satellite_relay_tagging_delayed_qc_2025-08-04.log
    2025-08-04 17:33:50,966 - INFO - CommonHandler.py:343 - validate_json - Successfully validated JSON configuration for dataset animal_ctd_satellite_relay_tagging_delayed_qc against schema_validation_parquet.json.
    2025-08-04 17:33:51,873 - INFO - GenericParquetHandler.py:897 - _add_metadata_sidecar - None: Existing Parquet store found at s3://imos-data-lab-optimised/animal_ctd_satellite_relay_tagging_delayed_qc.parquet. Updating Metadata
    2025-08-04 17:33:52,087 - INFO - GenericParquetHandler.py:975 - _add_metadata_sidecar - None: Parquet metadata file successfully published to s3://imos-data-lab-optimised/animal_ctd_satellite_relay_tagging_delayed_qc.parquet/_common_metadata

    [DEBUG] FileHandler added. Log file path: /tmp/cloud_optimised_satellite_ghrsst_l4_gamssa_1day_multi_sensor_world_2025-08-04.log
    2025-08-04 17:35:20,127 - INFO - CommonHandler.py:343 - validate_json - Successfully validated JSON configuration for dataset satellite_ghrsst_l4_gamssa_1day_multi_sensor_world against schema_validation_zarr.json.
    2025-08-04 17:35:21,026 - ERROR - GenericZarrHandler.py:646 - _update_metadata - Dataset satellite_ghrsst_l4_gamssa_1day_multi_sensor_world does not exist yet - cannot update metadata
    [DEBUG] Initializing logger: satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia
    [DEBUG] Adding StreamHandler...
    [DEBUG] StreamHandler added.
    [DEBUG] Adding FileHandler...
    [DEBUG] FileHandler added. Log file path: /tmp/cloud_optimised_satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia_2025-08-04.log
    2025-08-04 17:35:21,060 - INFO - CommonHandler.py:343 - validate_json - Successfully validated JSON configuration for dataset satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia against schema_validation_zarr.json.
    2025-08-04 17:35:21,966 - INFO - GenericZarrHandler.py:599 - _update_metadata - None: Existing Zarr store found at s3://imos-data-lab-optimised/satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr. Updating Metadata
    2025-08-04 17:35:21,968 - INFO - GenericZarrHandler.py:603 - _update_metadata - Dataset satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia: Updating Global Attributes
    2025-08-04 17:35:23,165 - INFO - GenericZarrHandler.py:608 - _update_metadata - Dataset satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia: Updating Variable Attributes
    2025-08-04 17:35:23,305 - WARNING - GenericZarrHandler.py:687 - update_store_varattrs_from_schema - None: ⚠️  Type mismatch for 'time': schema says 'timestamp[ns]', Zarr store has 'int32'
    2025-08-04 17:35:24,531 - WARNING - GenericZarrHandler.py:687 - update_store_varattrs_from_schema - None: ⚠️  Type mismatch for 'lat': schema says 'float', Zarr store has 'float32'
    2025-08-04 17:35:26,358 - WARNING - GenericZarrHandler.py:687 - update_store_varattrs_from_schema - None: ⚠️  Type mismatch for 'lon': schema says 'float', Zarr store has 'float32'
    2025-08-04 17:35:29,033 - WARNING - GenericZarrHandler.py:687 - update_store_varattrs_from_schema - None: ⚠️  Type mismatch for 'sea_ice_fraction': schema says 'double', Zarr store has 'float64'
    2025-08-04 17:35:31,021 - WARNING - GenericZarrHandler.py:712 - update_store_varattrs_from_schema - None: ⚠️  Variable 'analysed_sst' in schema not found in Zarr store. Skipping.
    2025-08-04 17:35:31,163 - WARNING - GenericZarrHandler.py:687 - update_store_varattrs_from_schema - None: ⚠️  Type mismatch for 'analysis_error': schema says 'double', Zarr store has 'float64'
    2025-08-04 17:35:32,708 - WARNING - GenericZarrHandler.py:687 - update_store_varattrs_from_schema - None: ⚠️  Type mismatch for 'mask': schema says 'float', Zarr store has 'float64'
    2025-08-04 17:35:35,315 - WARNING - GenericZarrHandler.py:712 - update_store_varattrs_from_schema - None: ⚠️  Variable 'crs' in schema not found in Zarr store. Skipping.
    2025-08-04 17:35:55,934 - INFO - GenericZarrHandler.py:641 - _update_metadata - None: All expected global attributes successfully updated for dataset 'satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia'.


Notes
-----

- Make sure your configuration files are valid before applying updates.
- Only metadata is modified; no data is changed or re-encoded.
- For a Parquet dataset, the metadata sidecar file is updated
- For a Zarr dataset, variable attributes and global attributes are updated directly into the store. The dataset is then loaded to be checked
