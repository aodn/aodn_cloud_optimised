Usage
=====

The library provides multiple ways to create cloud-optimised datasets:

1. **Recommended: Use ``generic_cloud_optimised_creation`` with dataset config**
2. Alternative: Write Python code using the :ref:`api-reference` directly
3. For development: Use integration tests and custom handlers

Generic Cloud Optimised Creation Script
----------------------------------------

The primary way to process datasets is via the ``generic_cloud_optimised_creation`` command with a dataset configuration file.

Basic usage:

.. code-block:: bash

    generic_cloud_optimised_creation --config <dataset_config.json>

Getting help:

.. code-block:: bash

    generic_cloud_optimised_creation -h

Key arguments
^^^^^^^^^^^^^

``-c, --config``
  Path or name of the dataset configuration JSON file. This is the main argument.
  Examples: ``mooring_hourly_timeseries_delayed_qc.json`` or ``satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.json``

``-o, --json-overwrite``
  Optional JSON string to override config fields at runtime. Useful for testing without modifying the config file.

  Example: ``'{"run_settings": {"cluster": {"mode": null}, "raise_error": true}}'``

``-t, --test``
  Use integration testing buckets instead of default buckets (for development and testing).

Advanced options for data retrieval
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When processing input data, you can control what gets fetched:

``--bucket-raw``
  S3 bucket containing input files. Default: ``imos-data``

``--optimised-bucket-name``
  S3 bucket where cloud-optimised output will be written. Default: ``imos-data-lab-optimised``

``--root-prefix-cloud-optimised-path``
  S3 path prefix for output location. Example: ``cloud_optimised/example_testing``

Examples
^^^^^^^^

Process a Zarr dataset (gridded data):

.. code-block:: bash

    generic_cloud_optimised_creation --config satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia

Process a Parquet dataset with testing bucket:

.. code-block:: bash

    generic_cloud_optimised_creation --config mooring_hourly_timeseries_delayed_qc --test

Override cluster configuration at runtime:

.. code-block:: bash

    generic_cloud_optimised_creation --config my_dataset \
      --json-overwrite '{"run_settings": {"cluster": {"mode": null}}}'

.. note:: Dataset-Specific Commands

   Many pre-configured dataset scripts are available in the library. These call ``generic_cloud_optimised_creation`` with pre-set parameters. See ``aodn_cloud_optimised/bin/datasets/`` in the `GitHub repository <https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/bin/datasets>`_ for examples.


As a python module
------------------

Zarr example
^^^^^^^^^^^^

.. code-block:: python

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
           clear_existing_data=True,  # this will delete existing data, be cautious! If testing change the paths below
           optimised_bucket_name="imos-data-lab-optimised",  # optional, default value in config/common.json
           root_prefix_cloud_optimised_path="cloud_optimised/example_testing",  # optional, default value in config/common.json
           cluster_mode='local'
        )


    if __name__ == "__main__":
        main()

Parquet Example
^^^^^^^^^^^^^^^

.. code-block:: python

    from importlib.resources import files

    from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
    from aodn_cloud_optimised.lib.config import (
        load_variable_from_config,
        load_dataset_config,
    )
    from aodn_cloud_optimised.lib.s3Tools import s3_ls


    def main():
        BUCKET_RAW_DEFAULT = load_variable_from_config("BUCKET_RAW_DEFAULT")
        nc_obj_ls = s3_ls(BUCKET_RAW_DEFAULT, "IMOS/ANMN/NSW")

        # Apply filters
        filters = [ "_hourly-timeseries_", "FV02"]
        for filter_str in filters:
            nc_obj_ls = [s for s in nc_obj_ls if filter_str in s]

        dataset_config = load_dataset_config(
            str(files("aodn_cloud_optimised").joinpath("config").joinpath("dataset").joinpath("mooring_hourly_timeseries_delayed_qc.json")
                )
            )

        cloud_optimised_creation(
           nc_obj_ls,
           dataset_config=dataset_config,
           clear_existing_data=True,  # this will delete existing data, be cautious! If testing change the paths below
           optimised_bucket_name="imos-data-lab-optimised",  # optional, default value in config/common.json
           root_prefix_cloud_optimised_path="cloud_optimised/example_testing",  # optional, default value in config/common.json
           cluster_mode='local'
        )


    if __name__ == "__main__":
        main()
