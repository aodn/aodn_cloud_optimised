Usage
=====


As a standalone bash script
----------------------------

.. code-block:: bash

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
      --exclude EXCLUDE     Optional string to exclude files listed by s3_ls to filter S3 objects. '_FV01_'
      --dataset-config DATASET_CONFIG
                            Path to the dataset config JSON file. Example: 'mooring_hourly_timeseries_delayed_qc.json'
      --clear-existing-data
                            Flag to clear existing data. Default is False.
      --force-previous-parquet-deletion
                            Flag to force the search of previous equivalent parquet file created. Much slower. Default is False. Only for Parquet processing.
      --cluster-mode {local,remote}
                            Cluster mode to use. Options: 'local' or 'remote'. Default is 'local'.
      --optimised-bucket-name OPTIMISED_BUCKET_NAME
                            Bucket name where cloud optimised object will be created. Default is 'imos-data-lab-optimised'
      --root-prefix-cloud-optimised-path ROOT_PREFIX_CLOUD_OPTIMISED_PATH
                            Prefix value for the root location of the cloud optimised objects, such as s3://optimised-bucket-name/root-prefix-cloud-optimised-path/... Default is 'cloud_optimised/cluster_testing'
      --bucket-raw BUCKET_RAW
                            Bucket name where input object files will be searched for. Default is 'imos-data'
      --raise-error         Flag to exit the code on the first error. Default is False.


    Examples:
      generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA' --filters '_hourly-timeseries_' 'FV02' --dataset-config 'mooring_hourly_timeseries_delayed_qc.json' --clear-existing-data --cluster-mode 'remote'
      generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/QLD' --dataset-config 'anmn_ctd_ts_fv01.json'
      generic_cloud_optimised_creation --paths 'IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024' --dataset-config 'radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json' --clear-existing-data --cluster-mode 'remote'



.. note:: Important Note
   :class: custom-note
   :name: cloud-opt-scripts

   Specific dataset scripts are defined in the ``pyproject.toml`` file and can be directly run. They are defined under the following URL:

   `https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/bin <https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/bin>`_


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
