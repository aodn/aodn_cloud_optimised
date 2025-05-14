.. _debug-doc:

Debugging Dask
==============

It can be tricky to debug Dask run, even with the LocalCluster.

As of late 2024, debugging with PyCharm a LocalCluster fails.


Debug Unittests
=========
The easiest way is to run for example

.. code-block:: bash

    pytest -s test_generic_zarr_handler_netcdf3_netcdf4.py

and having anywhere in the code

.. code-block:: python

    import ipdb; ipdb.set_trace()


Debug a cloud optimised pipeline
================================

Modify a dataset processing script to use a None cluster, for example:

.. code-block:: python

    def main():
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            "IMOS/SRS/SST/ghrsst/L3S-1dS/dn/",
            "--dataset-config",
            "satellite_ghrsst_l3s_1day_daynighttime_single_sensor_southernocean.json",
            # "--cluster-mode",
            # "local",
        ]


and add anywhere in the code

.. code-block:: python

    import ipdb; ipdb.set_trace()


Run as normal
