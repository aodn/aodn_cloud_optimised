.. _debug-doc:

Debugging Dask
==============

It can be tricky to debug Dask run, even with the LocalCluster.

As of late 2024, debugging with PyCharm a LocalCluster fails.


The easiest way is to run for example

.. code-block:: bash

    pytest -s test_generic_zarr_handler_netcdf3_netcdf4.py

and having anywhere in the code

.. code-block:: python

    import ipdb; ipdb.set_trace()
