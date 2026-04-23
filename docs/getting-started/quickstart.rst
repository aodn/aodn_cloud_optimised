.. _quickstart:

Quick Start
===========

Once you've installed the package, here's a simple example to process a dataset.

Core Data Processing
--------------------

If you installed the **core** package with ``make core``, you can process a dataset using the command line:

.. code-block:: bash

    generic_cloud_optimised_creation \
      --dataset-config config/dataset/your_dataset.json \
      --paths "s3://bucket/path/to/data/" \
      --cluster-mode local

This will:
1. Read the dataset configuration from JSON
2. Discover files matching the path pattern
3. Convert and optimize the data to cloud format (Zarr or Parquet)
4. Upload to S3

For detailed configuration options, see :ref:`dataset-config-doc`.

Using DataQuery API (Notebooks)
-------------------------------

If you installed with notebooks support (``make dev`` or ``pip install .[notebooks]``),
you can query data from Jupyter:

.. code-block:: python

    from DataQuery import GetAodn

    aodn = GetAodn()

    # Query data from a dataset
    df = aodn.get_dataset('argo.parquet').get_data(
        date_start='2020-01-01',
        date_end='2020-03-01',
        lat_min=-35,
        lat_max=-27,
        lon_min=150,
        lon_max=158
    )

    print(df.head())

See :ref:`usage` and individual dataset notebooks in :ref:`notebooks` for more examples.

Next Steps
----------

- **Configure a new dataset**: :ref:`dataset-config-doc`
- **Write a Jupyter notebook**: :ref:`notebooks`
- **Set up the MCP server**: :ref:`mcp-server`
- **Contribute code**: :ref:`development`
