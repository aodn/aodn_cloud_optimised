.. _query-cloud-data:

Querying Cloud-Optimised Data
==============================

This guide shows how to read, filter, and visualize cloud-optimised datasets using the **GetAodn** API and the **DataQuery** library.

The DataQuery library (included with the ``notebooks`` extra) provides an intuitive interface for:

- Discovering available datasets
- Querying by spatial region (lat/lon) and time period
- Extracting time series at specific points
- Creating visualizations (maps, time series plots, etc.)

Installation
------------

To use DataQuery, install the ``notebooks`` extra:

.. code-block:: bash

    make notebooks
    # or for development: make dev

Quick Start
-----------

Import and initialize:

.. code-block:: python

    from aodn_cloud_optimised.lib.DataQuery import GetAodn

    # Initialize with default AWS S3 bucket
    aodn = GetAodn()

Browse available datasets:

.. code-block:: python

    # List all available datasets
    datasets = aodn.list_datasets()
    print(datasets[:5])  # Show first 5 datasets

Load a dataset:

.. code-block:: python

    # Load a Parquet dataset (tabular data)
    ds_parquet = aodn.get_dataset("mooring_temperature_logger_delayed_qc.parquet")

    # Load a Zarr dataset (gridded data)
    ds_zarr = aodn.get_dataset("satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.zarr")

Exploring Dataset Metadata
---------------------------

Get spatial and temporal extents:

.. code-block:: python

    # What geographic region does this dataset cover?
    extent = ds_parquet.get_spatial_extent()
    print(f"Bounding box: {extent}")

    # What time period is available?
    start_date, end_date = ds_parquet.get_temporal_extent()
    print(f"Data available: {start_date} to {end_date}")

Retrieve dataset metadata:

.. code-block:: python

    # Get full metadata
    metadata = ds_parquet.get_metadata()
    print(metadata.keys())

Querying Data
-------------

For Parquet Datasets (Tabular Data)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Query by geographic region and time:

.. code-block:: python

    # Get all data within a region and time period
    df = ds_parquet.get_data(
        lat_min=-35.0,
        lat_max=-30.0,
        lon_min=150.0,
        lon_max=155.0,
        date_start='2020-01-01',
        date_end='2020-12-31'
    )

    print(df.shape)  # (num_rows, num_columns)
    print(df.head())

Query by specific location (time series at a point):

.. code-block:: python

    # Extract time series at a specific location
    timeseries = ds_parquet.get_timeseries_data(
        var_name='TEMP',  # Variable name (adjust to your dataset)
        lat=-32.5,
        lon=152.5,
        date_start='2020-01-01',
        date_end='2020-12-31'
    )

    print(timeseries)

For Zarr Datasets (Gridded Data)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Query a gridded dataset:

.. code-block:: python

    # Get a spatial slice at a specific time
    data_array = ds_zarr.get_data(
        lat_min=-45.0,
        lat_max=-10.0,
        lon_min=110.0,
        lon_max=155.0,
        date_start='2020-01-01',
        date_end='2020-01-07'
    )

    # Result is an xarray Dataset
    print(data_array)

Time series at a point in gridded data:

.. code-block:: python

    # Extract time series at a grid point
    ts = ds_zarr.get_timeseries_data(
        var_name='sst',  # Sea surface temperature variable
        lat=-33.0,
        lon=151.0,
        date_start='2020-01-01',
        date_end='2020-12-31'
    )

Visualizations
--------------

Plot spatial extent:

.. code-block:: python

    # Show the geographic coverage of the dataset
    ds_parquet.plot_spatial_extent()

Plot time series:

.. code-block:: python

    # Plot a time series
    ds_parquet.plot_timeseries(
        timeseries,
        title='Temperature over time'
    )

Plot gridded data:

.. code-block:: python

    # Plot a gridded variable
    ds_zarr.plot_gridded_variable(
        var_name='sst',
        cmap='RdYlBu_r'
    )

Advanced: Custom S3 Storage
---------------------------

To query data from a custom S3 bucket (e.g., MinIO, LocalStack):

.. code-block:: python

    aodn = GetAodn(
        bucket_name="my-custom-bucket",
        prefix="cloud_optimised/my_datasets",
        s3_fs_opts={
            "key": "access_key",
            "secret": "secret_key",
            "client_kwargs": {
                "endpoint_url": "http://minio.example.com:9000"
            }
        }
    )

    ds = aodn.get_dataset("my_dataset.parquet")

Example Workflows
-----------------

**Workflow 1: Analyze mooring temperature data**

.. code-block:: python

    # Load mooring data
    mooring = aodn.get_dataset("mooring_temperature_logger_delayed_qc.parquet")

    # Get 1 year of data for a specific region
    df = mooring.get_data(
        lat_min=-35, lat_max=-30,
        lon_min=150, lon_max=155,
        date_start='2022-01-01',
        date_end='2022-12-31'
    )

    # Calculate statistics
    print(df['TEMP'].describe())

    # Plot
    import matplotlib.pyplot as plt
    df.set_index('TIME')['TEMP'].plot(figsize=(12, 4))
    plt.ylabel('Temperature (°C)')
    plt.show()

**Workflow 2: Compare satellite SST with mooring observations**

.. code-block:: python

    # Get satellite data
    sst = aodn.get_dataset("satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.zarr")

    # Get mooring data at same location
    mooring = aodn.get_dataset("mooring_temperature_logger_delayed_qc.parquet")

    # Extract at common location
    sat_ts = sst.get_timeseries_data(
        var_name='sst',
        lat=-33.0, lon=151.0,
        date_start='2020-01-01',
        date_end='2020-12-31'
    )

    moor_ts = mooring.get_timeseries_data(
        var_name='TEMP',
        lat=-33.0, lon=151.0,
        date_start='2020-01-01',
        date_end='2020-12-31'
    )

    # Compare
    import pandas as pd
    comparison = pd.DataFrame({
        'satellite': sat_ts,
        'mooring': moor_ts
    })
    print(comparison.corr())

See Also
--------

- :ref:`getting-started` — Installation and setup
- :ref:`notebooks` — Example Jupyter notebooks
- :ref:`mcp-server` — Using the MCP server for AI integration
- `DataQuery API Reference <https://aodn-cloud-optimised.readthedocs.io/en/latest/module-overview.html>`_
