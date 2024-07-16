# AODN Cloud Optimised Conversion

![Build Status](https://github.com/aodn/aodn_cloud_optimised/actions/workflows/build.yml/badge.svg)
![Test Status](https://github.com/aodn/aodn_cloud_optimised/actions/workflows/test-mamba.yml/badge.svg)
![Release](https://img.shields.io/github/v/release/aodn/aodn_cloud_optimised.svg)
[![codecov](https://codecov.io/gh/aodn/aodn_cloud_optimised/branch/main/graph/badge.svg)](https://codecov.io/gh/aodn/aodn_cloud_optimised/branch/main)
[![Documentation Status](https://readthedocs.org/projects/aodn-cloud-optimised/badge/?version=latest)](https://aodn-cloud-optimised.readthedocs.io/en/latest/?badge=latest)

A tool designed to convert IMOS NetCDF and CSV files into Cloud Optimised formats such as Zarr and Parquet

## Documentation

Visit the documentation on [ReadTheDocs](https://aodn-cloud-optimised.readthedocs.io/en/latest/) for detailed information.

[![Documentation Status](https://readthedocs.org/projects/aodn-cloud-optimised/badge/?version=latest)](https://aodn-cloud-optimised.readthedocs.io/en/latest/?badge=latest)

## Key Features

* Conversion of CSV/NetCDF to Cloud Optimised format (Zarr/Parquet)
  * YAML configuration approach with parent and child YAML configuration if multiple dataset are very similar (i.e. Radar ACORN, GHRSST, see [config](https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/config/dataset))
  * Generic handlers for most dataset ([GenericParquetHandler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/GenericParquetHandler.py), [GenericZarrHandler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/GenericZarrHandler.py)).
  * Specific handlers can be written and inherits methods from a generic handler ([Argo handler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/ArgoHandler.py), [Mooring Timseries Handler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/AnmnHourlyTsHandler.py))
* Clustering capability:
  * Local dask cluster
  * Remote Coiled cluster
  * driven by configuration/can be easily overwritten
  * Zarr: gridded dataset are done in batch and in parallel with xarray.open_mfdataset
  * Parquet: tabular files are done in batch and in parallel as independent task, done with future
* Reprocessing:
  * Zarr,: reprocessing is achieved by writting to specific regions with slices. Non-contigous regions are handled
  * Parquet: reprocessing is done via pyarrow internal overwritting function, but can also be forced in case an input file has significantly changed
* Chunking:
  * Parquet: to facilitate the query of geospatial data, polygon and timestamp slices are created as partitions
  * Zarr: done via dataset configuration
* Metadata:
  * Parquet: Metadata is created as a sidecar _metadata.parquet file
* Unittesting of module: Very close to integration testing, local cluster is used to create cloud optimised files


# Quick Guide
## Installation

Requirements:
* Python >= 3.10.14
* AWS SSO to push files to S3
* An account on [Coiled](https://cloud.coiled.io/) for remote clustering (Optional)


### Automatic installation of the latest wheel release
```bash
curl -s https://raw.githubusercontent.com/aodn/aodn_cloud_optimised/main/install.sh | bash
```

Otherwise, go to the [release](http://github.com/aodn/aodn_cloud_optimised/releases/latest) page.

## Development
See [ReadTheDocs - Dev](https://aodn-cloud-optimised.readthedocs.io/en/latest/development/installation.html)

## Usage
See [ReadTheDocs - Usage](https://aodn-cloud-optimised.readthedocs.io/en/latest/usage.html)

# Notebooks
[Notebooks](https://github.com/aodn/aodn_cloud_optimised/blob/main/notebooks/) can directly be imported into Google Colab.
