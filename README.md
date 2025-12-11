# AODN (Australian Ocean Data Network) Cloud Optimised library

![Build Status](https://github.com/aodn/aodn_cloud_optimised/actions/workflows/build.yml/badge.svg)
![Test Status](https://github.com/aodn/aodn_cloud_optimised/actions/workflows/test-mamba.yml/badge.svg)
![Release](https://img.shields.io/github/v/release/aodn/aodn_cloud_optimised.svg)
[![codecov](https://codecov.io/gh/aodn/aodn_cloud_optimised/branch/main/graph/badge.svg)](https://codecov.io/gh/aodn/aodn_cloud_optimised/branch/main)
[![Documentation Status](https://readthedocs.org/projects/aodn-cloud-optimised/badge/?version=latest)](https://aodn-cloud-optimised.readthedocs.io/en/latest/?badge=latest)
[![Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/aodn/aodn_cloud_optimised/)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/aodn/aodn_cloud_optimised/main?filepath=notebooks)

AODN Cloud Optimised library allows to convert oceanographic datasets from [IMOS (Integrated Marine Observing System)](https://imos.org.au/) / [AODN (Australian Ocean Data Network)](https://portal.aodn.org.au/) into cloud-optimised formats such as [Zarr](https://zarr.readthedocs.io/) (for gridded multidimensional data) and [Parquet](https://parquet.apache.org/docs/) (for tabular data).

## Documentation

Visit the documentation on [ReadTheDocs](https://aodn-cloud-optimised.readthedocs.io/en/latest/) for detailed information.

[![Documentation Status](https://readthedocs.org/projects/aodn-cloud-optimised/badge/?version=latest)](https://aodn-cloud-optimised.readthedocs.io/en/latest/?badge=latest)

## Key Features

### Data Conversion
- Convert **CSV** or **NetCDF** (single or multidimensional) to **Zarr** or **Parquet**.
- **Dataset configuration:** YAML-based configuration with inheritance, allowing similar datasets to share settings.  
  Example: [Radar ACORN](https://github.com/aodn/aodn_cloud_optimised/tree/main/aodn_cloud_optimised/config/dataset), [GHRSST](https://www.ghrsst.org/).
- Semi-automatic creation of dataset configuration: [ReadTheDocs guide](https://aodn-cloud-optimised.readthedocs.io/en/latest/development/dataset-configuration.html#create-dataset-configuration-semi-automatic).
- Generic handlers for standard datasets:  
  [GenericParquetHandler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/GenericParquetHandler.py),  
  [GenericZarrHandler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/GenericZarrHandler.py)
- Custom handlers can inherit from generic handlers:  
  [Argo handler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/ArgoHandler.py),  
  [Mooring Timeseries Handler](https://github.com/aodn/aodn_cloud_optimised/blob/main/aodn_cloud_optimised/lib/AnmnHourlyTsHandler.py)

### Clustering & Parallel Processing
- Supports local **Dask cluster** and remote clusters:
  - [Coiled](https://aodn-cloud-optimised.readthedocs.io/en/latest/development/dataset-configuration.html#coiled-cluster-configuration)
  - [EC2](https://aodn-cloud-optimised.readthedocs.io/en/latest/development/dataset-configuration.html#ec2-cluster-configuration)
  - Fargate cluster
- Cluster behaviour is configuration-driven and can be easily overridden.
- Automatic restart of remote cluster upon Dask failure.
- **Zarr:** Gridded datasets are processed in batch and in parallel using [`xarray.open_mfdataset`](https://xarray.pydata.org/en/stable/generated/xarray.open_mfdataset.html).
- **Parquet:** Tabular files are processed in batch and in parallel as independent tasks, implemented with `concurrent.futures.Future`.
- **S3 / S3-Compatible Storage Support:**  
  Support for AWS S3 and S3-compatible endpoints (e.g., MinIO, LocalStack) with configurable input/output buckets and authentication via `s3fs` and `boto3`. 
### Reprocessing
- **Zarr:** Reprocessing is achieved by writing to specific slices, including non-contiguous regions.
- **Parquet:** Reprocessing uses PyArrow internal overwriting; can also be forced when input files change significantly.

### Chunking & Partitioning
- Improves performance for querying and parallel processing.
- **Parquet:** Partitioned by polygon and timestamp slices. [Issue reference](https://github.com/aodn/aodn_cloud_optimised/issues/240)
- **Zarr:** Chunking is defined in dataset configuration.

### Dynamic Variable Definition
See [doc](https://aodn-cloud-optimised.readthedocs.io/en/latest/development/dataset-configuration.html#adding-variables-dynamically)
- Global Attributes -> variable
- variable attribute -> variable
- filename part -> variable
- ...
  
### Metadata
- **Parquet:** Metadata stored as a sidecar `_metadata.parquet` file for faster queries and schema discovery.


# Quick Guide
## Installation

Requirements:
* Python >= 3.10.14
* AWS SSO configured for pushing files to S3
* Optional: [Coiled](https://cloud.coiled.io/) account for remote clustering

### Automatic installation of the latest wheel release
```bash
curl -s https://raw.githubusercontent.com/aodn/aodn_cloud_optimised/main/install.sh | bash
```

Otherwise, go to the [release](http://github.com/aodn/aodn_cloud_optimised/releases/latest) page.

## Development
See [ReadTheDocs - Dev](https://aodn-cloud-optimised.readthedocs.io/en/latest/development/installation.html)

## Usage
See [ReadTheDocs - Usage](https://aodn-cloud-optimised.readthedocs.io/en/latest/usage.html)

## Getting Started - Notebooks
[![Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/aodn/aodn_cloud_optimised/)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/aodn/aodn_cloud_optimised/main?filepath=notebooks)

A curated list of Jupyter [Notebooks](https://github.com/aodn/aodn_cloud_optimised/blob/main/notebooks/) ready to be loaded in Google Colab and Binder for users to play with IMOS/AODN converted to Cloud Optimised dataset. Click on the badge above
