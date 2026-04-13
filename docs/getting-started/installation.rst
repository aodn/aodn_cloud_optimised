.. _install:

Installation
============

Choose your installation method based on your use case:

- **Data Processing Only** (recommended): ``make core``
- **Jupyter Notebooks**: ``make notebooks``
- **Contributing to the Project**: ``make dev``
- **Building Docs**: ``make docs``
- **Full Test Suite**: ``make tests``

Requirements
------------

- Python >= 3.11
- `miniforge3 <https://github.com/conda-forge/miniforge>`_ **or** Poetry (see installation options below)

Recommended: Data Processing (Core Only)
========================================

The fastest way to start processing data is with the core package:

.. code-block:: bash

   git clone https://github.com/aodn/aodn_cloud_optimised.git
   cd aodn_cloud_optimised
   make core

This installs the minimum set: zarr, parquet, xarray, dask, and all processing dependencies.

Then use:

.. code-block:: bash

   generic_cloud_optimised_creation --dataset-config config/dataset/your_dataset.json ...

See :ref:`quickstart` for an example.

Jupyter Notebooks (Data Analysis)
=================================

To use the DataQuery API and create analysis notebooks:

.. code-block:: bash

   git clone https://github.com/aodn/aodn_cloud_optimised.git
   cd aodn_cloud_optimised
   make notebooks

This adds Jupyter, matplotlib, cartopy, and visualization tools. See :ref:`notebooks` for examples.

Contributing to the Project
===========================

For full development setup (recommended for contributors):

**1. Clone the repository**

.. code-block:: bash

   gh repo clone aodn/aodn_cloud_optimised
   cd aodn_cloud_optimised

**2. Install using Makefile (Poetry venv)**

The **Makefile** is the primary recommended workflow for contributors:

.. code-block:: bash

   make dev

This creates a Poetry-managed virtual environment with all tools: testing, documentation, linting, and debugging.

After setup, install pre-commit hooks:

.. code-block:: bash

   poetry run pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: make-dev

   Run ``make dev`` once after cloning and after every ``git pull`` that
   changes ``poetry.lock`` or ``pyproject.toml``.

**Alternative: Mamba/Conda named environments**

For contributors who prefer conda/mamba named environments:

.. code-block:: bash

   ./setup_miniforge_venvs.sh dev        # AodnCloudOptimised_dev (recommended)
   ./setup_miniforge_venvs.sh notebooks  # AodnCloudOptimised_notebooks
   ./setup_miniforge_venvs.sh tests      # AodnCloudOptimised_tests
   ./setup_miniforge_venvs.sh docs       # AodnCloudOptimised_docs
   ./setup_miniforge_venvs.sh all        # Create all at once

   # Activate the environment
   mamba activate AodnCloudOptimised_dev
   pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: mamba-env

   The mamba env may need to be (re)activated after installation for all
   scripts to be available in ``$PATH``.

Dependency Extras
-----------------

The project uses `PEP 621 optional extras <https://peps.python.org/pep-0621/#dependencies-optional-dependencies>`_
so you only install what you need:

.. list-table::
   :header-rows: 1
   :widths: 20 50 30

   * - Extra
     - Contents
     - Use case
   * - *(core)*
     - zarr, parquet, xarray, dask, coiled
     - Data processing pipelines
   * - ``notebooks``
     - core + DataQuery + Jupyter, matplotlib, cartopy, seaborn
     - Jupyter notebooks, DataQuery API, data analysis
   * - ``tests``
     - core + pytest, coverage, moto
     - Running the test suite
   * - ``docs``
     - core + Sphinx and tools
     - Building documentation locally
   * - ``mcp``
     - core + MCP server dependencies
     - Running the Model Context Protocol server
   * - ``dev``
     - All of the above + poetry, pre-commit, ipdb
     - Full contributor setup (recommended)

Direct Installation (pip / Poetry)
==================================

If you're not using the Makefile, you can install directly:

**Using pip** (in an existing environment):

.. code-block:: bash

   pip install aodn-cloud-optimised              # core only
   pip install aodn-cloud-optimised[notebooks]   # core + notebooks
   pip install aodn-cloud-optimised[dev]         # full dev setup

**Using Poetry directly**:

.. code-block:: bash

   poetry install --with dev
   poetry run pre-commit install

DataQuery Import Note
---------------------

.. note::

   ``DataQuery`` is **not** exported from the top-level package. Always import it
   directly::

       from aodn_cloud_optimised.lib.DataQuery import GetAodn

What's Next?
============

- **Process your first dataset**: :ref:`quickstart`
- **Configure a new dataset**: :ref:`dataset-config-doc`
- **Write Jupyter notebooks**: :ref:`notebooks`
- **Set up the MCP server**: :ref:`mcp-server`
- **Start contributing**: :ref:`development`
