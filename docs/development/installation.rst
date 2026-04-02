.. _install-doc:

Contributing
============

Clone the repository
--------------------

.. code-block:: bash

   gh repo clone aodn/aodn_cloud_optimised
   cd aodn_cloud_optimised

Installation
------------

Requirements:

- Python >= 3.11
- `miniforge3 <https://github.com/conda-forge/miniforge>`_ **or** Poetry (see options below)

Dependency Extras
~~~~~~~~~~~~~~~~~

The project uses `PEP 621 optional extras <https://peps.python.org/pep-0621/#dependencies-optional-dependencies>`_
so you only install what you need:

.. list-table::
   :header-rows: 1
   :widths: 15 50 35

   * - Extra
     - Contents
     - Use case
   * - *(core)*
     - zarr / parquet / NetCDF processing, Dask, Coiled
     - Data processing pipelines
   * - ``notebooks``
     - :mod:`DataQuery` + visualisation libs (cartopy, matplotlib, seaborn, …)
     - Jupyter notebooks, DataQuery API
   * - ``tests``
     - pytest, coverage, moto
     - Running the test suite
   * - ``docs``
     - Sphinx and related tools
     - Building documentation
   * - ``dev``
     - Contributor tooling: poetry, pre-commit, ipdb
     - Code quality / tooling

.. note::

   ``DataQuery`` is **not** exported from the top-level package. Import it
   directly::

       from aodn_cloud_optimised.lib.DataQuery import GetAodn

----

Recommended: Makefile (Poetry venv)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The **Makefile** is the primary recommended workflow for contributors using
Poetry's built-in virtual environment (configured via ``poetry.toml``):

.. code-block:: bash

   make core       # core data-processing only
   make notebooks  # core + DataQuery / Jupyter notebooks
   make tests      # core + test infra + notebooks (full test suite)
   make docs       # core + Sphinx tools
   make dev        # everything — full contributor setup (recommended)

After ``make dev``, install pre-commit hooks:

.. code-block:: bash

   poetry run pre-commit install

Each target calls ``poetry sync --extras "<extras>"`` which syncs the
``.venv`` in the project root to exactly the locked dependencies.

.. note:: Important Note
   :class: custom-note
   :name: make-dev

   Run ``make dev`` once after cloning and after every ``git pull`` that
   changes ``poetry.lock`` or ``pyproject.toml``.

----

Alternative: Mamba/Conda named environments (``setup_miniforge_venvs.sh``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For contributors who prefer named conda/mamba environments (one per install
mode), use the ``setup_miniforge_venvs.sh`` script.

Requirements:

- `miniforge3 <https://github.com/conda-forge/miniforge>`_ (mamba, micromamba, or conda)

.. code-block:: bash

   # Create a single named environment
   ./setup_miniforge_venvs.sh dev        # AodnCloudOptimised_dev (recommended)
   ./setup_miniforge_venvs.sh core       # AodnCloudOptimised_core
   ./setup_miniforge_venvs.sh notebooks  # AodnCloudOptimised_notebooks
   ./setup_miniforge_venvs.sh tests      # AodnCloudOptimised_tests
   ./setup_miniforge_venvs.sh docs       # AodnCloudOptimised_docs

   # Or create ALL environments at once
   ./setup_miniforge_venvs.sh all

After creation, activate the environment:

.. code-block:: bash

   mamba activate AodnCloudOptimised_dev
   pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: mamba-env

   The mamba env may need to be (re)activated after installation for all
   scripts to be available in ``$PATH``.

----

Core-only install (data processing, no dev tools)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you only need the core data-processing functionality (converting NetCDF/CSV
to Zarr/Parquet) without test infrastructure or notebook visualisation:

.. code-block:: bash

   # Using Makefile
   make core

   # Using setup script
   ./setup_miniforge_venvs.sh core

   # Direct Poetry
   poetry install

   # Direct pip (e.g., inside an existing environment)
   pip install aodn-cloud-optimised

This installs the minimum set: zarr, parquet, xarray, dask, coiled, and all
NetCDF/CSV processing dependencies. See :ref:`install` for the
automatic wheel install.

----

Project Pre-commit Integration
------------------------------

The project uses `pre-commit <https://pre-commit.com>`_ with hooks for
YAML/JSON validation, code quality, and dependency management.

.. code-block:: bash

   # After make dev or setup_miniforge_venvs.sh dev:
   pre-commit install      # or: poetry run pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: poetry-lock

    Ensure every commit triggers pre-commit hooks. If hooks do not trigger,
    reinstall with ``pre-commit install`` after reactivating your environment.

See below how pre-commit works when a JSON file is badly indented for example:

.. asciinema:: ../_static/recordings/precommit.cast
   :preload: 1
   :theme: solarized-dark
   :autoplay: true
   :speed: 0.80
   :cols: 100
   :rows: 30


Dependencies Update
-------------------

Poetry Commands and Their Use Cases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **``make dev``** (or ``poetry sync --extras "notebooks tests docs dev"``):
  Full re-sync of the development environment after any change to
  ``pyproject.toml`` or ``poetry.lock``.

- **``poetry lock``**:
  Update ``poetry.lock`` after modifying ``pyproject.toml``.

- **``poetry lock --regenerate``**:
  Fully regenerate ``poetry.lock`` from scratch.

.. note:: Important Note
   :class: custom-note
   :name: poetry-lock-issue

    Differences in ``poetry.lock`` can occur between local and CI environments
    due to platform-specific metadata. A helper script at the repository root
    runs ``poetry lock`` inside a Docker container for platform-agnostic
    lock-file generation.

Updating ``pyproject.toml``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Modify `pyproject.toml <https://github.com/aodn/aodn_cloud_optimised/blob/main/pyproject.toml>`_,
then run:

.. code-block:: bash

   poetry lock
   make dev        # re-sync your local environment

.. note:: Important Note
   :class: custom-note
   :name: poetry-lock-commit

    Don't forget to commit ``poetry.lock``.

.. note:: Important Note
   :class: custom-note
   :name: poetry-requirements

    When core dependencies change, update ``requirements.txt`` (used by the
    mamba environment and Coiled clusters). Stale ``requirements.txt`` can
    cause Coiled cluster spin-up failures.

    .. code-block:: bash

        poetry export -f requirements.txt --without-hashes -o requirements.txt

Handling Pre-commit Hook Issues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ensure ``poetry.lock`` is up-to-date using ``poetry lock``.

For more details refer to
`pyproject.toml <https://github.com/aodn/aodn_cloud_optimised/blob/main/pyproject.toml>`_.


Release
-------

Create a new release from the `releases page <https://github.com/aodn/aodn_cloud_optimised/releases>`_.

Click **Draft a new release** and create a new tag by incrementing the version
number. A GitHub Actions workflow will automatically build and upload the wheel.

.. note:: Important Note
   :class: custom-note
   :name: todo-release

    There is currently a minor issue with the release workflow: a second draft
    release is created. Delete it manually for now.
