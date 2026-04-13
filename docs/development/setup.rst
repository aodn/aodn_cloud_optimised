.. _development-setup:

Development Setup
=================

Complete setup for contributors developing the ``aodn_cloud_optimised`` library.

Clone the Repository
--------------------

.. code-block:: bash

   gh repo clone aodn/aodn_cloud_optimised
   cd aodn_cloud_optimised

Install Development Environment
-------------------------------

See :ref:`install` for full installation options. For development, use:

.. code-block:: bash

   make dev

This creates a Poetry-managed virtual environment with all tools for testing, documentation, linting, and debugging.

After setup, install pre-commit hooks:

.. code-block:: bash

   poetry run pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: make-dev

   Run ``make dev`` once after cloning and after every ``git pull`` that
   changes ``poetry.lock`` or ``pyproject.toml``.

Pre-commit Hooks
----------------

The project uses `pre-commit <https://pre-commit.com>`_ with hooks for
YAML/JSON validation, code quality, and dependency management.

.. code-block:: bash

   # After make dev:
   pre-commit install      # or: poetry run pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: precommit-install

    If hooks do not trigger automatically, reinstall with ``pre-commit install``
    after reactivating your environment.

Example: pre-commit catching a badly indented JSON file:

.. asciinema:: ../_static/recordings/precommit.cast
   :preload: 1
   :theme: solarized-dark
   :autoplay: true
   :speed: 0.80
   :cols: 100
   :rows: 30

Managing Dependencies
---------------------

Poetry Commands and Their Use Cases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **``make dev``** (or ``poetry sync --extras "notebooks tests docs dev"``):
  Full re-sync of the development environment after any change to
  ``pyproject.toml`` or ``poetry.lock``.

- **``poetry lock``**:
  Update ``poetry.lock`` after modifying ``pyproject.toml``.

- **``poetry lock --regenerate``**:
  Fully regenerate ``poetry.lock`` from scratch.

.. note:: Important Note
   :class: custom-note
   :name: poetry-lock

    Ensure every commit triggers pre-commit hooks. If hooks do not trigger,
    reinstall with ``pre-commit install`` after reactivating your environment.

Updating pyproject.toml
~~~~~~~~~~~~~~~~~~~~~~~

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
        poetry export -f requirements.txt --without-hashes --extras notebooks -o notebooks/requirements.txt

Regenerating poetry.lock
~~~~~~~~~~~~~~~~~~~~~~~~

Differences in ``poetry.lock`` can occur between local and CI environments
due to platform-specific metadata. A helper script at the repository root
runs ``poetry lock`` inside a Docker container for platform-agnostic
lock-file generation.

.. note:: Important Note
   :class: custom-note
   :name: poetry-lock-issue

    Platform-specific differences in ``poetry.lock`` can cause CI failures.
    Use the Docker-based helper script (see ``poetry_lock_helper.sh``) for
    platform-agnostic regeneration.

Running Tests
-------------

.. code-block:: bash

   # Run all tests
   make tests

   # Or use pytest directly
   pytest test_aodn_cloud_optimised/

   # Run specific test file
   pytest test_aodn_cloud_optimised/test_generic_parquet_handler.py

   # Run with coverage
   pytest --cov=aodn_cloud_optimised test_aodn_cloud_optimised/

Building Documentation
----------------------

.. code-block:: bash

   make docs

Then open ``build/html/index.html`` in your browser.

See :ref:`documentation` for detailed documentation guidelines.

Next Steps
----------

- **Debug code**: See :ref:`debugging` for debugging techniques
- **Run tests**: See :ref:`testing` for test suite information
- **Update docs**: See :ref:`documentation` for documentation guidelines
- **Release**: See :ref:`release` for release procedures
