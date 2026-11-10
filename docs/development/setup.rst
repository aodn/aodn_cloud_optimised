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

This creates a uv-managed virtual environment (``.venv``) with all tools for testing, documentation, linting, and debugging.

After setup, install pre-commit hooks:

.. code-block:: bash

   uv run pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: make-dev

   Run ``make dev`` once after cloning and after every ``git pull`` that
   changes ``uv.lock`` or ``pyproject.toml``.

Pre-commit Hooks
----------------

The project uses `pre-commit <https://pre-commit.com>`_ with hooks for
YAML/JSON validation, code quality, and dependency management.

.. code-block:: bash

   # After make dev:
   pre-commit install      # or: uv run pre-commit install

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

uv Commands and Their Use Cases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **``make dev``** (or ``uv sync --extra notebooks --extra tests --extra docs --extra dev --extra mcp``):
  Full re-sync of the development environment after any change to
  ``pyproject.toml`` or ``uv.lock``.

- **``uv lock``**:
  Update ``uv.lock`` after modifying ``pyproject.toml`` (keeps existing pins where possible).

- **``uv lock --upgrade``** (or ``--upgrade-package <name>``):
  Re-resolve to the latest allowed versions, all packages or just one.

.. note:: Important Note
   :class: custom-note
   :name: uv-lock

    Ensure every commit triggers pre-commit hooks. If hooks do not trigger,
    reinstall with ``pre-commit install`` after reactivating your environment.

Updating pyproject.toml
~~~~~~~~~~~~~~~~~~~~~~~

Modify `pyproject.toml <https://github.com/aodn/aodn_cloud_optimised/blob/main/pyproject.toml>`_,
then run:

.. code-block:: bash

   uv lock
   make dev        # re-sync your local environment

.. note:: Important Note
   :class: custom-note
   :name: uv-lock-commit

    Don't forget to commit ``uv.lock``. The ``uv-lock`` pre-commit hook and CI
    fail if it is out of date.

.. note:: Important Note
   :class: custom-note
   :name: uv-requirements

    When core dependencies change, update ``requirements.txt`` (used by the
    mamba environment and Coiled clusters). Stale ``requirements.txt`` can
    cause Coiled cluster spin-up failures.

    .. code-block:: bash

        uv export --no-dev --no-hashes --no-emit-project -o requirements.txt
        uv export --no-dev --no-hashes --no-emit-project --extra notebooks -o notebooks/requirements.txt

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
