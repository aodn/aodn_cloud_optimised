Contributing
============

Clone the repository
--------------------

.. code-block:: bash

   gh repo clone aodn/aodn_cloud_optimised``
   cd aodn_cloud_optimised

Installation
------------

Requirements:

- python >= 3.10.14
- pip/uv


Option 1: Install with Mamba/Conda
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Requirements:

- Mamba from `miniforge3 <https://github.com/conda-forge/miniforge>`_

.. code-block:: bash

   mamba env create --file=environment.yml
   mamba activate CloudOptimisedParquet

   poetry install --with dev
   pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: mamba-env

   the conda/mamba env might have to be (re) activated after installation to have all the scripts available in the shell $PATH

Option 2: Installation using Poetry's virtual environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a virtual environment of your choice and activate it:

.. code-block:: bash
   curl -sSL https://install.python-poetry.org | python3 -

   poetry shell
   poetry install --with dev
   pre-commit install

.. note:: Important Note
   :class: custom-note
   :name: poetry-env

   Using this method, the poetry virtual env will have to be (re) activated after being installed with ``poetry shell``
   once in the directory

Project Pre-commit Integration
------------------------------

The project utilises `pre-commit <https://pre-commit.com>`_ with various hooks for validating YAML/JSON files, ensuring good coding practices, and managing Python module dependencies.



.. note:: Important Note
   :class: custom-note
   :name: poetry-lock

    Ensure that every commit triggers pre-commit hooks. If hooks do not trigger, you may need to install pre-commit or reactivate your Python environment
    with ``pre-commit install``

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

When managing dependencies with Poetry, it's crucial to use the right commands to address issues like outdated dependencies or pre-commit hook failures.

Poetry Commands and Their Use Cases:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **`poetry install --with dev`**:
  Use this command to install both main and development dependencies.

- **`poetry lock`**:
  Use `poetry lock` to update `poetry.lock` based on changes in `pyproject.toml`.

- **`poetry lock --no-update`**:
  Use `poetry lock --no-update` to regenerate `poetry.lock` without updating dependencies.

Updating `pyproject.toml`:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Modify `pyproject.toml <https://github.com/aodn/aodn_cloud_optimised/blob/main/pyproject.toml>`_
, then run ``poetry install`` followed by ``poetry lock``.

.. note:: Important Note
   :class: custom-note
   :name: poetry-lock

    Don't forget to add and commit ``poetry.lock``

.. note:: Important Note
   :class: custom-note
   :name: poetry-requirements

    When modules are updated, it can be good practice to also update the ``requirements.txt`` file used to create the Mamba env.
    It is not done automatically yet.

    .. code-block:: bash

        poetry export -f requirements.txt --without-hashes -o requirements.txt

Handling Pre-commit Hook Issues:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ensure `poetry.lock` is up-to-date using ``poetry lock`` or ``poetry lock --no-update``.

For more details, refer to the `pyproject.toml` file in your repository:
`pyproject.toml <https://github.com/aodn/aodn_cloud_optimised/blob/main/pyproject.toml>`_.


Release
-------
Create a new release by going to the release page `here <https://github.com/aodn/aodn_cloud_optimised/releases>`_.

Click on ``Draft a new release`` and create a new tag by incrementing the version number.

A Github Action workflow will automatically be triggered and build a new wheel and upload it to the latest release.

.. note:: Important Note
   :class: custom-note
   :name: todo-release

    There is currently a minor issue with the release workflow. A second draft release is being created.
    This draft release needs to be deleted manually for now
