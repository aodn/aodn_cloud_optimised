Installation
============

Users
-----

Requirements:
- python >= 3.10.14

Automatic Installation of Latest Wheel Release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   curl -s https://raw.githubusercontent.com/aodn/aodn_cloud_optimised/main/install.sh | bash

Otherwise, go to the `release page <http://github.com/aodn/aodn_cloud_optimised/releases/latest>`_.

Development
-----------

Option 1: Install with Mamba/Conda
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Requirements:
- Mamba from `miniforge3 <https://github.com/conda-forge/miniforge>`_

.. code-block:: bash

   mamba env create --file=environment.yml
   mamba activate CloudOptimisedParquet

   poetry install --with dev

Option 2: Create a Virtual Environment of Your Choice
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a virtual environment of your choice and activate it:

.. code-block:: bash

   pip install poetry
   poetry install --with dev
   #pip install -r requirements.txt  #
   #pre-commit install  # should be done by poetry install
