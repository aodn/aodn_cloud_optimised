.. include:: development

.. _install:

Quick Installation
==================


Requirements:
- python >= 3.11

Automatic Installation of Latest Wheel Release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Run the following **only** if you're not planning on doing any development!

.. code-block:: bash

   curl -s https://raw.githubusercontent.com/aodn/aodn_cloud_optimised/main/install.sh | bash

Otherwise, go to :ref:`install-doc`

Core-only install (data processing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To install just the core data-processing functionality (Zarr/Parquet
conversion, no notebooks or test tooling):

From source:

.. code-block:: bash

   git clone https://github.com/aodn/aodn_cloud_optimised.git
   cd aodn_cloud_optimised
   pip install .       # core only
   # or: make core     # via Poetry venv

See :ref:`install-doc` for the full contributor setup.
