Notebooks
=========

Notebooks are available at:

`Notebooks Repository <https://github.com/aodn/aodn_cloud_optimised/tree/main/notebooks>`_

When creating a new dataset, it is mandatory create a notebook to showcase how to use it.

A template for parquet dataset is provided at `template <https://github.com/aodn/aodn_cloud_optimised/blob/main/notebooks/template_parquet.ipynb>`_

This notebook will then added to the AWS OpenData registry configuration.

The notebooks should also work by being directly imported into Google Colab:

`Notebooks on Google Colab <https://github.com/aodn/aodn_cloud_optimised/blob/main/notebooks/README.md>`_


.. note:: Important Note
   :class: custom-note
   :name: notebooks

    Notebooks MUST have the same name as the dataset, so that they can automatically be added to the AWS OpenData
    registry yaml file, by the the ``cloud_optimised_create_aws_registry_dataset`` script.
