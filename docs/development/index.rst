.. _development:

Development
===========

This section is for contributors. If you're just using the library, see :ref:`getting-started` and :ref:`how-to`.

.. toctree::
   :maxdepth: 2
   :caption: For contributors:

   setup
   debugging
   testing
   documentation
   release

**Quick start for developers**

1. Clone and install: ``git clone ... && make dev``
2. Run pre-commit: ``poetry run pre-commit install``
3. See :ref:`development-setup` for full setup options
4. Run tests: ``make tests`` or ``pytest test_aodn_cloud_optimised/``
5. Build docs: ``make docs`` then open ``build/html/index.html``
6. Debug code: Use ``import ipdb; ipdb.set_trace()`` (see :ref:`debugging`)
7. Before pushing: ``poetry run pre-commit run --all-files``

**Contributing workflow**

1. Create a feature branch: ``git checkout -b feature/my-feature``
2. Make changes and test locally: ``make tests``
3. Update documentation if needed: :ref:`documentation`
4. Run pre-commit: ``poetry run pre-commit run --all-files``
5. Submit a pull request

See individual guides below for detailed information on each task.
