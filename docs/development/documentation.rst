.. _documentation:

Building Documentation
======================

The documentation is automatically built by `ReadTheDocs <https://about.readthedocs.com>`_

To build and preview documentation locally before committing:

.. code-block:: bash

    make docs

Then open ``build/html/index.html`` in your browser.

**Or, manually with Sphinx**:

.. code-block:: bash

    sphinx-build -b html docs/ build/html

Documentation Structure
-----------------------

The documentation is organized into four main sections:

- **Getting Started** (`docs/getting-started/`): Installation and quick start
- **How-To Guides** (`docs/how-to/`): Task-oriented guides for users and operators
- **Development** (`docs/development/`): Setup, debugging, testing, release procedures
- **Module Reference** (`docs/module-overview.rst`, `docs/usage.rst`): API reference

File locations:

- Jupyter notebooks and dataset examples: ``docs/notebooks/``
- Static assets (images, recordings): ``docs/_static/``
- Sphinx configuration: ``docs/conf.py``

Documentation Standards
-----------------------

- **Format**: reStructuredText (.rst)
- **Sphinx extensions**: autosummary, napoleon (Google docstrings), asciinema
- **Cross-references**: Use ``.. _label:`` and ``:ref:\`label\```` for internal links
- **Code blocks**: Use ``.. code-block:: language`` with proper indentation
- **Notes**: Use ``.. note::`` directive for important information

Docstring Style
~~~~~~~~~~~~~~~

Python docstrings use Google style (configured in ``conf.py``):

.. code-block:: python

    def my_function(param1, param2):
        """Short description.

        Longer description if needed.

        Args:
            param1: Description of param1
            param2: Description of param2

        Returns:
            Description of return value

        Raises:
            ValueError: When something is wrong
        """
        pass

See the Sphinx configuration in ``docs/conf.py`` for details.

Common Tasks
------------

**Add a new page to the documentation:**

1. Create a new `.rst` file in the appropriate directory
2. Add a label at the top: ``.. _my-page-label:``
3. Add it to the parent's ``toctree`` in ``index.rst``
4. Run ``make docs`` to verify
5. Commit and push

**Update cross-references:**

If you move or rename a page, update all ``:ref:`` links that point to it.
Use ``grep -r "ref.*old-label" docs/`` to find references.

**Add images:**

Store images in ``docs/_static/`` and reference with:

.. code-block:: rst

    .. image:: _static/my-image.png
       :alt: Description

**Embed asciinema recordings:**

Place recordings in ``docs/_static/recordings/`` and use:

.. code-block:: rst

    .. asciinema:: _static/recordings/my-recording.cast
       :preload: 1
       :theme: solarized-dark
       :autoplay: true
       :speed: 0.80

**Check for broken links and warnings:**

.. code-block:: bash

    sphinx-build -b html -W --keep-going docs/ build/html

The ``-W`` flag treats warnings as errors, ``--keep-going`` continues past first error.

ReadTheDocs Integration
-----------------------

The project uses `ReadTheDocs <https://readthedocs.org>`_ for continuous documentation builds.

Configuration is in ``.readthedocs.yaml`` at the repository root.

Every push to main automatically rebuilds and publishes at:
https://aodn-cloud-optimised.readthedocs.io/

**To preview documentation before merge:**

ReadTheDocs builds pull requests automatically. Check the PR for a "ReadTheDocs" status.
Click "Details" to preview the built docs.

Tips
~~~~

- When adding new dependencies (e.g., packages that must be imported in examples), update ``docs/requirements.txt``.
- If builds fail on ReadTheDocs but pass locally, check for platform-specific issues or missing dependencies.
- ReadTheDocs runs in a clean environment, so ensure all imports in examples are listed in requirements.
