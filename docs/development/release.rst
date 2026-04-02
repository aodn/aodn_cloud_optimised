.. _release:

Creating a Release
==================

Releases are fully automated via the **Release** GitHub Actions workflow
(:file:`.github/workflows/release.yml`).  A single ``workflow_dispatch`` trigger
handles everything: running tests, bumping the version, building wheels, creating
the Git tag, and publishing the GitHub Release — with no manual tag creation needed.

.. note::

   The old approach required creating a Git tag manually, which caused two GitHub
   Releases to be created (one from the tag push, one from the workflow).  The new
   approach avoids this by having the workflow create the tag itself.

How to trigger a release
-------------------------

1. Go to the repository on GitHub and open **Actions → Release**.
2. Click **Run workflow** (top-right of the workflow runs list).
3. Select the target branch (usually ``main``).
4. Choose the **bump type**:

   .. list-table::
      :header-rows: 1
      :widths: 15 85

      * - Option
        - Effect
      * - ``patch``
        - Increments the last number, e.g. ``1.2.3`` → ``1.2.4`` (bug fixes)
      * - ``minor``
        - Increments the middle number and resets patch, e.g. ``1.2.3`` → ``1.3.0``
      * - ``major``
        - Increments the first number and resets the rest, e.g. ``1.2.3`` → ``2.0.0``

5. Click **Run workflow**.

What the workflow does
----------------------

The workflow executes the following steps automatically:

1. **Checkout** — full history (``fetch-depth: 0``) so the tag push works.
2. **Install dependencies** — Poetry is installed at the version pinned in
   :file:`.poetry-version`; project deps are installed via ``make tests``
   (equivalent to ``poetry sync --extras "tests notebooks"``).
3. **Run tests** — full pytest suite must pass before any release artefact is built.
4. **Bump version** — the ``version`` field in :file:`pyproject.toml` is updated
   in-place by a small Python script.
5. **Build wheels** — two artefacts are produced in ``dist-artifacts/``:

   .. list-table::
      :header-rows: 1
      :widths: 40 60

      * - File
        - Description
      * - ``aodn_cloud_optimised-<ver>-py3-none-any.whl``
        - **Frozen wheel** — all dependency versions pinned in wheel metadata via
          `poetry-plugin-freeze <https://github.com/poetry-plugins/poetry-plugin-freeze>`_.
          Use this for reproducible installs.
      * - ``aodn_cloud_optimised-<ver>.unfrozen.whl``
        - **Unfrozen wheel** — standard wheel with flexible dependency ranges
          (as declared in :file:`pyproject.toml`).  Use this when you want pip
          to resolve the latest compatible versions.
      * - ``aodn_cloud_optimised-<ver>.tar.gz``
        - Source distribution (sdist).

6. **Verify** — the frozen wheel is installed into the Poetry venv with
   ``--no-deps --force-reinstall`` and a smoke-import is run.

   .. note::

      ``--no-deps`` is required because the project uses a custom Git fork of
      ``xarray`` that is not available on PyPI.  All dependency resolution happens
      via :file:`poetry.lock`.

7. **Commit & tag** — the workflow commits the updated :file:`pyproject.toml`,
   creates the ``v<version>`` Git tag, and pushes both to the target branch.
8. **GitHub Release** — ``gh release create`` publishes a new release with
   auto-generated notes and all three artefacts attached.

.. tip::

   Steps 7 and 8 are guarded with ``if: ${{ !env.ACT }}``, so they are
   **skipped automatically** when testing the workflow locally with
   `act <https://github.com/nektos/act>`_.  You can test the full pipeline
   (everything except the push and release) with:

   .. code-block:: bash

      act workflow_dispatch \
        -W .github/workflows/release.yml \
        --input bump_type=patch \
        --artifact-server-path ./artifacts

Installing a released wheel
----------------------------

.. code-block:: bash

   # Frozen wheel (pinned deps) — recommended for production
   pip install aodn-cloud-optimised --no-deps

   # Or install a specific wheel from the GitHub Release assets:
   pip install https://github.com/aodn/aodn_cloud_optimised/releases/download/v<version>/aodn_cloud_optimised-<version>-py3-none-any.whl --no-deps

Updating the version manually (if needed)
------------------------------------------

If you ever need to bump the version without triggering a full release
(e.g. to fix a pre-release commit), edit :file:`pyproject.toml` directly:

.. code-block:: toml

   [project]
   version = "1.2.4"   # ← change this

Then commit with a ``chore:`` prefix:

.. code-block:: bash

   git commit -m "chore: bump version to v1.2.4"
