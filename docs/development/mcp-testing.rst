.. _mcp-testing:

Testing the MCP Server
======================

The MCP server ships with a dedicated integration test suite in
``integration_testing/test_mcp_server.py``.  All tests call the MCP tool
functions directly (not via the protocol transport) so they exercise exactly
the same code paths an AI assistant uses — without requiring a running server
process.

The suite is **excluded from the default** ``pytest`` run (``--ignore=integration_testing``
in ``pyproject.toml``) because some tests make live S3 requests or execute full
Jupyter notebooks, which would be too slow and network-dependent for a standard
CI run.

Prerequisites
-------------

Install the ``mcp`` optional dependency group before running:

.. code-block:: bash

    pip install -e ".[mcp]"
    # or with poetry:
    poetry install --extras mcp

Test Tiers and CLI Flags
------------------------

Three custom flags control which test tiers are enabled.

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Flag
     - Mark applied
     - What it enables
   * - *(none)*
     - —
     - Offline tests only (catalog, plot guide, Python REPL). No network
       access required. Runs in ~8 s.
   * - ``--run-s3``
     - ``@pytest.mark.s3``
     - Adds live anonymous S3 queries: ``check_dataset_coverage`` and
       ``introspect_dataset_live`` against the real AODN public bucket.
       Requires internet access (~10–60 s per test).
   * - ``--run-notebooks``
     - ``@pytest.mark.notebooks``
     - Adds notebook tests (S3 also required):

       **Synthetic unit tests** (``TestMcpValidateNotebook``, fast): verify
       ``validate_notebook`` itself works on tiny synthetic notebooks.

       **Scripted-agent generation** (``TestMcpGeneratedNotebook``): calls
       ``get_dataset_schema``, ``check_dataset_coverage``, and
       ``execute_python_cell`` in sequence, assembles a NEW ``.ipynb``, and
       validates it.  ~5–10 min.

       **Real user scenarios** (``TestMcpEndToEnd``): 5 tests driven by real
       user questions (Coffs Harbour, SA Gulfs, Argo Coral Sea, mooring near
       Sydney, SST Southern Ocean).  Each chains the full MCP tool sequence and
       generates + validates a fresh notebook.  ~10–20 min.
   * - ``--run-all``
     - both
     - Short-hand for ``--run-s3`` + ``--run-notebooks`` combined.

Marked tests are **automatically skipped** unless the corresponding flag is
passed — there is no need to use ``-m`` expressions.

Running the Tests
-----------------

.. code-block:: bash

    # Fast offline tests (catalog, plot-guide, Python REPL) — no network needed:
    pytest integration_testing/test_mcp_server.py -v

    # Include live S3 queries:
    pytest integration_testing/test_mcp_server.py -v --run-s3

    # Scripted-agent notebook generation (also needs --run-s3):
    pytest integration_testing/test_mcp_server.py -v --run-s3 --run-notebooks

    # Run everything:
    pytest integration_testing/test_mcp_server.py -v --run-all

Test Classes
------------

.. list-table::
   :header-rows: 1
   :widths: 35 10 55

   * - Class
     - Tests
     - Description
   * - ``TestMcpCatalogOffline``
     - 13
     - ``list_datasets``, ``search_datasets``, ``get_dataset_info``,
       ``get_dataset_schema``, ``get_dataset_config``. Includes parent/child
       config inheritance assertions (e.g. radar child inheriting schema from
       parent via ``load_dataset_config``).
   * - ``TestMcpNotebookOffline``
     - 8
     - ``get_notebook_template``, ``get_plot_guide`` (parquet, zarr, radar),
       ``get_dataquery_reference``. Verifies safe-date helpers, xarray
       anti-pattern warnings, and standalone function signatures.
   * - ``TestMcpExecutePython``
     - 8
     - ``execute_python_cell``: success output, error tracebacks, magic
       stripping, session persistence, session isolation, pre-populated
       symbols (``GetAodn``, ``plot_ts_diagram``), timeout handling.
   * - ``TestMcpLiveS3``
     - 7
     - ``check_dataset_coverage`` and ``introspect_dataset_live`` against
       real S3. Verifies Argo coverage inside/outside Australia, SST zarr
       coverage, ``JULD`` vs ``TIME`` in Argo, and the ``wind_speed`` ⚠️
       flag (present in JSON config but absent from the live Zarr store).
   * - ``TestMcpValidateNotebook``
     - 2
     - ``validate_notebook`` on synthetic ``.ipynb`` files: clean notebook
       returns only ✅ cells; notebook with ``1/0`` returns ❌ + traceback.
   * - ``TestMcpGeneratedNotebook``
     - 2
     - **Scripted-agent notebook generation.** Simulates an AI workflow for
       Argo (parquet) and SST (zarr): calls ``get_dataset_schema`` to discover
       real variable names (e.g. ``JULD`` not ``TIME``), calls
       ``check_dataset_coverage`` to confirm data exists, uses
       ``execute_python_cell`` to iteratively test each code snippet, then
       assembles a new ``.ipynb`` and validates it with ``validate_notebook``.
       Fails if the schema tool returns wrong variable names, proving the
       MCP tools produce working notebooks.  Requires ``--run-s3 --run-notebooks``
       (or ``--run-all``).
   * - ``TestMcpEndToEnd``
     - 5
     - Five real user-question scenarios.  Each chains the full MCP tool
       sequence (``search_datasets`` → ``check_dataset_coverage`` →
       ``get_dataset_schema`` → ``execute_python_cell`` →
       ``validate_notebook``) and generates a fresh ``.ipynb``.
       See :ref:`mcp-e2e-scenarios` below.

.. _mcp-e2e-scenarios:

End-to-End Scenarios
--------------------

Each scenario encodes a real user question.  The test chains MCP tools in
the same order an AI assistant would, builds a new notebook from the tool
outputs, and validates it executes cleanly — no pre-existing notebooks required.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Test / User question
     - MCP tool chain
   * - **Coffs Harbour Jan 2020 sea state**
     - *"A notebook showing radar data at Coffs Harbour in January 2020,
       compare with Argo and gridded SST."*
       search → coverage (argo + SST) → schema (discovers ``JULD``) →
       REPL (load + query both) →
       generates ``generated_coffs_harbour.ipynb`` → validate.
   * - **SA Gulfs HAB — April, chlorophyll**
     - *"Explore AODN datasets in the HAB area (SA Gulfs 134–141.5°E,
       34–39.5°S). Compare datasets across April. Add chlorophyll."*
       search (radar, chlorophyll, argo) → coverage → schema → REPL →
       generates ``generated_sa_gulfs.ipynb`` → validate.
   * - **Argo Coral Sea 2018**
     - *"Argo float T/S profiles in the Coral Sea in 2018."*
       schema (confirms ``JULD``) → coverage → REPL →
       generates ``generated_argo_coral_sea.ipynb`` → validate.
   * - **Mooring temperature near Sydney**
     - *"Mooring temperature near Sydney 2018–2022, include T/S diagram."*
       search → schema (``TIME``, ``TEMP``) → coverage → REPL →
       generates ``generated_mooring_sydney.ipynb`` → validate.
   * - **SST Southern Ocean Dec–Feb**
     - *"SST anomaly in the Southern Ocean during austral summer."*
       search → schema → coverage → REPL →
       generates ``generated_sst_southern_ocean.ipynb`` → validate.


Adding New Tests
----------------

To add a test for a new dataset or scenario:

1. Import the MCP tool functions at the top of ``test_mcp_server.py``::

       from aodn_cloud_optimised.mcp.server import check_dataset_coverage, ...

2. Subclass ``_McpAgentMixin, unittest.TestCase`` and decorate with
   ``@pytest.mark.s3`` + ``@pytest.mark.notebooks``.

3. Follow the 5-step pattern: search → coverage → schema → REPL test →
   ``_make_notebook`` + ``validate_notebook``.  **Do not call boto3, xarray,
   or pandas directly** — use only MCP tool functions.
