MCP Server
==========

The ``aodn_cloud_optimised`` package ships an optional **MCP (Model Context Protocol)
server** that exposes the AODN dataset catalog, schema definitions, and Jupyter
notebook templates to AI assistants such as `Claude Desktop
<https://claude.ai/download>`_.

The AI can use the server to:

1. **Discover** datasets relevant to a user request (e.g. "mooring temperature near Sydney").
2. **Inspect** schema variables, CF attributes, and S3 location for a specific dataset.
3. **Retrieve** the canonical Jupyter notebook template for that dataset.
4. **Adapt** the template notebook — adding location filters, date ranges, or
   custom plots — based on the user's specific needs.

All catalog information is built from the local ``config/dataset/*.json`` files
shipped with the package. **No S3 calls or credentials are required to start the
server.**

Installation
------------

The MCP server requires the optional ``mcp`` extra::

    pip install aodn-cloud-optimised[mcp]

Or, from the source tree::

    pip install -e ".[mcp]"

Starting the Server
-------------------

Run the entry point directly::

    aodn-mcp-server

The server speaks the MCP protocol over *stdio* and is designed to be launched
by an MCP client. **Do not run it directly in a terminal** — stdin becomes the
JSON-RPC channel, so any keyboard input will appear as malformed JSON to the server.

.. note::

   To verify the server is working you can use the MCP inspector::

       npx @modelcontextprotocol/inspector aodn-mcp-server

Gemini CLI (Linux / Ubuntu)
------------------------------------------

`Gemini CLI <https://github.com/google-gemini/gemini-cli>`_ reads MCP server
configuration from ``~/.gemini/settings.json`` (user-wide) or
``.gemini/settings.json`` in your project directory (project-specific, takes
precedence).

.. important::

   **Use the full absolute path to ``aodn-mcp-server``** in your MCP config.
   AI CLI tools (Copilot CLI, Gemini CLI) spawn MCP servers in a bare
   environment that does **not** inherit your shell's ``PATH`` or conda
   activation.  If you use just ``"command": "aodn-mcp-server"``, the client
   will fail with ``ENOENT`` (file not found).

   Find the correct path with::

       which aodn-mcp-server
       # e.g. /home/<your-user>/miniforge3/envs/AodnCloudOptimised/bin/aodn-mcp-server

Create or edit ``~/.gemini/settings.json``:

.. code-block:: json

    {
      "mcpServers": {
        "aodn": {
          "command": "/home/<your-user>/miniforge3/envs/<env>/bin/aodn-mcp-server",
          "env": {
            "AODN_NOTEBOOKS_PATH": "/home/<your-user>/aodn_cloud_optimised/notebooks",
            "AODN_CONFIG_PATH": "/home/<your-user>/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
          },
          "trust": true
        }
      }
    }

Replace ``<your-user>`` and ``<env>`` with your username and conda environment
name.  The ``trust: true`` flag skips confirmation dialogs for each tool call —
remove it if you prefer to approve each action.

Once saved, start Gemini CLI and use ``/mcp`` to verify the server is listed and
connected. You can then prompt it naturally, for example::

    Give me a notebook for mooring temperature data near Sydney between 2020 and 2023.

GitHub Copilot CLI (Linux)
---------------------------

`GitHub Copilot CLI <https://docs.github.com/copilot/concepts/agents/about-copilot-cli>`_
stores its MCP configuration in ``~/.copilot/mcp-config.json`` (the directory
can be changed with the ``COPILOT_HOME`` environment variable).

**Option A — interactive setup** (recommended for first-time setup):

Start the CLI and run::

    /mcp add

Fill in the server details using :kbd:`Tab` to move between fields, then press
:kbd:`Ctrl+S` to save.

**Option B — direct JSON editing**:

Create or edit ``~/.copilot/mcp-config.json``:

.. code-block:: json

    {
      "mcpServers": {
        "aodn": {
          "type": "stdio",
          "command": "/home/<your-user>/miniforge3/envs/<env>/bin/aodn-mcp-server",
          "env": {
            "AODN_NOTEBOOKS_PATH": "/home/<your-user>/aodn_cloud_optimised/notebooks",
            "AODN_CONFIG_PATH": "/home/<your-user>/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
          },
          "tools": ["*"]
        }
      }
    }

``"tools": ["*"]`` enables all tools.  You can restrict it to a subset, for
example ``["search_datasets", "get_dataset_info", "get_notebook_template"]``.

Once configured, restart the CLI. Use ``/mcp`` to confirm the ``aodn`` server
is listed. The server tools are available automatically in any session — just
prompt naturally::

    Give me a notebook for mooring temperature data near Sydney between 2020 and 2023.

.. note::

   **Tool name prefixing (Copilot CLI v1.0.x):**
   Copilot CLI may call MCP tools as shell commands prefixed with the server
   key, e.g. ``aodn-search_datasets "mooring temperature"``.
   The package registers a standalone executable for every tool so these calls
   succeed without any additional configuration:

   .. code-block:: bash

       aodn-search_datasets "wave buoy Tasmania"
       aodn-list_datasets --format parquet
       aodn-get_dataset_info argo.parquet
       aodn-get_dataset_schema satellite_ghrsst_l3s_1d_nrt
       aodn-check_dataset_coverage argo \
           --lat-min -45 --lat-max -10 --lon-min 140 --lon-max 155 \
           --date-start 2020-01-01 --date-end 2020-12-31
       aodn-introspect_dataset_live argo.parquet
       aodn-get_notebook_template argo.parquet
       aodn-get_plot_guide argo.parquet
       aodn-get_dataquery_reference

   All executables accept ``--help`` for a usage summary.

GitHub Copilot in VS Code (Linux)
-----------------------------------

GitHub Copilot's **Agent Mode** supports MCP servers from VS Code 1.99+. You need
the `GitHub Copilot <https://marketplace.visualstudio.com/items?itemName=GitHub.copilot>`_
extension and agent mode enabled.

**Option A — Workspace config** (repo-specific, checked into version control):

Create ``.vscode/mcp.json`` at the root of your project:

.. code-block:: json

    {
      "servers": {
        "aodn": {
          "type": "stdio",
          "command": "/home/<your-user>/miniforge3/envs/<env>/bin/aodn-mcp-server",
          "env": {
            "AODN_NOTEBOOKS_PATH": "${workspaceFolder}/notebooks",
            "AODN_CONFIG_PATH": "${workspaceFolder}/aodn_cloud_optimised/config/dataset"
          }
        }
      }
    }

``${workspaceFolder}`` expands to the repo root automatically — no hard-coded
paths needed when working from the cloned repository.

**Option B — User/global config** (applies to all workspaces):

Open VS Code user settings (``Ctrl+,`` → "Open Settings JSON") and add:

.. code-block:: json

    "mcp.servers": {
      "aodn": {
        "type": "stdio",
        "command": "/home/<your-user>/miniforge3/envs/<env>/bin/aodn-mcp-server",
        "env": {
          "AODN_NOTEBOOKS_PATH": "/home/<your-user>/aodn_cloud_optimised/notebooks",
          "AODN_CONFIG_PATH": "/home/<your-user>/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
        }
      }
    }

**Option C — CLI one-liner**::

    code --add-mcp '{"name":"aodn","type":"stdio","command":"aodn-mcp-server","env":{"AODN_NOTEBOOKS_PATH":"/home/<your-user>/aodn_cloud_optimised/notebooks","AODN_CONFIG_PATH":"/home/<your-user>/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"}}'

After configuration, open the Copilot Chat panel, switch to **Agent Mode**
(``@workspace`` → Agent), then press ``Ctrl+Shift+P`` and run
``MCP: List Servers`` to confirm ``aodn`` is listed and started.

.. note::

   Ensure agent mode is enabled in VS Code settings::

       "chat.agent.enabled": true

Claude Desktop Configuration (macOS / Windows)
------------------------------------------------

Edit the Claude Desktop configuration file:

- **macOS:** ``~/Library/Application Support/Claude/claude_desktop_config.json``
- **Windows:** ``%APPDATA%\Claude\claude_desktop_config.json``

.. code-block:: json

    {
      "mcpServers": {
        "aodn": {
          "command": "/home/<your-user>/miniforge3/envs/<env>/bin/aodn-mcp-server",
          "env": {
            "AODN_NOTEBOOKS_PATH": "/path/to/aodn_cloud_optimised/notebooks",
            "AODN_CONFIG_PATH": "/path/to/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
          }
        }
      }
    }

Replace the paths with the absolute paths in your cloned repository. If you
installed from a wheel that includes notebooks, you can omit the env variables.

Environment Variables
---------------------

.. envvar:: AODN_NOTEBOOKS_PATH

   Absolute path to the directory containing AODN Jupyter notebooks (``*.ipynb``).
   If not set, the server attempts to auto-detect the ``notebooks/`` directory
   relative to the package source tree. Set this variable explicitly when running
   the server from a non-standard install location.

.. envvar:: AODN_CONFIG_PATH

   Absolute path to the directory containing AODN dataset JSON config files
   (``*.json``). These files define the schema, CF variable attributes,
   partitioning strategy, and S3 source paths for each dataset, and share the same
   base filename as their corresponding notebook (e.g.
   ``mooring_temperature_logger_delayed_qc.json`` ↔
   ``mooring_temperature_logger_delayed_qc.ipynb``).

   If not set, the server loads configs from the installed package via
   ``importlib.resources``. Set this variable to use configs from a local clone
   or a custom location.

Available MCP Tools
-------------------

Once connected, an AI assistant has access to the following tools:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Tool
     - Description
   * - ``list_datasets``
     - List all available AODN datasets. Supports optional filters for format
       (``parquet`` / ``zarr``) and dataset name prefix.
   * - ``search_datasets``
     - Fuzzy keyword search across dataset names, AWS registry descriptions, and
       CF variable attributes (``standard_name``, ``long_name``).
   * - ``get_dataset_info``
     - Full metadata for a specific dataset: description, S3 ARN, all schema
       variables with CF attributes, and partitioning strategy.
   * - ``get_dataset_schema``
     - **Authoritative variable listing** — call this before writing any notebook
       code. Returns every schema variable with its exact column name, CF role
       (``TIME_AXIS``, ``LAT``, ``LON``, ``DEPTH``, ``DATA``), type, units,
       ``standard_name``, and ``long_name``. Also includes the inferred data type
       (timeseries, profiles, gridded, radar, etc.), an AWS description excerpt,
       and recommended ``DataQuery`` methods for the dataset's format.
   * - ``get_dataset_summary``
     - **Single-call dataset profile** — returns everything an AI needs to USE a
       dataset: name, format, data type classification, full AWS description,
       coordinate and data variable tables, matching notebook path, and
       ready-to-use code patterns.  This replaces calling ``get_dataset_schema`` +
       ``get_dataset_info`` + ``get_dataset_config`` + ``get_notebook_template``
       separately.
   * - ``check_dataset_coverage``
     - **Live S3 coverage query** — makes anonymous S3 requests to determine the
       dataset's actual temporal extent (first/last timestamp), spatial bounding
       box, and key global metadata attributes (title, institution, summary,
       licence, etc.).  Accepts optional ``lat_min / lat_max / lon_min / lon_max
       / date_start / date_end`` filters; when supplied, reports ✅/❌ overlap
       verdicts so the AI can confirm a dataset really covers the area and era
       the user needs before recommending it.  Parquet datasets use fast
       partition-key scanning; Zarr datasets read the time and coordinate arrays
       directly.
   * - ``introspect_dataset_live``
     - **Real variable introspection from the live S3 store.** Unlike
       ``get_dataset_schema`` (which reads the JSON config), this tool opens the
       actual dataset and returns what is truly there.  For Zarr datasets it
       lists every ``data_var`` with dimensions, shape, dtype, units, and
       long_name; for Parquet it reads the embedded pyarrow schema.  It also
       cross-checks JSON config variables against the live store and flags any
       that are listed in the config but **absent** from the store (e.g.
       ``wind_speed`` in the GHRSST dataset).  Always call this before writing
       code that accesses individual variable names in a Zarr store.
   * - ``validate_notebook``
     - **Run a notebook cell by cell and report errors.** Uses
       ``nbconvert``'s ``ExecutePreprocessor`` to execute every code cell in the
       current Python environment.  Returns a per-cell ✅ / ❌ / ⏱️ table with
       full error tracebacks for failed cells.  The AI is expected to fix every
       ❌ cell and re-validate until the notebook executes cleanly before
       delivering it to the user.  ``cell_timeout`` (default 120 s) can be
       increased for notebooks with heavy S3 data downloads.
   * - ``execute_python_cell``
     - **Interactive Python REPL for per-cell testing.** Runs a code snippet in
       a persistent, named session (``session_id``) so that variables survive
       between calls — exactly like a running Jupyter kernel.  Pre-populates
       every session with ``GetAodn`` and ``plot_ts_diagram`` from
       ``DataQuery.py``.  Strips Jupyter magic commands (``%%time``,
       ``%matplotlib``) automatically.  Use this to test each notebook cell
       **before** writing it to the ``.ipynb`` file; never deliver a notebook
       whose cells have not been verified here or by ``validate_notebook``.
   * - ``get_dataset_config``
     - Full raw JSON config for a specific dataset (complete ``schema``,
       ``schema_transformation``, ``run_settings``, ``aws_opendata_registry``).
       Useful when the AI needs unabridged variable definitions or source path
       details. Config files share the same stem as their matching notebook.
       Child configs that extend a ``parent_config`` are automatically merged.
   * - ``get_notebook_template``
     - Returns the canonical Jupyter notebook for a dataset as readable text.
       Falls back to a generic template if no dataset-specific notebook exists.
   * - ``get_plot_guide``
     - Returns ready-to-paste plotting code snippets for a specific dataset.
       Automatically selects Parquet (non-gridded) or Zarr (gridded) patterns,
       injects real variable names from the schema (including the full variable
       table), and adds radar-specific vector plots when relevant.
   * - ``get_dataquery_reference``
     - Public API reference for ``DataQuery.py`` (classes, method signatures,
       docstrings, including the new ``describe()`` method for live variable
       introspection).  Useful when adapting notebook code.
   * - ``start_notebook``
     - **Start building a validated notebook.** Initialises a draft with a title
       and output path, auto-adds and executes the DataQuery setup cell.
       Returns a ``session_id`` to use with ``add_notebook_cell`` and
       ``save_notebook``.
   * - ``add_notebook_cell``
     - **Add a validated cell to a notebook draft.** Code cells are executed in
       the persistent session BEFORE being committed — if execution fails, the
       cell is rejected with the traceback and the AI must fix and retry.
       Markdown cells are added unconditionally.
   * - ``save_notebook``
     - **Save and validate a notebook.** Writes cells to ``.ipynb``, then
       **re-executes the entire notebook in a fresh Jupyter kernel**.  If any
       cell fails, the draft is kept alive and the error report is returned —
       the AI must fix broken cells with ``replace_notebook_cell`` and call
       ``save_notebook`` again.  Only succeeds when all cells pass.
   * - ``replace_notebook_cell``
     - **Fix a cell in an existing draft.** Replaces a cell by index, with the
       same execute-then-commit validation as ``add_notebook_cell``.  Use after
       ``save_notebook`` reports ❌ cells.
   * - ``fix_notebook``
     - **Rescue an existing broken notebook.** Validates the ``.ipynb`` in a fresh
       kernel.  If errors are found, imports all cells into a builder session so
       the AI can fix them with ``replace_notebook_cell`` → ``save_notebook``.

Available MCP Resources
------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Resource URI
     - Description
   * - ``catalog://datasets``
     - Machine-readable JSON array of all datasets with name, format,
       description, S3 ARN, catalogue URL, and variable list.

Example AI Prompts
------------------

The following prompts work well with an MCP-enabled AI assistant:

* *"Give me a notebook to access mooring temperature data near Sydney between
  2020 and 2023."*  ← the AI will call ``check_dataset_coverage`` to confirm
  the dataset actually covers the Sydney area and that time range.

* *"Show me all satellite sea surface temperature datasets available as Zarr."*

* *"What variables are in the Argo float dataset? Give me a notebook that plots
  temperature profiles."*  ← the AI will call ``get_dataset_schema`` and find
  that the time axis is ``JULD``, not ``TIME``.

* *"I need ocean chlorophyll-a data from MODIS Aqua for the Coral Sea in 2022 —
  can you prepare a notebook for that?"*

* *"List all radar datasets covering South Australian waters."*

* *"Does the SOOP-BA dataset have data in the Bass Strait between 2018 and 2021?"*
  ← directly exercises ``check_dataset_coverage`` with lat/lon and date filters.


Notebook Builder Workflow
-------------------------

The recommended workflow for generating validated Jupyter notebooks uses the
**builder pattern** — a sequence that guarantees every code cell has
been executed successfully before the notebook is delivered:

.. code-block:: text

   ┌─────────────────────┐
   │  1. start_notebook   │──▶ session_id
   └────────┬────────────┘
            │
            ▼  (repeat for each cell)
   ┌─────────────────────────────┐
   │  2. add_notebook_cell        │
   │     code → execute → commit  │
   │     if fails → ❌ reject     │
   └────────┬────────────────────┘
            │
            ▼
   ┌──────────────────────────────────────────┐
   │  3. save_notebook                         │
   │     write .ipynb → re-execute in fresh    │
   │     kernel → if ❌ → keep draft open      │
   └────────┬─────────────────────────────────┘
            │ (if validation fails)
            ▼
   ┌──────────────────────────────────────────┐
   │  4. replace_notebook_cell(cell_index, …)  │
   │     fix broken cells → go to step 3       │
   └──────────────────────────────────────────┘

**Key properties:**

* ``start_notebook`` creates a draft session with a DataQuery setup cell
  (imports ``GetAodn``, ``plot_ts_diagram``) that is auto-executed on creation.
  The setup cell adds the notebooks directory to ``sys.path`` so imports work
  in any kernel.
* ``add_notebook_cell`` executes code cells in the persistent session **before**
  committing them.  Variables persist across cells (just like a Jupyter kernel).
  If a cell raises an exception, it is **rejected** — the AI must fix the code
  and retry.
* ``save_notebook`` writes cells to the ``.ipynb`` file, then **re-executes the
  entire notebook in a fresh Jupyter kernel** (via ``validate_notebook``).  If
  any cell fails, the draft stays alive and the error report is returned.
* ``replace_notebook_cell`` replaces a broken cell by index (with the same
  execute-then-commit validation), then the AI calls ``save_notebook`` again.

This architecture makes it **impossible** to deliver a broken notebook.
``save_notebook`` will not succeed until every cell passes full-kernel
validation — including setup imports, data queries, and plots.

**Typical sequence for an oceanographic analysis notebook:**

1. ``search_datasets("wave buoy Tasmania")`` — find relevant datasets.
2. ``get_dataset_summary("wave_buoys_realtime_nonqc.parquet")`` — understand type,
   variables, code patterns.
3. ``check_dataset_coverage("wave_buoys_realtime_nonqc.parquet", ...)`` — confirm
   data exists in the user's region and time window.
4. ``start_notebook(title="Wave Buoy Analysis — Tasmania", output_path="wave_buoy_tasmania.ipynb")``
5. ``add_notebook_cell(session_id, "# Introduction\n\nWave buoy analysis...", cell_type="markdown")``
6. ``add_notebook_cell(session_id, "ds = GetAodn('wave_buoys_realtime_nonqc.parquet')\ndf = ds.get_data(...)")``
7. ``add_notebook_cell(session_id, "df.plot(...)")`` — creates a plot cell.
8. ``save_notebook(session_id)`` — writes the validated notebook.

Known Code Pitfalls Avoided by the Server
------------------------------------------

The server instructions and ``get_plot_guide`` tool explicitly guard against
these recurring Python errors in oceanographic notebooks:

**1. Day-of-month overflow (``ValueError: Day out of range "2015-04-31"``).**
   Never add 1 to the last day returned by ``calendar.monthrange()`` to create
   an exclusive upper bound — April, June, September and November only have 30
   days. Use the safe helper instead::

       def _next_month_start(yr, m):
           ts = pd.Timestamp(year=yr, month=m, day=1) + pd.DateOffset(months=1)
           return np.datetime64(ts.strftime('%Y-%m-%d'))

**2. numpy datetime64 f-string format spec (``ValueError: Invalid format
   specifier '%Y-%m-%d'``).** The format spec ``{arr[0]:%Y-%m-%d}`` fails for
   ``numpy.datetime64`` values. Always convert first::

       pd.Timestamp(arr[0]).strftime('%Y-%m-%d')

**3. DataQuery standalone functions called as class methods
   (``AttributeError: 'ParquetDataSource' has no attribute 'plot_ts_diagram'``).**
   ``plot_ts_diagram``, ``plot_timeseries``, and similar helpers are module-level
   functions, not methods of any dataset class. Import and call them directly::

       from DataQuery import plot_ts_diagram
       plot_ts_diagram(df, temp_col='TEMP', psal_col='PSAL', z_col='DEPTH')

**4. xarray ``NotImplementedError`` (slice + ``method='nearest'``).** Xarray
   refuses to combine a range slice and a nearest-neighbour lookup in one
   ``.sel()`` call. Always chain two separate calls::

       ds.sel(time=slice(t0, t1)).sel(lat=y, lon=x, method='nearest')

**5. pandas duplicate-column ``ValueError``.** Renaming a column to a name that
   already exists creates duplicate columns and breaks many pandas operations.
   Pass original column names as keyword arguments instead of renaming.

Dataset–Notebook Mapping
--------------------------

Each dataset in ``config/dataset/`` has a corresponding Jupyter notebook in
``notebooks/`` sharing the same base name. For example:

* ``config/dataset/mooring_temperature_logger_delayed_qc.json``
* ``notebooks/mooring_temperature_logger_delayed_qc.ipynb``

The notebooks use the standalone ``DataQuery.py`` library (see
:ref:`module-overview`) which provides the ``GetAodn`` class and associated
methods for querying and visualising cloud-optimised data on S3.

Testing
-------

A dedicated integration test suite validates all 12 MCP tools, including live
S3 coverage queries, notebook execution, and end-to-end user-prompt scenarios.
See :ref:`mcp-testing` for full instructions.
