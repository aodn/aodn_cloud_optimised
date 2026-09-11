.. _tune-zarr:

Tune a Zarr Dataset
===================

This guide explains how to configure and tune a Zarr dataset from its dataset JSON
configuration. Follow the steps in order: each step describes what to inspect, what
to change, and how to verify the result.

It applies to Zarr configurations under
``aodn_cloud_optimised/config/dataset/`` and the Prefect and Dask flows that run
them. It assumes that the dataset configuration and schema already exist. See
:ref:`dataset-config-doc` to create them.

.. contents:: On this page
   :local:
   :depth: 2

Scope and tuning principles
---------------------------

The dataset configuration describes the data and workload, not the machines.

.. list-table:: Configuration ownership
   :header-rows: 1
   :widths: 50 50

   * - Dataset configuration (data engineer)
     - Infrastructure (infrastructure team)
   * - Chunks
     - Worker vCPU, memory, and threads
   * - Append dimension
     - Fleet size and scaling policy
   * - Batch size
     - Scheduler endpoint
   * - Source paths and schema
     - Deployment configuration

Confirm infrastructure-owned values with the infrastructure team before using them
in a calculation. For general infrastructure guidance, see :ref:`clustering-guide`.

Chunk size and batch size are the two main Zarr tuning controls:

* **Chunk size** determines the physical layout on S3 and therefore the efficiency
  of every downstream read and write. Chunks that are too small create excessive
  objects and metadata overhead. Chunks that are too large increase task memory and
  reduce parallelism. Changing chunks after creating the store is expensive.
* **Batch size** determines the width of each Dask task graph. A graph that is too
  narrow may drain before the scaling alarm fires, leaving work on one worker even
  when more workers are available.

Step 1: Inspect the source data
-------------------------------

Inspect a representative source file before changing the configuration. Record:

* file size on disk;
* dimension names and sizes, such as time, latitude, and longitude;
* data variables and their dtypes (``float32`` is 4 bytes and ``float64`` is
  8 bytes);
* the spatial grid shape;
* the number of time steps per file; and
* the number of data variables, ``n_vars``.

Use the THREDDS OPeNDAP form at
``https://thredds.aodn.org.au/thredds/dodsC/<path-to-file>.nc.html`` or inspect a
local file with xarray:

.. code-block:: python

   import xarray as xr

   ds = xr.open_dataset("sample.nc")
   print(ds)
   print(ds["<var>"].dtype)

Before continuing, write down ``grid_lat``, ``grid_lon``, ``itemsize``,
``time_steps_per_file``, and ``n_vars``.

Step 2: Map the dimensions
--------------------------

Define Zarr dimensions and chunks under ``schema_transformation.dimensions``:

.. code-block:: json

   "schema_transformation": {
     "dimensions": {
       "time":      {"name": "time", "chunk": 7,   "append_dim": true},
       "latitude":  {"name": "lat",  "chunk": 630, "size": 0},
       "longitude": {"name": "lon",  "chunk": 895, "size": 0}
     }
   }

Apply these rules:

* Set ``append_dim`` to ``true`` on exactly one dimension, normally time. New
  files are concatenated along this dimension.
* Set ``name`` to the exact dimension variable name in the source NetCDF.
* Use ``size: 0`` for fixed-grid spatial dimensions when the configuration needs
  to represent their full extent.

Step 3: Choose the chunk sizes
------------------------------

A chunk is the smallest unit that Zarr reads or writes. Choose chunks that fall in
the target size range and tile the source grid cleanly.

Calculate the uncompressed chunk size
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For a three-dimensional variable:

.. code-block:: text

   chunk_bytes = chunk_time * chunk_lat * chunk_lon * itemsize

For example, a ``float32`` variable with chunks of 7 time steps by 630 latitude
points by 895 longitude points uses approximately 15.05 MiB:

.. code-block:: text

   7 * 630 * 895 * 4 bytes = 15,783,600 bytes = 15.05 MiB

Aim for **10--150 MiB per uncompressed chunk**.

* Below the range, the store can contain millions of small objects, increasing S3
  and metadata overhead.
* Above the range, each task needs more memory and the workload has less
  parallelism, increasing the risk of out-of-memory failures.

Align spatial chunks to the grid
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Prefer spatial chunk sizes that divide the grid dimensions evenly:

.. code-block:: text

   grid_lat / chunk_lat = whole number
   grid_lon / chunk_lon = whole number

For example, a grid of 1,890 by 2,685 with chunks of 630 by 895 produces a clean
3 by 3 spatial tiling without ragged edge chunks.

Choose the append-dimension chunk
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A time chunk of a few steps, commonly 5--14, is a useful starting point. Keep it
large enough to avoid one chunk per time step, but small enough to keep the
calculated chunk size in the target range.

Record the chosen time and spatial chunks and their calculated uncompressed size.
These values become part of the store layout.

Step 4: Choose the batch size
-----------------------------

``run_settings.batch_size`` is the number of input files processed in one Dask
graph:

.. code-block:: json

   "run_settings": {
     "batch_size": 91
   }

Each batch runs to completion before the next batch starts. The flow builds the
graph, calls ``to_zarr(compute=True)``, waits, and then starts the next batch.

Estimate the graph width with:

.. code-block:: text

   primary_chunks ~= ceil(batch_size * time_steps_per_file / chunk_time)
                     * ceil(grid_lat / chunk_lat)
                     * ceil(grid_lon / chunk_lon)
                     * n_vars

This is an estimate of the primary data chunks, not the scheduler's exact task
count. If each file contains one time step, ``time_steps_per_file`` is 1.

For a dataset with one time step per file, a time chunk of 7, a 3 by 3 spatial
tiling, and 5 data variables:

.. list-table:: Batch-size comparison
   :header-rows: 1
   :widths: 15 35 20 30

   * - ``batch_size``
     - Calculation
     - Primary chunks
     - Observed result
   * - 30
     - ``ceil(30 / 7) * 3 * 3 * 5``
     - 225
     - Remained at one worker
   * - 91
     - ``ceil(91 / 7) * 3 * 3 * 5``
     - 585
     - Scaled from one to three to five workers

Aim for several hundred primary chunks per batch so the scheduler has enough
runnable work to trigger scaling. Prefer a ``batch_size`` that is an aligned
multiple of ``chunk_time`` when each file contributes one time step. When files
contain multiple time steps, align using the total time steps contributed by the
batch.

Step 5: Check the memory ceiling
--------------------------------

Check the memory required by chunks processed concurrently on one worker:

.. code-block:: text

   chunk_bytes * threads_per_worker * overhead_factor < worker_memory_limit

Use a small overhead factor based on observations from a representative run.
Confirm the current thread count and worker memory limit with the infrastructure
team rather than assuming them.

The Zarr path streams data while ``persist()`` is disabled. A healthy run can use
only 9--20% of worker memory while keeping the CPU busy. Do not enlarge chunks just
to consume otherwise idle memory.

If the ceiling does not hold, reduce the batch size to limit concurrent work. If a
single task still requires too much memory, reduce the chunk size and recalculate
the store layout.

Step 6: Understand scale-out behaviour
--------------------------------------

On the shared ECS and Fargate path, scale-out follows this chain:

.. code-block:: text

   wider graph -> QueuedTasks exceeds threshold -> alarm holds for a full period
               -> ECS step scaling -> Fargate tasks start
               -> workers register -> work is distributed

Keep these details in mind when diagnosing a run:

* ``QueuedTasks`` is not the total task count. It counts runnable tasks waiting in
  the queue. For example, a run may show 3,270 total tasks, 1,756 waiting, 264
  queued, and 9 processing.
* The backlog must persist for the full alarm period, which may be about 240
  seconds. A narrow sequential batch can drain before the alarm fires, so no new
  workers start.

In the dashboard, Task Stream lanes represent threads: two lanes normally mean one
worker with two threads. Use the Workers panel for worker count, CPU, and memory.
A newly registered worker showing 0% CPU for one refresh is normal.

Step 7: Run and record a baseline
---------------------------------

Run the flow and record:

* worker count over time;
* peak queued tasks;
* peak worker memory percentage; and
* total runtime.

Keep these results with the proposed configuration change. Change one tuning
variable at a time so the next run can be compared with this baseline.

Step 8: Diagnose and adjust
---------------------------

.. list-table:: Zarr tuning symptoms
   :header-rows: 1
   :widths: 28 36 36

   * - Symptom
     - Likely cause
     - Action
   * - Stuck at one worker or two Task Stream lanes
     - The graph is too narrow, or its backlog drains before the alarm period.
     - Raise ``batch_size`` by an aligned multiple and rerun.
   * - Worker memory stays below 20%
     - Normal streaming behaviour.
     - Take no action if CPU is active; do not enlarge chunks merely to use memory.
   * - ``KilledWorker`` or out-of-memory failure
     - Too much concurrent work, or chunk size multiplied by active threads exceeds
       the memory ceiling.
     - Lower ``batch_size`` and recheck Step 5. Reduce chunks if one task still
       exceeds the ceiling.
   * - Coiled settings have no effect
     - The flow is running on the Prefect and ECS path.
     - Ignore Coiled-only options and tune ``batch_size``.
   * - ``batch_size`` behaves as 1
     - The key is missing from ``run_settings``.
     - Set ``batch_size`` explicitly.
   * - Time is not monotonic or contains duplicates
     - Cross-batch file ordering, a region write, or an interrupted worker.
     - Rerun the flow.
   * - A variable is missing from some files
     - The source files are inconsistent.
     - Set ``"drop_var": true`` for that variable in ``schema`` when it should not
       be included in the output.

The operational rule is: if the workload did not scale, widen ``batch_size``; if
workers died, narrow it. Change the physical chunk layout only when the memory
calculation or downstream access pattern shows that the chunks are the problem.

Pre-merge checklist
-------------------

Before merging the dataset configuration, confirm that:

.. checklist intentionally uses ordinary bullets for broad Sphinx compatibility.

* source dimensions and the grid were inspected, and ``itemsize``,
  ``time_steps_per_file``, and ``n_vars`` are recorded;
* exactly one dimension has ``append_dim: true``;
* the calculated uncompressed chunk size is stated and falls in the 10--150 MB
  target range;
* spatial chunks divide the grid evenly, where practical;
* ``batch_size`` is present, aligned, and produces several hundred primary chunks;
* the memory ceiling was checked against current worker limits; and
* a test run records worker count, peak queued tasks, peak memory percentage, and
  runtime.
