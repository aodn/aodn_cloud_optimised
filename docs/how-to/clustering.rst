.. _clustering-guide:

Cluster Configuration & Distributed Processing
===============================================

All large-scale AODN processing uses `Coiled <https://coiled.io/>`_ — a managed Dask cluster
service that auto-scales AWS workers. This guide explains how to choose the right settings for
``batch_size``, ``memory_limit``, ``nthreads``, ``n_workers``, and ``worker_vm_types``.

Getting these parameters wrong is the most common cause of slow or failing processing jobs.

.. contents:: On this page
   :local:
   :depth: 2

----

The ``batch_size`` Paradox
--------------------------

.. important::

   **Bigger ``batch_size`` does NOT create more parallelism — it creates less.**

Each batch is one Dask task. One task runs on one worker. Coiled auto-scales the number of
active workers based on the number of tasks waiting in the scheduler queue. The relationship is:

.. code-block:: text

   n_tasks  = total_files / batch_size
   n_active_workers ≈ min(n_tasks, n_workers_max)

If you have 1 000 files and ``batch_size=100``, you get 10 tasks. With 20 workers, at most 10 are
ever active at the same time — the other 10 stay idle for the whole job.

If you use ``batch_size=10``, you get 100 tasks → all 20 workers stay busy for 5 rounds.

**Rule of thumb:**

.. code-block:: text

   batch_size ≈ total_files / (n_workers_max × 5)

Targeting 5 rounds of work per worker means all workers stay busy and the job finishes
efficiently. Targeting more rounds (10–20) is better for jobs with uneven file sizes.

**Real example (AC-S hourly, 42 000 files):**

- ``batch_size=50``, ``n_workers_max=20`` → 840 tasks → good utilisation ✓
- ``batch_size=5000``, ``n_workers_max=20`` → 8 tasks → 12 workers idle at all times ✗

.. note::

   Each batch runs sequentially inside a single worker: all files in the batch are opened,
   processed, and written to Zarr one after another. A larger batch does not speed up a
   single worker; it just makes the scheduler give that worker more work before it can share.

----

The ``memory_limit`` Pitfall
-----------------------------

``memory_limit`` in the ``worker_options`` block tells Dask how much RAM the worker has
available. When usage approaches this value, Dask spills data to disk to avoid OOM crashes.

**If ``memory_limit`` is set higher than the actual VM RAM, Dask never spills**, because it
thinks there is still headroom. This leads to:

- Workers running out of real RAM silently
- OS swap activity → major slowdowns (100× slower than RAM)
- Unexpected OOM kills from the OS (no warning, no retry)

The correct value is roughly **87.5 % of actual VM RAM** (leaving ~12.5 % for the OS, Python
interpreter, and Dask overhead).

.. _vm-ram-table:

VM RAM reference table
^^^^^^^^^^^^^^^^^^^^^^

+------------------------+----------+---------------------------+
| VM type                | Actual   | Recommended               |
|                        | RAM      | ``memory_limit``          |
+========================+==========+===========================+
| ``m7i-flex.large``     | 8 GB     | **7 GB**                  |
| ``m7i.large``          |          |                           |
+------------------------+----------+---------------------------+
| ``m7i-flex.xlarge``    | 16 GB    | **14 GB**                 |
| ``m7i.xlarge``         |          |                           |
+------------------------+----------+---------------------------+
| ``m7i-flex.2xlarge``   | 32 GB    | **28 GB**                 |
| ``m7i.2xlarge``        |          |                           |
+------------------------+----------+---------------------------+
| ``m7i-flex.4xlarge``   | 64 GB    | **56 GB**                 |
| ``m7i.4xlarge``        |          |                           |
+------------------------+----------+---------------------------+
| ``m7i-flex.8xlarge``   | 128 GB   | **112 GB**                |
| ``m7i.8xlarge``        |          |                           |
+------------------------+----------+---------------------------+

**Parquet vs. Zarr memory behaviour:**

- **Parquet** (timeseries): all files in a batch are fully loaded into a Pandas DataFrame.
  Peak memory ≈ ``batch_size × avg_file_size × 3``. Keep ``batch_size`` small enough that the
  whole batch fits in ``memory_limit``.

- **Zarr / xarray** (gridded, spectral): files are opened lazily with ``open_mfdataset``.
  Only the chunks currently being written are in memory. Peak memory is typically 1–3 chunks,
  regardless of ``batch_size``. ``batch_size`` affects parallelism, not peak memory for Zarr.

----

``nthreads`` — Thread Count per Worker
---------------------------------------

Each Dask worker can run multiple Python threads. Threads share memory but are subject to
Python's Global Interpreter Lock (GIL) for CPU-bound code.

General guidance:

+-----------------------------------+-------------------+-------------------------------------+
| Workload type                     | Recommended       | Reason                              |
|                                   | ``nthreads``      |                                     |
+===================================+===================+=====================================+
| Parquet, small NetCDF files       | 4–8               | I/O-bound; threads overlap          |
| (mooring, vessel, Argo)           |                   | download + decode                   |
+-----------------------------------+-------------------+-------------------------------------+
| Standard Zarr (gridded satellite, | 4–8               | Moderate I/O and compute            |
| mooring timeseries)               |                   |                                     |
+-----------------------------------+-------------------+-------------------------------------+
| Spectral Zarr with regridding     | 2                 | Heavy SciPy interpolation; more     |
| (ACS, HyperOCR, DALEC)            |                   | threads cause GIL contention        |
+-----------------------------------+-------------------+-------------------------------------+
| Ocean colour (large tiled arrays, | 16–32             | Dask chunks processed independently;|
| ``satellite_ocean_colour_*``)     |                   | each thread works on one tile       |
+-----------------------------------+-------------------+-------------------------------------+

.. warning::

   Setting ``nthreads`` too high on a small VM multiplies memory pressure. A worker with
   ``nthreads=32`` and ``memory_limit=14GB`` allocates only 437 MB per thread — easily
   exceeded by any scientific interpolation step.

----

``n_workers`` — Worker Count
-----------------------------

Set as a list ``[min, max]``. Coiled auto-scales between these limits:

- ``min`` controls the number of workers that start immediately (warm-up cost).
  For short jobs set ``min=1``. For jobs with thousands of tasks, set ``min=5–20``
  so the cluster doesn't start cold.
- ``max`` is the ceiling. Coiled only runs workers when there are tasks to process.
  Setting a high ``max`` costs nothing if the task queue is small.

.. tip::

   Do not set ``max`` below ``n_tasks / 5``. If there are fewer workers than tasks ÷ 5,
   workers will be busy for too many sequential rounds and the job will run slowly.

----

``worker_vm_types`` — VM Selection
------------------------------------

Choose the instance type based on the data format and spectral complexity:

+----------------------+--------------------------------------+------------------------------------+
| VM type              | Best for                             | Notes                              |
+======================+======================================+====================================+
| ``m7i-flex.large``   | Scalar parquet timeseries            | 8 GB RAM; light I/O-only workloads |
| (8 GB RAM)           | (WQM, EcoTriplet, small vessel data) |                                    |
+----------------------+--------------------------------------+------------------------------------+
| ``m7i-flex.xlarge``  | Standard Zarr, moderate NetCDF       | 16 GB RAM; most GHRSST datasets,   |
| (16 GB RAM)          | (mooring, glider, radar, GHRSST)     | Argo, most mooring collections     |
+----------------------+--------------------------------------+------------------------------------+
| ``m7i.2xlarge``      | Spectral Zarr, large gridded NetCDF  | 32 GB RAM; ACS, HyperOCR, DALEC,   |
| (32 GB RAM)          | (LJCO instruments, satellite L4)     | large GHRSST composites            |
+----------------------+--------------------------------------+------------------------------------+
| ``m7i.4xlarge``      | High-resolution ocean colour,        | 64 GB RAM; multi-variable tiled    |
| (64 GB RAM)          | multi-band satellite products        | products with large arrays         |
+----------------------+--------------------------------------+------------------------------------+

Use the ``m7i-flex.*`` variants where possible — they are `Flex` instances that can burst
above their baseline CPU for short periods, which helps with decompression and interpolation spikes.

----

Putting It All Together — Configuration Reference
--------------------------------------------------

The full Coiled configuration block in a dataset JSON looks like this:

.. code-block:: json

    {
      "run_settings": {
        "batch_size": 50,
        "coiled_cluster_options": {
          "n_workers": [5, 40],
          "scheduler_options": {"idle_timeout": "2 hours"},
          "worker_vm_types": "m7i-flex.xlarge",
          "allow_ingress_from": "everyone",
          "compute_purchase_option": "spot_with_fallback",
          "worker_options": {
            "nthreads": 4,
            "memory_limit": "14GB"
          }
        }
      }
    }

----

Worked Examples
---------------

Argo float profiles (Parquet, ~500 000 files across 11 DACs)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each profile NetCDF is ~1.2 MB. Loaded fully into Pandas (parquet format).

.. code-block:: json

    {
      "batch_size": 300,
      "coiled_cluster_options": {
        "n_workers": [2, 80],
        "worker_vm_types": "m7i-flex.xlarge",
        "worker_options": {"nthreads": 8, "memory_limit": "12GB"}
      }
    }

- ``batch_size=300``: 300 × 1.2 MB × 3× expansion ≈ 1.1 GB per batch → well within 12 GB
- ``n_workers_max=80``: 500 000 / 300 ≈ 1 667 tasks → 80 workers → ~21 rounds
- ``nthreads=8``: parquet loading is I/O-bound; threads overlap S3 downloads

.. note::

   Argo processing is inherently long due to the sheer file count. With 1 667 tasks and
   80 workers at ~3 min/task, expect ~60 minutes. This is expected behaviour, not a bug.

AC-S hourly (Spectral Zarr, 42 000 files, 707-wavelength regridding)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each hourly file is ~4 MB. Heavy SciPy interpolation during preprocessing.

.. code-block:: json

    {
      "batch_size": 5,
      "coiled_cluster_options": {
        "n_workers": [1, 40],
        "worker_vm_types": "m7i.2xlarge",
        "worker_options": {"nthreads": 2, "memory_limit": "28GB"}
      }
    }

- ``batch_size=5``: 42 000 / 5 = 8 400 tasks → 40 workers → 210 rounds, each tiny
- ``nthreads=2``: SciPy ``interp`` is CPU-bound; more threads cause GIL contention
- ``memory_limit=28GB``: 87.5 % of actual 32 GB RAM → safe Dask spill threshold

.. warning::

   A previous misconfiguration had ``target_wavelength_grids.step=0.1``, creating 3 531
   wavelength points instead of 707. This caused each batch to load ~65 GB of data and the
   job ran for 12+ hours without completing. Always verify spectral grid sizes match the
   ``dimensions.chunk`` and ``dimensions.size`` values in the config.

GHRSST L3S 1-day (Gridded Zarr, ~15 000 files, 139 MB each)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Large gridded files opened lazily with xarray — peak memory per worker ≈ 1–2 tiles.

.. code-block:: json

    {
      "batch_size": 60,
      "coiled_cluster_options": {
        "n_workers": [35, 120],
        "worker_vm_types": "m7i-flex.xlarge",
        "worker_options": {"nthreads": 2, "memory_limit": "14GB"}
      }
    }

- ``batch_size=60``: 15 000 / 60 = 250 tasks → 120 workers → 2 rounds (fast!)
- ``nthreads=2``: xarray zarr writes benefit from 2 threads for I/O overlap
- Lazy loading: only active chunks are in memory, so 60 × 139 MB is not loaded at once

----

Troubleshooting
---------------

**Processing is very slow despite many workers**

Check that ``n_tasks ≫ n_workers_max``. If ``total_files / batch_size < n_workers_max``,
most workers are idle. **Reduce** ``batch_size``.

**Workers keep crashing with OOM**

1. Check ``memory_limit`` does not exceed actual VM RAM (see :ref:`vm-ram-table`).
2. For Parquet: reduce ``batch_size`` (each batch is fully loaded).
3. For spectral Zarr with ``target_wavelength_grids``: verify the ``step`` produces the
   expected number of points — a small step creates massive arrays.

**Very high CPU but low throughput (GIL contention)**

Reduce ``nthreads``. For spectral interpolation workloads, ``nthreads=2`` is usually optimal.
Additional threads queue behind the GIL and add overhead without adding throughput.

**Job completes quickly but output Zarr store is missing data**

The ``n_workers_max`` might have been hit before all tasks were queued. Check Coiled
dashboard for task failures. If workers were killed by OOM (no graceful error), reduce
``batch_size`` and/or upgrade ``worker_vm_types``.

Additional Resources
--------------------

- :ref:`dataset-config-doc` — Full dataset configuration reference including
  ``coiled_cluster_options`` schema
- `Coiled Documentation <https://docs.coiled.io/>`_
- `Dask Distributed Documentation <https://distributed.dask.org/>`_
