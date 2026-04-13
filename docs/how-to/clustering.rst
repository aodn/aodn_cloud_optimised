.. _clustering-guide:

Cluster Configuration & Distributed Processing
===============================================

The AODN Cloud Optimised library supports both local and remote processing of large datasets using Dask clusters. This guide explains clustering options, when to use them, and how to configure them.

When to Use Clustering
----------------------

**Local Mode (Default)**

Use local clustering for:

- Development and testing
- Small to medium datasets (< 100GB)
- Single-machine processing
- Rapid iteration and debugging

Configuration:

.. code-block:: json

    {
      "run_settings": {
        "cluster": {
          "mode": null
        }
      }
    }

Or via CLI:

.. code-block:: bash

    generic_cloud_optimised_creation --config my_dataset \
      --json-overwrite '{"run_settings": {"cluster": {"mode": null}}}'

**Remote Mode**

Use remote clustering for:

- Large datasets (> 100GB)
- Multi-hour processing jobs
- Production pipelines
- Parallel processing across many machines

Remote Mode Options
-------------------

Coiled (AWS-Hosted)
^^^^^^^^^^^^^^^^^^^

`Coiled <https://coiled.io/>`_ is a managed Dask cluster service. It automatically scales workers and handles infrastructure.

**When to use:**

- Easy setup without managing infrastructure
- Automatic scaling based on data size
- Pay-per-use pricing
- Supports spot instances for cost savings

**Configuration:**

1. Create a Coiled account at https://coiled.io/

2. Set your API token in environment variables:

   .. code-block:: bash

       export COILED_TOKEN="your-api-token"

3. Configure in dataset JSON:

   .. code-block:: json

       {
         "run_settings": {
           "cluster": {
             "mode": "remote",
             "type": "coiled",
             "n_workers": 10,
             "worker_memory": "4GB",
             "worker_vm_types": "t3.large"
           }
         }
       }

4. Or override at runtime:

   .. code-block:: bash

       generic_cloud_optimised_creation --config my_dataset \
         --json-overwrite '{
           "run_settings": {
             "cluster": {
               "mode": "remote",
               "type": "coiled",
               "n_workers": 20
             }
           }
         }'

**Cost Estimate:**

- 10 workers × 4GB memory: ~$2-5/hour depending on instance type
- Spot instances: ~50% cost reduction

**Pros:**

- No infrastructure management
- Automatic scaling and monitoring
- Easy integration with AWS S3
- Built-in dashboard

**Cons:**

- Managed service cost
- Less control over infrastructure
- Requires account and API token

EC2 (AWS-Managed)
^^^^^^^^^^^^^^^^^

Run Dask on your own AWS EC2 instances. More control but requires infrastructure setup.

**When to use:**

- Full control over infrastructure
- Cost optimization for long-running jobs
- On-premises or private cloud compatibility
- Complex networking requirements

**Configuration:**

1. Set up AWS credentials:

   .. code-block:: bash

       aws configure

2. Configure in dataset JSON:

   .. code-block:: json

       {
         "run_settings": {
           "cluster": {
             "mode": "remote",
             "type": "ec2",
             "n_workers": 10,
             "instance_type": "m5.xlarge",
             "bootstrap_script": "bootstrap.sh"
           }
         }
       }

3. Bootstrap script example (``bootstrap.sh``):

   .. code-block:: bash

       #!/bin/bash
       # Install system dependencies
       sudo apt-get update
       sudo apt-get install -y python3.11 python3-pip
       pip install aodn-cloud-optimised[core]

**Cost Estimate:**

- m5.xlarge instance: ~$0.20/hour
- 10 workers: ~$2/hour + data transfer costs
- Spot pricing: ~70% cost reduction

**Pros:**

- Full control over instance types
- No service fees (just AWS compute)
- Flexible networking and security
- Long-term cost savings for sustained workloads

**Cons:**

- Infrastructure management overhead
- Manual scaling and monitoring
- Longer setup time
- AWS account required

Fargate (AWS-Serverless)
^^^^^^^^^^^^^^^^^^^^^^^^

Run Dask on AWS Fargate (serverless containers). Minimal infrastructure management.

**When to use:**

- Serverless preference
- Unpredictable workload patterns
- Integration with AWS container services
- Short-lived jobs (< 24 hours)

**Configuration:**

.. code-block:: json

    {
      "run_settings": {
        "cluster": {
          "mode": "remote",
          "type": "fargate",
          "n_workers": 5,
          "cpu": 2048,
          "memory": 4096
        }
      }
    }

**Cost Estimate:**

- 5 workers × 2GB memory: ~$0.05/hour per task

**Pros:**

- No infrastructure management
- Pay-per-second granular billing
- Automatic scaling
- Good for batch/intermittent jobs

**Cons:**

- More expensive for sustained workloads
- Cold start delays (30-60s per task)
- Limited customization
- Smaller max instance sizes

Local Mode Details
------------------

Even in "local" mode, Dask can use multiple processes or threads. Configure the number of workers:

.. code-block:: json

    {
      "run_settings": {
        "cluster": {
          "mode": null,
          "threads_per_worker": 4,
          "n_workers": 2
        }
      }
    }

This creates 2 Dask worker processes with 4 threads each = 8 parallel tasks.

Common Cluster Operations
-------------------------

**Monitor cluster status**

With Coiled:

.. code-block:: bash

    coiled clusters list
    coiled clusters info my-cluster

With EC2: Check AWS EC2 console or use AWS CLI:

.. code-block:: bash

    aws ec2 describe-instances --filters "Name=tag:cluster,Values=my-cluster"

**Gracefully restart a failed cluster**

The library automatically restarts remote clusters on failure, but you can manually trigger:

.. code-block:: bash

    # With Coiled
    coiled clusters close my-cluster
    # Then re-run the command (will create new cluster)

    # With EC2
    aws ec2 terminate-instances --instance-ids i-1234567890abcdef0

**View cluster logs**

With Coiled, logs are available in the dashboard.

With EC2, SSH to the scheduler node:

.. code-block:: bash

    ssh -i your-key.pem ubuntu@scheduler-ip
    tail -f /var/log/dask-scheduler.log

Troubleshooting
---------------

**"Connection timeout to cluster"**

- Check security groups allow port 8786 (Dask scheduler)
- Verify network connectivity from your machine to cluster
- Check if cluster is still running (may have been auto-terminated)

**"Workers are too slow"**

- Increase worker memory (more workers can run in parallel)
- Use faster instance types (m5.xlarge → c5.2xlarge)
- Check if network is bottleneck (S3 download bandwidth)

**"OutOfMemory errors during processing"**

- Increase ``worker_memory`` in cluster config
- Reduce number of workers (use available memory more efficiently)
- Enable spilling to disk (Dask feature)

**"Cluster creation hangs"**

- With Coiled: Check API token is valid
- With EC2: Check AWS credentials and IAM permissions
- Check logs for bootstrap script errors

See Also
--------

- :ref:`install` — Installation with different extras
- :ref:`usage` — Using ``generic_cloud_optimised_creation``
- :ref:`dataset-config-doc` — Full configuration reference
- `Dask Distributed Documentation <https://distributed.dask.org/>`_
- `Coiled Documentation <https://docs.coiled.io/>`_
