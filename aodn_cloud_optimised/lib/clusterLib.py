import importlib
import logging
from enum import Enum

import dask.config
from coiled import Cluster as CoiledCluster
from dask.distributed import Client, LocalCluster
from dask_cloudprovider.aws import EC2Cluster

from .config import load_dataset_config


class ClusterMode(Enum):
    LOCAL = "local"
    COILED = "coiled"
    EC2 = "ec2"
    NONE = None


def parse_cluster_mode(value):
    return ClusterMode(value)


# TODO:
# WARNING CLIENT AND CLUSTER should not be self! This creates some serialization issue. Create a simple example to lodge a bug to dask


class ClusterManager:
    def __init__(self, cluster_mode, dataset_name, dataset_config, logger=None):
        self.cluster_mode = parse_cluster_mode(cluster_mode)
        self.dataset_name = dataset_name
        self.dataset_config = dataset_config
        self.logger = logger or logging.getLogger(__name__)
        self.local_cluster_options = {
            "n_workers": 2,
            "memory_limit": "8GB",
            "threads_per_worker": 2,
        }
        self.coiled_cluster_default_options = {
            "n_workers": [1, 2],
            "scheduler_vm_types": "t3.small",
            "worker_vm_types": "t3.medium",
            "allow_ingress_from": "me",
            "compute_purchase_option": "spot_with_fallback",
            "worker_options": {"nthreads": 4, "memory_limit": "8GB"},
        }

        self.ec2_cluster_default_options = (
            {
                "n_workers": 1,
                "scheduler_instance_type": "t3.xlarge",
                "worker_instance_type": "t3.2xlarge",
                "security": False,
                "docker_image": "ghcr.io/aodn/aodn_cloud_optimised:latest",
            },
        )
        self.ec2_cluster_default_adapt_options = {"minimum": 1, "maximum": 120}

    def _apply_common_dask_config(self):
        """Apply common Dask configurations."""
        dask.config.set(
            {
                "array.slicing.split_large_chunks": False,
                "distributed.scheduler.worker-saturation": "inf",
                "dataframe.shuffle.method": "p2p",
            }
        )

        dask_distributed_config = load_dataset_config(
            str(
                importlib.resources.files("aodn_cloud_optimised.config").joinpath(
                    "distributed.yaml"
                )
            )
        )

        dask.config.set(dask_distributed_config)

    def _create_client_and_cluster(self, cluster_constructor, options=None):
        """Generic method to create a cluster and its corresponding client."""
        try:
            cluster = cluster_constructor(**options)
            self._apply_common_dask_config()
            client = Client(cluster)
            self.logger.info(f"Cluster dask dashboard: {cluster.dashboard_link}")
            if client:
                client.forward_logging()
        except Exception as e:
            self.logger.warning(
                f"Failed to create a {cluster_constructor} cluster: {e}. Falling back to local cluster."
            )
            self.cluster_mode = ClusterMode.LOCAL  # overwrite value
            return self.create_local_cluster()
        return client, cluster, self.cluster_mode.value

    def create_local_cluster(self):
        return self._create_client_and_cluster(LocalCluster, self.local_cluster_options)

    def create_coiled_cluster(self):
        coiled_cluster_options = self.dataset_config.get("coiled_cluster_options", None)
        if coiled_cluster_options is None:
            self.logger.warning(
                f"Missing coiled_cluster_options in dataset_config. Using default value {self.coiled_cluster_default_options}"
            )
            coiled_cluster_options = self.coiled_cluster_default_options

        coiled_cluster_options["name"] = f"Processing_{self.dataset_name}"

        return self._create_client_and_cluster(
            CoiledCluster, options=coiled_cluster_options
        )

    def create_ec2_cluster(self):
        ec2_cluster_options = self.dataset_config.get("ec2_cluster_options", None)

        if ec2_cluster_options is None:
            self.logger.warning(
                f"Missing ec2_cluster_options entry in dataset_config. Using default value {self.ec2_cluster_default_options}"
            )
            ec2_cluster_options = self.ec2_cluster_default_options

        cluster = EC2Cluster(**ec2_cluster_options)
        ec2_adapt_options = self.dataset_config.get("ec2_adapt_options", None)
        if ec2_adapt_options:
            cluster.adapt(**ec2_adapt_options)
        else:
            self.logger.warning(
                f"Missing ec2_adapt_options entry in dataset_config. Using default value {self.ec2_cluster_default_adapt_options}"
            )
            cluster.adapt(**self.ec2_cluster_default_adapt_options)

        self._apply_common_dask_config()
        client = Client(cluster)
        self.logger.info(f"EC2 Cluster dask dashboard: {cluster.dashboard_link}")

        return client, cluster, self.cluster_mode

    def create_cluster(self):

        if self.cluster_mode == ClusterMode.LOCAL:
            return self.create_local_cluster()
        elif self.cluster_mode == ClusterMode.COILED:
            return self.create_coiled_cluster()
        elif self.cluster_mode == ClusterMode.EC2:
            return self.create_ec2_cluster()
        elif self.cluster_mode == ClusterMode.NONE:
            client, cluster = None, None
            return client, cluster, self.cluster_mode.value
        else:
            raise ValueError(f"Unsupported cluster mode: {self.cluster_mode}")

    def close_cluster(self, client, cluster):
        if not client or not cluster:
            return

        try:
            client.close()
            self.logger.info("Successfully closed Dask client.")
            cluster.close()
            self.logger.info("Successfully closed Dask cluster.")
        except Exception as e:
            self.logger.error(f"Error while closing the cluster or client: {e}")
