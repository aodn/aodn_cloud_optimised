import pydantic

import aodn_cloud_optimised.lib.clusterLib


class ClusterConfig(pydantic.BaseModel):
    """Cluster processing configuration.

    Attributes:
        mode: Cluster mode (local, coiled, ec2, or None).
        restart_every_path: Restart cluster after processing each path.
    """

    mode: aodn_cloud_optimised.lib.clusterLib.ClusterMode | None = pydantic.Field(
        default=None,
        description=f"The mode for the cluster. Must be one of: {[m.value for m in aodn_cloud_optimised.lib.clusterLib.ClusterMode]} or null.",
    )
    restart_every_path: bool = pydantic.Field(
        default=False,
        description="Whether to restart the cluster after each path is processed.",
    )
