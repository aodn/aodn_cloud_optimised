import pydantic


class Ec2ClusterOptions(pydantic.BaseModel):
    """Configuration options for a EC2 cluster.

    Attributes:
        n_workers: integer specifying the number of workers (e.g., 25).
        scheduler_instance_type: VM type to use for the Coiled cluster scheduler.
        worker_instance_type: VM type to use for Coiled cluster workers.
        security: boolean.
        docker_image: url as string
        worker_options: Configuration for individual Coiled cluster workers.
    """

    n_workers: int = pydantic.Field(..., description="integer of workers  (e.g., 25).")
    scheduler_instance_type: str
    worker_instance_type: str
    security: bool
    docker_image: str
