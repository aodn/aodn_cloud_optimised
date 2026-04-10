import pydantic

from .worker_options import WorkerOptions


class CoiledClusterOptions(pydantic.BaseModel):
    """Configuration options for a Coiled cluster.

    Attributes:
        n_workers: List of two integers specifying the minimum and maximum number of workers (e.g., [25, 150]).
        scheduler_vm_types: VM type to use for the Coiled cluster scheduler.
        worker_vm_types: VM type to use for Coiled cluster workers.
        allow_ingress_from: IP or CIDR block allowed to access the cluster.
        compute_purchase_option: AWS compute purchase option (e.g., "on_demand", "spot").
        worker_options: Configuration for individual Coiled cluster workers.
    """

    n_workers: list[int] = pydantic.Field(
        ..., description="List of integers: min and max workers (e.g., [25, 150])."
    )
    scheduler_vm_types: str
    worker_vm_types: str
    allow_ingress_from: str
    compute_purchase_option: str
    worker_options: WorkerOptions
