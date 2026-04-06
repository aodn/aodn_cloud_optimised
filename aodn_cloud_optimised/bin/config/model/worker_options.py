import pydantic


class WorkerOptions(pydantic.BaseModel):
    """Worker configuration for Coiled clusters.

    Attributes:
        nthreads: Number of threads per worker.
        memory_limit: Memory limit per worker (e.g., "64GB").
    """

    nthreads: int
    memory_limit: str  # "64GB", etc.
