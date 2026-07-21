import pydantic


class PrefectConfig(pydantic.BaseModel):
    """Cluster processing configuration.

    Attributes:
        mode: Cluster mode (local, coiled, ec2, or None).
        restart_every_path: Restart cluster after processing each path.
    """

    env: dict = pydantic.Field(
        default_factory=dict, description="Override deployment environment variables"
    )
    job_variables: dict = pydantic.Field(
        default_factory=dict,
        description="Override deployment job_variables. Most notably allows configuration of cpu and memory",
    )
