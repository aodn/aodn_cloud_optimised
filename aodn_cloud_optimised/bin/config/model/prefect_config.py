import pydantic


class PrefectConfig(pydantic.BaseModel):
    """Prefect processing configuration.

    Attributes:
        env: environment variables to override on the worker
        job_variables: job_variables to override on the worker
    """

    job_variables: dict = pydantic.Field(
        default_factory=dict,
        description="Override deployment job_variables. Most notably allows configuration of cpu and memory",
    )
