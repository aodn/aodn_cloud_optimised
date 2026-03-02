import pydantic


class Ec2AdaptOptions(pydantic.BaseModel):
    """Configuration options for a EC2 cluster workers.

    Attributes:
    """

    minimum: int = pydantic.Field(
        ..., description="integer of minimum workers  (e.g., 25)."
    )
    maximum: int = pydantic.Field(
        ..., description="integer of maximum workers  (e.g., 25)."
    )
