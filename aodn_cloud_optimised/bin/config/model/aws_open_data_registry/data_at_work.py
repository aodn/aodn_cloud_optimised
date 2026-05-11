import enum

import pydantic


class AWSService(enum.Enum):
    """
    https://github.com/awslabs/open-data-registry/blob/main/services.yaml
    """

    APPSTREAM_2_0 = "Amazon AppStream 2.0"
    ATHENA = "Amazon Athena"
    APPSYNC = "AWS AppSync"
    BATCH = "AWS Batch"
    CLOUDFORMATION = "AWS CloudFormation"
    COMPREHEND = "Amazon Comprehend"
    DYNAMODB = "Amazon DynamoDB"
    EC2 = "Amazon EC2"
    ECR = "Amazon ECR"
    EKS = "Amazon EKS"
    EMR = "Amazon EMR"
    FARGATE = "AWS Fargate"
    FSX = "Amazon FSx"
    GLUE = "AWS Glue"
    HEALTHLAKE = "AWS HealthLake"
    KENDRA = "Amazon Kendra"
    LAKE_FORMATION = "AWS Lake Formation"
    LAMBDA = "AWS Lambda"
    NEPTUNE = "Amazon Neptune"
    PARALLELCLUSTER = "AWS ParallelCluster"
    Q = "Amazon Q"
    QUICKSIGHT = "Amazon QuickSight"
    RDS = "Amazon RDS"
    REDSHIFT = "Amazon Redshift"
    S3 = "Amazon S3"
    S3_GLACIER = "Amazon S3 Glacier"
    SAGEMAKER = "Amazon SageMaker"
    SAGEMAKER_STUDIO_LAB = "Amazon SageMaker Studio Lab"
    SNS = "Amazon SNS"
    STEP_FUNCTIONS = "AWS Step Functions"


class DataAtWorkBase(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        serialize_by_alias=True,
    )

    """Common pydantic.Fields for all DataAtWork entries."""

    title: str = pydantic.Field(
        alias="Title",
        description="The title of the tutorial, tool, application, or publication that uses the data.",
    )
    url: pydantic.AnyHttpUrl = pydantic.Field(
        alias="URL",
        description="A link to the tutorial, tool, application, or publication that uses the data.",
    )
    author_name: str = pydantic.Field(
        alias="AuthorName",
        description="Name(s) of person or entity that created the tutorial, tool, application, or publication. Limit scientific publication author lists to the first six authors in the format Last Name First Initial, followed by 'et al'.",
    )
    author_url: pydantic.AnyHttpUrl | None = pydantic.Field(
        default=None,
        alias="AuthorURL",
        description="URL for person or entity that created the tutorial, tool, application, or publication.",
    )


class Publication(DataAtWorkBase): ...


class ToolOrApplication(DataAtWorkBase): ...


class Tutorial(DataAtWorkBase):
    model_config = pydantic.ConfigDict(
        serialize_by_alias=True,
        use_enum_values=True,
    )
    notebook_url: pydantic.AnyHttpUrl | None = pydantic.Field(
        default=None, alias="NotebookURL"
    )
    services: list[AWSService] | None = pydantic.Field(
        alias="Services", default=None, description="AWS Services applied."
    )


class DataAtWork(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        serialize_by_alias=True,
        populate_by_name=True,
    )

    tutorials: list[Tutorial] = pydantic.Field(
        alias="Tutorials",
        default_factory=list,
        description="A list of links to example tutorialsthat use the data.",
    )
    tools_and_applications: list[ToolOrApplication] = pydantic.Field(
        validation_alias="Tools & Applications",
        serialization_alias="Tools & Applications",
        default_factory=list,
        description="A list of links to example tools & applicationsthat use the data.",
    )
    publications: list[Publication] = pydantic.Field(
        alias="Publications",
        default_factory=list,
        description="A list of links to example publications that use the data.",
    )
