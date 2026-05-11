import enum

import pydantic


class ResourceType(enum.Enum):
    """
    https://github.com/awslabs/open-data-registry/blob/main/resources.yaml
    """

    CLOUDFRONT_DISTRIBUTION = "CloudFront Distribution"
    DB_SNAPSHOT = "DB Snapshot"
    EBS_SNAPSHOT = "EBS Snapshot"
    S3_BUCKET = "S3 Bucket"
    SNS_TOPIC = "SNS Topic"


class Resource(pydantic.BaseModel):
    description: str = pydantic.Field(
        alias="Description",
        description="A technical description of the data available within the AWS resource, including information about file formats and scope.",
    )
    arn: str = pydantic.Field(
        alias="ARN",
        pattern=r"^arn:aws:.*",
        description="Amazon Resource Name for resource, e.g. arn:aws:s3:::commoncrawl",
    )
    region: str = pydantic.Field(
        alias="Region", description="AWS region unique identifier, e.g. us-east-1"
    )
    type: ResourceType = pydantic.Field(
        alias="Type",
        description="Can be CloudFront Distribution, DB Snapshot, S3 Bucket, or SNS Topic. A list of supported resources is maintained in the resources.yaml file in the AWS Open Data Registry Repo: https://github.com/awslabs/open-data-registry/blob/main/resources.yaml",
    )
    requester_pays: bool | None = pydantic.Field(
        default=None,
        alias="RequesterPays",
        description="Only appropriate for Amazon S3 buckets, indicates whether the bucket has Requester Pays enabled or not.",
    )
    account_required: str | None = pydantic.Field(
        alias="AccountRequired",
        default=None,
        description="Is an AWS account required to access this data? Note that while Requester Pays means you will need an account, this is meant for cases where an account is required outside of that scenario.",
    )
    controlled_access: pydantic.AnyHttpUrl | None = pydantic.Field(
        default=None,
        alias="ControlledAccess",
        description="Only appropriate for Amazon S3 buckets with controlled access. Please provide a URL to instructions on how to request and gain access to the S3 bucket.",
    )
    explore: list[pydantic.AnyHttpUrl] | None = pydantic.Field(
        default=None,
        alias="Explore",
        description="Additional links that can be used to explore the bucket resource, i.e. links to S3 JS Explorer index.html for the bucket or the AWS S3 console.",
    )
