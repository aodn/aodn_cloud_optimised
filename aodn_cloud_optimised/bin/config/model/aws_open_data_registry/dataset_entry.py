import enum

import pydantic
import yaml

from .data_at_work import DataAtWork
from .resource import Resource
from .tags import Tag


class ADXCategory(enum.Enum):
    """
    https://github.com/awslabs/open-data-registry/blob/main/adx_categories.yaml
    """

    FINANCIAL_SERVICES = "Financial Services Data"
    RETAIL_LOCATION_MARKETING = "Retail, Location & Marketing Data"
    PUBLIC_SECTOR = "Public Sector Data"
    HEALTHCARE_LIFE_SCIENCES = "Healthcare & Life Sciences Data"
    RESOURCES = "Resources Data"
    MEDIA_ENTERTAINMENT = "Media & Entertainment Data"
    TELECOMMUNICATIONS = "Telecommunications Data"
    ENVIRONMENTAL = "Environmental Data"
    AUTOMOTIVE = "Automotive Data"
    MANUFACTURING = "Manufacturing Data"
    GAMING = "Gaming Data"


class DatasetEntry(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        serialize_by_alias=True,
        use_enum_values=True,
    )

    name: str = pydantic.Field(
        min_length=5,
        max_length=130,
        alias="Name",
        description='The public facing name of the dataset. Spell out acronyms and abbreviations. We do not require "AWS" or "Open Data" to be in the dataset name. Must be between 5 and 130 characters.',
    )
    description: str = pydantic.Field(
        alias="Description",
        description="A high-level description of the dataset. Only the first 600 characters will be displayed on the homepage of the Registry of Open Data on AWS",
    )  # Full text, though homepage only shows 600
    documentation_url: pydantic.AnyHttpUrl = pydantic.Field(
        alias="Documentation",
        description="A link to documentation of the dataset, preferably hosted on the data provider's website or Github repository.",
    )
    contact: str = pydantic.Field(
        alias="Contact",
        description="May be an email address, a link to contact form, a link to GitHub issues page, or any other instructions to contact the producer of the dataset",
    )
    managed_by: str = pydantic.Field(
        alias="ManagedBy",
        description="The name of the laboratory, institution, or organization who is responsible for the data ingest process. Avoid using individuals. If your institution manages several datasets hosted by the Public Dataset Program, please list the managing institution identically. For an example why, check out the Managed By section of the TARGET dataset",
    )
    update_frequency: str = pydantic.Field(
        alias="UpdateFrequency",
        description="An explanation of how frequently the dataset is updated",
    )
    tags: list[Tag] = pydantic.Field(
        alias="Tags",
        description="Select tags that are related to an intrinsic property or descriptor of the dataset. A list of supported tags is maintained in the tags.yaml file in this repo. If you want to recommend a tag that is not included in tags.yaml, please submit a pull request to add it to that file.",
    )
    license: str = pydantic.Field(
        alias="License",
        description="An explanation of the dataset license and/or a URL to more information about data terms of use of the dataset",
    )

    citation: str | None = pydantic.Field(
        alias="Citation",
        description='Custom citation language to be used when citing this dataset, which will be appended to the default citation used for all datasets. Default citation language is as follows: "[DATASET NAME] was accessed on [DATE] at registry.opendata.aws/[dataset]"',
    )
    resources: list[Resource] = pydantic.Field(
        alias="Resources",
        description="A list of AWS resources that users can use to consume the data. See resource.py for more details.",
    )

    data_at_work: DataAtWork = pydantic.Field(
        alias="DataAtWork",
        default_factory=DataAtWork,
        description="A list of links to example tutorials, tools & applications, publications that use the data.",
    )
    deprecated_notice: str | None = pydantic.Field(
        None,
        alias="DeprecatedNotice",
        description="Only appropriate for datasets that are being retired, indicates to users that the dataset will soon be deprecated and should include the date that the dataset will no longer be available.",
    )
    adx_categories: list[ADXCategory] | None = pydantic.Field(
        None,
        max_length=2,
        alias="ADXCategories",
        description="Allowed categories can be found in adx_categories.yaml, at most, 2 can be added. Adding categories to your listing will improve searchability within the AWS Data Exchange.",
    )

    def to_yaml(self) -> str:
        return yaml.safe_dump(self.model_dump(mode="json", exclude_none=True))
