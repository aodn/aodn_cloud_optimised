"""Pydantic models for S3 ``list_objects_v2`` response content.

:see: https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/list_objects_v2.html
"""

import datetime
import typing

import pydantic

ChecksumAlgorithm = typing.Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"]
ChecksumType = typing.Literal["COMPOSITE", "FULL_OBJECT"]
StorageClass = typing.Literal[
    "STANDARD",
    "REDUCED_REDUNDANCY",
    "GLACIER",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "DEEP_ARCHIVE",
    "OUTPOSTS",
    "GLACIER_IR",
    "SNOW",
    "EXPRESS_ONEZONE",
    "FSX_OPENZFS",
    "FSX_ONTAP",
]


class Content(pydantic.BaseModel):
    """Validate one S3 object entry from list_objects_v2.

    :param Key: Object key (path) in the S3 bucket.
    :type Key: str
    :param LastModified: Last modified timestamp.
    :type LastModified: datetime.datetime
    :param ETag: Object ETag value.
    :type ETag: str
    :param ChecksumAlgorithm: Checksum algorithms reported for this object.
    :type ChecksumAlgorithm: list[ChecksumAlgorithm]
    :param ChecksumType: Checksum scope for this object.
    :type ChecksumType: ChecksumType
    :param Size: Object size in bytes.
    :type Size: int
    :param StorageClass: Storage class for the object.
    :type StorageClass: StorageClass
    :raises pydantic.ValidationError: If input does not match the expected schema.
    :return: A validated content model instance.
    :rtype: Content
    """

    Key: str
    LastModified: datetime.datetime
    ETag: str
    ChecksumAlgorithm: list[ChecksumAlgorithm]
    ChecksumType: ChecksumType
    Size: int
    StorageClass: StorageClass


class ListObjectsV2Page(pydantic.BaseModel):
    """Validate one list_objects_v2 page payload.

    :param Contents: Object entries in the response page, defaults to an empty list.
    :type Contents: list[Content], optional
    :raises pydantic.ValidationError: If input does not match the expected schema.
    :return: A validated list-objects page model.
    :rtype: ListObjectsV2Page
    """

    Contents: list[Content] = pydantic.Field(default_factory=list)


if __name__ == "__main__":
    print(
        Content.model_validate(
            {
                "Key": "stored/datauplift/seagrass/seagrass.parquet",
                "LastModified": datetime.datetime(
                    2026, 2, 4, 22, 37, 39, tzinfo=datetime.timezone.utc
                ),
                "ETag": '"63cf1ff253b0d648dd9291f81c968a1d"',
                "ChecksumAlgorithm": ["CRC32"],
                "ChecksumType": "FULL_OBJECT",
                "Size": 58367160,
                "StorageClass": "STANDARD",
            }
        )
    )
