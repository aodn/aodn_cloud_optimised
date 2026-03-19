import typing
import urllib.parse

import boto3
import pydantic

from aodn_cloud_optimised.bin.orchestrate import Content, ListObjectsV2Page


class BucketBlock(typing.Protocol):
    """Structural interface for an S3 bucket configuration block.

    Any object exposing ``bucket_name`` and ``bucket_folder`` satisfies this
    protocol, including ``prefect_aws.S3Bucket`` blocks as well as plain
    dataclasses or named tuples used in non-Prefect contexts.
    """

    bucket_name: str
    bucket_folder: str | None


class FileCollector(pydantic.BaseModel):
    """Strategy interface for collecting input file paths.

    Provides methods to collect S3 object paths from either S3 URIs or
    Prefect S3 block configurations.
    """

    bucket: str
    prefix: str | None = None
    suffix: str | None = None

    @staticmethod
    def _parse_s3_uri(s3_uri: str) -> tuple[str, str | None]:
        """Parse S3 URI and return bucket and prefix.

        :param s3_uri: Full S3 URI in the format s3://bucket/prefix
        :type s3_uri: str
        :raises ValueError: If the URI scheme is not 's3' or netloc is empty
        :return: Tuple containing bucket name and prefix (or None if no prefix)
        :rtype: tuple[str, str | None]
        """

        # Pre-Process
        s3_uri = s3_uri.rstrip("/")

        # Parse
        parsed_s3_uri = urllib.parse.urlsplit(s3_uri)

        # Check the scheme
        if parsed_s3_uri.scheme != "s3" or not parsed_s3_uri.netloc:
            raise ValueError(
                f"Expected a full S3 URI (s3://bucket/prefix), got: {s3_uri!r}"
            )

        # Get the bucket and prefix
        bucket = parsed_s3_uri.netloc
        prefix = parsed_s3_uri.path.lstrip("/")

        return bucket, prefix or None

    @staticmethod
    def _parse_s3_block(
        s3_bucket_block: BucketBlock,
        path: str | None,
    ) -> tuple[str, str | None]:
        """Parse S3 block and return bucket and prefix.

        Combines bucket_folder from the S3 block with the provided path to
        determine the final prefix for S3 operations.

        :param s3_bucket_block: S3 bucket block configuration (any object with
            ``bucket_name`` and ``bucket_folder`` attributes)
        :type s3_bucket_block: BucketBlock
        :param path: Optional S3 path to append to the bucket folder, defaults to None
        :type path: str | None
        :return: Tuple containing bucket name and combined prefix (or None if neither bucket_folder nor path)
        :rtype: tuple[str, str | None]
        """

        # Get the bucket name and bucket folder
        bucket_name = s3_bucket_block.bucket_name
        bucket_folder = s3_bucket_block.bucket_folder

        # Pre-Process
        bucket_folder = bucket_folder.rstrip("/") if bucket_folder else None
        path = path.strip("/") if path else None

        # Determine prefix based on bucket_folder and path combination
        match (bucket_folder, path):
            case (None, None):
                prefix = None
            case (None, path):
                prefix = path
            case (bucket_folder, None):
                prefix = bucket_folder
            case (bucket_folder, path):
                prefix = f"{bucket_folder}/{path}"

        return bucket_name, prefix

    @staticmethod
    def _list_bucket(
        bucket: str,
        prefix: str | None = None,
        suffix: str | None = None,
    ) -> list[Content]:
        """List S3 bucket objects with optional prefix and suffix filtering.

        Uses paginated S3 API calls to retrieve all objects in bucket and
        optionally filters by suffix.

        :param bucket: S3 bucket name
        :type bucket: str
        :param prefix: Optional S3 prefix to limit results, defaults to None
        :type prefix: str | None, optional
        :param suffix: Optional file suffix to filter results, defaults to None
        :type suffix: str | None, optional
        :return: List of S3 Content objects matching the criteria
        :rtype: list[Content]
        """

        # Set up bucket iterator
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
        )

        # Validate and paginate
        contents: list[Content] = []
        for page in pages:
            parsed_page = ListObjectsV2Page.model_validate(page)
            contents.extend(parsed_page.Contents)

        # Filter the suffix if provided
        if suffix is not None:
            contents = [content for content in contents if content.Key.endswith(suffix)]

        return contents

    def collect(self) -> list[str]:
        """Collect and format S3 object paths from the configured bucket.

        Uses the bucket, prefix, and suffix attributes to list S3 objects
        and formats them as full S3 URIs.

        :return: List of full S3 URIs for all objects found
        :rtype: list[str]
        """
        contents = self._list_bucket(self.bucket, self.prefix, self.suffix)
        return self._format_s3_uris(self.bucket, contents)

    @staticmethod
    def _format_s3_uris(bucket: str, contents: list[Content]) -> list[str]:
        """Format Content objects into full S3 URIs.

        :param bucket: S3 bucket name
        :type bucket: str
        :param contents: List of S3 Content objects
        :type contents: list[Content]
        :return: List of full S3 URIs
        :rtype: list[str]
        """
        return [f"s3://{bucket}/{item.Key}" for item in contents]

    @classmethod
    def from_s3_bucket_block(
        cls,
        s3_bucket_block: BucketBlock,
        path: str | None = None,
        suffix: str | None = None,
    ) -> typing.Self:
        """Collect S3 object paths from a bucket block.

        :param s3_bucket_block: S3 bucket block configuration (any object with
            ``bucket_name`` and ``bucket_folder`` attributes, such as a
            ``prefect_aws.S3Bucket`` block)
        :type s3_bucket_block: BucketBlock
        :param path: Optional S3 path to append to the block's bucket folder, defaults to None
        :type path: str | None, optional
        :return: FileCollector configured with the resolved bucket and prefix
        :rtype: FileCollector
        """
        bucket, prefix = cls._parse_s3_block(s3_bucket_block, path)
        return cls(
            bucket=bucket,
            prefix=prefix,
            suffix=suffix,
        )

    @classmethod
    def from_s3_uri(
        cls,
        s3_uri: str,
        suffix: str | None = None,
    ) -> typing.Self:
        """Collect S3 object paths from an S3 URI.

        :param s3_uri: Full S3 URI in the format s3://bucket/prefix
        :type s3_uri: str
        :return: List of full S3 URIs for all objects found
        :rtype: list[str]
        """
        bucket, prefix = cls._parse_s3_uri(s3_uri)
        return cls(
            bucket=bucket,
            prefix=prefix,
            suffix=suffix,
        )
