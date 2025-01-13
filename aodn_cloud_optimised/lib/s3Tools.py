import logging
from urllib.parse import urlparse

import boto3
import s3fs
from botocore import UNSIGNED
from botocore.config import Config


def s3_ls(bucket, prefix, suffix=".nc", s3_path=True) -> list:
    """
    Return a list of object keys under a specific prefix in the specified S3 bucket
    with the specified suffix.

    Args:
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix to filter objects in the S3 bucket.
        suffix (str, optional): The suffix to filter object keys (default is '.nc').
        s3_path (bool, optional): Whether to return S3 paths or object keys without the bucket name (default is True).

    Returns:
        list[str]: A list of object keys under the specified prefix and with the specified suffix.
                   If s3_path=True, returns list of S3 paths (s3://bucket_name/key).
                   If s3_path=False, returns list of object keys (key).
    """
    # Store the initial logger state
    initial_logger = logging.getLogger()

    # Check if the root logger already has handlers
    if not initial_logger.hasHandlers():
        # Set up logging configuration if no handlers exist
        logging.basicConfig(level=logging.INFO)  # Set the logging level as needed

    # Get the logger instance
    logger = logging.getLogger()

    logger.info(f"Listing S3 objects in {bucket} under {prefix} ending with {suffix}")

    # DONE: allow S3 connection publicly. Is this a regression doing so?
    # s3 = boto3.client("s3")
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    s3_objs = []

    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                if s3_path:
                    s3_objs.append(f"s3://{bucket}/{obj['Key']}")
                else:
                    s3_objs.append(obj["Key"])

    if not initial_logger.hasHandlers():
        # Restore the original state if no handlers were initially present
        logging.shutdown()

    return s3_objs


def delete_objects_in_prefix(bucket_name, prefix):
    """
    Delete all objects in an S3 bucket under a specified prefix recursively.

    This function lists all objects under the specified prefix in the given S3 bucket
    and deletes them. It handles paginated results to ensure all objects are deleted,
    processing up to 1000 objects at a time.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix under which to delete objects. This can be a folder
                      or directory-like structure in S3.

    Example:
        delete_objects_in_prefix('my-bucket', 'my/prefix/')

    Notes:
        - Ensure that the AWS credentials are configured properly either through
          environment variables, AWS credentials file, or IAM roles.
        - This operation is irreversible. Use with caution as it will permanently
          delete all objects under the specified prefix.
        - The script will print the number of deleted objects in each batch.

    Raises:
        botocore.exceptions.ClientError: If there is an error with the S3 client operation.
    """
    s3 = boto3.client("s3")

    # Get the logger instance
    logger = logging.getLogger()

    # Continuation token for paginated results
    continuation_token = None

    while True:
        # List objects under the specified prefix
        list_kwargs = {
            "Bucket": bucket_name,
            "Prefix": prefix,
        }

        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**list_kwargs)

        # Check if there are any objects to delete
        if "Contents" not in response:
            logger.info(
                f"No objects found with prefix '{prefix}' in bucket '{bucket_name}'."
            )
            return

        # Collect object keys to delete
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

        # Delete objects
        delete_response = s3.delete_objects(
            Bucket=bucket_name,
            Delete={
                "Objects": objects_to_delete,
            },
        )

        logger.info(f"Deleted {len(delete_response['Deleted'])} objects.")

        # Check if there are more objects to delete
        if response["IsTruncated"]:
            continuation_token = response["NextContinuationToken"]
        else:
            break


def split_s3_path(s3_path):
    """
    Split an S3 path into bucket name and prefix.

    Args:
        s3_path (str): The S3 path (e.g., 's3://bucket-name/path/to/object/').

    Returns:
        tuple: A tuple containing the bucket name and prefix.
    """
    parsed_url = urlparse(s3_path)
    bucket_name = parsed_url.netloc
    prefix = parsed_url.path.lstrip("/")
    return bucket_name, prefix


def prefix_exists(s3_path):
    """
    Check if a given S3 prefix exists.

    This function parses an S3 path to extract the bucket name and prefix,
    then checks if the prefix exists in the specified S3 bucket.

    Args:
        s3_path (str): The S3 path to check, in the format "s3://bucket-name/prefix".

    Returns:
        bool: True if the prefix exists, False otherwise.

    Raises:
        ValueError: If the provided path does not appear to be an S3 URL.

    """
    # Parse the S3 path
    parsed_url = urlparse(s3_path)

    if parsed_url.scheme != "s3":
        raise ValueError("The provided path does not appear to be an S3 URL.")

    bucket_name = parsed_url.netloc
    prefix = parsed_url.path.lstrip("/")

    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
    return "Contents" in response


def create_fileset(s3_paths, s3_fs=None):
    """
    Create a fileset from S3 objects specified by a list of full S3 paths.

    Args:
        s3_paths (str or list[str]): Either a single full S3 path (e.g., 's3://bucket_name/object_key')
                                     or a list of full S3 paths.

    Returns:
        list[file-like object]: List of file-like objects representing each object in the fileset.
    """
    if s3_fs is None:
        s3_fs = s3fs.S3FileSystem(anon=True)

    if isinstance(s3_paths, str):
        s3_paths = [s3_paths]

    if not isinstance(s3_paths, list):
        raise ValueError("Invalid input format. Expecting either str or list[str].")

    # Create a fileset by opening each file
    fileset = [s3_fs.open(file) for file in s3_paths]

    return fileset
