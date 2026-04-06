import logging
import socket
from pathlib import PurePosixPath
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import boto3
import s3fs
from botocore import UNSIGNED
from botocore.config import Config

logger = logging.getLogger(__name__)


def get_free_local_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))  # 0 tells the OS to find an available port
        return s.getsockname()[1]


def s3fs_from_opts(s3_fs_opts: dict) -> s3fs.S3FileSystem:
    """Create an s3fs filesystem from s3_fs_opts."""
    return s3fs.S3FileSystem(**s3_fs_opts)


def boto3_from_opts_dict(s3_fs_opts: Optional[dict]) -> dict:
    """Convert s3_fs_opts into boto3-compatible keyword arguments."""
    if not s3_fs_opts:
        return {}
    boto_kwargs = {}
    if "key" in s3_fs_opts:
        boto_kwargs["aws_access_key_id"] = s3_fs_opts["key"]
    if "secret" in s3_fs_opts:
        boto_kwargs["aws_secret_access_key"] = s3_fs_opts["secret"]
    if "token" in s3_fs_opts:
        boto_kwargs["aws_session_token"] = s3_fs_opts["token"]
    if "client_kwargs" in s3_fs_opts and "endpoint_url" in s3_fs_opts["client_kwargs"]:
        boto_kwargs["endpoint_url"] = s3_fs_opts["client_kwargs"]["endpoint_url"]
    return boto_kwargs


def boto3_s3_from_opts_dict(s3_fs_opts: Optional[dict]) -> Tuple[str, dict]:
    """
    Return a (service_name, kwargs) tuple for creating a boto3 S3 client.
    """
    return "s3", boto3_from_opts_dict(s3_fs_opts)


def boto3_from_opts(s3_fs_opts: Optional[dict], service_name: str = "s3"):
    """Create a boto3 client from s3_fs_opts."""
    kwargs = boto3_from_opts_dict(s3_fs_opts)
    return boto3.client(service_name, **kwargs)


def s3_ls(
    bucket: str,
    prefix: str,
    suffix: Optional[str] = ".nc",
    s3_path: Optional[bool] = True,
    exclude: Optional[str] = None,
    s3_client_opts: Optional[dict] = None,
) -> list:
    """
    Return a list of object keys under a specific prefix in the specified S3 bucket
    with the specified suffix.

    Args:
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix to filter objects in the S3 bucket.
        suffix (str or None, optional): The suffix to filter object keys (default is '.nc').
                                        Set to None to disable suffix filtering.
        s3_path (bool, optional): Whether to return S3 paths or object keys without the bucket name (default is True).
        s3_client_opts(dict): s3 client dict. Example:
                                s3_client_opts = {
                                    "service_name": "s3",
                                    "region_name": "us-east-1",
                                    "endpoint_url": f"http://{endpoint_ip}:{port}",
                                    }


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
    if s3_client_opts is None:
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    else:
        s3 = boto3.client("s3", **s3_client_opts)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    s3_objs = []

    for page in pages:
        for obj in page.get("Contents", []):
            if exclude and exclude in obj["Key"]:
                continue

            if suffix is None or obj["Key"].endswith(suffix):
                if s3_path:
                    s3_objs.append(f"s3://{bucket}/{obj['Key']}")
                else:
                    s3_objs.append(obj["Key"])

    if not initial_logger.hasHandlers():
        # Restore the original state if no handlers were initially present
        logging.shutdown()

    return s3_objs


def delete_objects_in_prefix(
    bucket_name: str,
    prefix: str,
    s3_client_opts: Optional[Union[Tuple[str, Dict], Dict]] = None,
):
    """
    Delete all objects in an S3 bucket under a specified prefix recursively.

    This function lists all objects under the specified prefix in the given S3 bucket
    and deletes them. It handles paginated results to ensure all objects are deleted,
    processing up to 1000 objects at a time.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix under which to delete objects. This can be a folder
                      or directory-like structure in S3.
        s3_client_opts(dict): s3 client dict. Example:
                                s3_client_opts = {
                                    "service_name": "s3",
                                    "region_name": "us-east-1",
                                    "endpoint_url": f"http://{endpoint_ip}:{port}",
                                    }


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

    # Create boto3 client depending on opts format
    if s3_client_opts is None:
        s3_client = boto3.client("s3")  # default, real AWS
    elif isinstance(s3_client_opts, tuple) and len(s3_client_opts) == 2:
        service_name, kwargs = s3_client_opts
        s3_client = boto3.client(service_name, **kwargs)
    elif isinstance(s3_client_opts, dict):
        # legacy format where service_name is inside the dict
        service_name = s3_client_opts.pop("service_name", "s3")
        s3_client = boto3.client(service_name, **s3_client_opts)
    else:
        raise ValueError(
            "s3_client_opts must be None, a (service_name, kwargs) tuple, or a dict."
        )

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

        response = s3_client.list_objects_v2(**list_kwargs)

        # Check if there are any objects to delete
        if "Contents" not in response:
            logger.info(
                f"No objects found with prefix '{prefix}' in bucket '{bucket_name}'."
            )
            return

        # Collect object keys to delete
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

        # Delete objects
        delete_response = s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={
                "Objects": objects_to_delete,
            },
        )

        deleted = delete_response.get("Deleted", [])
        errors = delete_response.get("Errors", [])

        if deleted:
            logger.info(f"Deleted {len(deleted)} objects.")
        else:
            if errors:
                for err in errors:
                    logger.warning(
                        f"Failed to delete {err.get('Key')}: {err.get('Code')} - {err.get('Message')}"
                    )
                raise RuntimeError(f"Failed to delete {len(errors)} objects.")
            else:
                logger.warning(
                    "S3 delete_objects response did not include 'Deleted'. Possible no objects were deleted."
                )
        # Check if there are more objects to delete
        if response["IsTruncated"]:
            continuation_token = response["NextContinuationToken"]
        else:
            break


def split_s3_path(s3_path: str):
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


def prefix_exists(
    s3_path: str, s3_client_opts: Optional[Union[Tuple[str, Dict], Dict]] = None
):
    """
    Check if a given S3 prefix exists.

    This function parses an S3 path to extract the bucket name and prefix,
    then checks if the prefix exists in the specified S3 bucket.

    Args:
        s3_path (str): The S3 path to check, in the format "s3://bucket-name/prefix".
        s3_client_opts(dict): s3 client dict. Example:
                                s3_client_opts = {
                                    "service_name": "s3",
                                    "region_name": "us-east-1",
                                    "endpoint_url": f"http://{endpoint_ip}:{port}",
                                    }

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

    # Create boto3 client depending on opts format
    if s3_client_opts is None:
        s3_client = boto3.client("s3")  # default, real AWS
    elif isinstance(s3_client_opts, tuple) and len(s3_client_opts) == 2:
        service_name, kwargs = s3_client_opts
        s3_client = boto3.client(service_name, **kwargs)
    elif isinstance(s3_client_opts, dict):
        # legacy format where service_name is inside the dict
        service_name = s3_client_opts.pop("service_name", "s3")
        s3_client = boto3.client(service_name, **s3_client_opts)
    else:
        raise ValueError(
            "s3_client_opts must be None, a (service_name, kwargs) tuple, or a dict."
        )

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

        s3_fs = s3fs.S3FileSystem(
            anon=True,
            default_cache_type="readahead",
            default_fill_cache=False,
            config_kwargs={"max_pool_connections": 30},
        )

    if isinstance(s3_paths, str):
        s3_paths = [s3_paths]

    if not isinstance(s3_paths, list):
        raise ValueError("Invalid input format. Expecting either str or list[str].")

    # Create a fileset by opening each file
    fileset = [s3_fs.open(file) for file in s3_paths]

    return fileset


def discover_parquet_datasets(
    s3_uri: str,
    partitioning: Optional[str],
    bucket_raw: Optional[str],
    s3_fs_opts: Optional[dict] = None,
) -> List[str]:
    """Discover parquet datasets or files in a folder.

    Args:
        s3_uri: S3 path to folder containing parquet sources (can be full s3:// URI or relative path)
        partitioning: "hive" for hive-partitioned datasets, None for flat parquet files
        bucket_raw: Required if s3_uri is not a full S3 URI
        s3_fs_opts: Optional dict with s3fs.S3FileSystem initialization options (e.g., anon, client_kwargs)

    Returns:
        List of S3 paths to parquet datasets/files (as full s3:// URIs)

    Raises:
        ValueError: If no parquet datasets/files found or if path doesn't exist
    """
    # Initialize S3 filesystem
    s3_fs = s3fs.S3FileSystem(**(s3_fs_opts or {}))

    # Parse URI to get bucket and prefix
    if s3_uri.startswith("s3://"):
        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
    else:
        if not bucket_raw:
            raise ValueError(
                "bucket_raw must be provided when s3_uri is not a full S3 URI."
            )
        bucket = bucket_raw
        prefix = s3_uri

    prefix = str(PurePosixPath(prefix))  # normalize path
    full_path = f"{bucket}/{prefix}".rstrip("/")

    logger.info(
        f"Discovering parquet {'datasets' if partitioning == 'hive' else 'files'} in s3://{full_path}"
    )

    # List contents (first level only)
    try:
        entries = s3_fs.ls(full_path, detail=True)
    except FileNotFoundError:
        raise ValueError(f"Path not found: s3://{full_path}")

    discovered = []

    if partitioning == "hive":
        # Find directories ending with .parquet (each is a hive-partitioned dataset)
        for entry in entries:
            if entry["type"] == "directory":
                entry_name = entry["name"].split("/")[-1]
                if entry_name.endswith(".parquet"):
                    # Check if this directory contains hive partitions directly
                    # or if it contains another nested .parquet directory
                    try:
                        nested_entries = s3_fs.ls(entry["name"], detail=True)
                        has_hive_partitions = any(
                            "=" in e["name"].split("/")[-1]
                            for e in nested_entries
                            if e["type"] == "directory"
                        )
                        has_nested_parquet = any(
                            e["name"].split("/")[-1].endswith(".parquet")
                            for e in nested_entries
                            if e["type"] == "directory"
                        )

                        if has_hive_partitions:
                            # Direct hive partitions - use this path
                            discovered.append(f"s3://{entry['name']}")
                            logger.debug(
                                f"  Discovered hive dataset: s3://{entry['name']}"
                            )
                        elif has_nested_parquet:
                            # Nested .parquet directory - look one level deeper
                            for nested_entry in nested_entries:
                                if nested_entry["type"] == "directory" and nested_entry[
                                    "name"
                                ].split("/")[-1].endswith(".parquet"):
                                    discovered.append(f"s3://{nested_entry['name']}")
                                    logger.debug(
                                        f"  Discovered nested hive dataset: s3://{nested_entry['name']}"
                                    )
                        else:
                            # No clear structure - still add it
                            discovered.append(f"s3://{entry['name']}")
                            logger.debug(
                                f"  Discovered hive dataset: s3://{entry['name']}"
                            )
                    except Exception as e:
                        # If we can't list, just add the directory
                        logger.warning(
                            f"  Could not inspect {entry['name']}: {e}, adding as-is"
                        )
                        discovered.append(f"s3://{entry['name']}")
                        logger.debug(f"  Discovered hive dataset: s3://{entry['name']}")
    else:
        # Find files ending with .parquet (flat parquet files)
        for entry in entries:
            if entry["type"] == "file":
                entry_name = entry["name"].split("/")[-1]
                if entry_name.endswith(".parquet"):
                    discovered.append(f"s3://{entry['name']}")
                    logger.debug(f"  Discovered parquet file: s3://{entry['name']}")

    if not discovered:
        raise ValueError(
            f"No parquet {'datasets' if partitioning == 'hive' else 'files'} "
            f"found in s3://{full_path}. Ensure the folder contains "
            f"{'subdirectories ending with .parquet' if partitioning == 'hive' else 'files ending with .parquet'}"
        )

    logger.info(
        f"Discovered {len(discovered)} parquet {'dataset(s)' if partitioning == 'hive' else 'file(s)'}"
    )
    return discovered
