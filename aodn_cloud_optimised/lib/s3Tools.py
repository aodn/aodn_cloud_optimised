import boto3
from urllib.parse import urlparse


def s3_ls(bucket, prefix, suffix=".nc") -> list:
    """
    Return a list of object keys under a specific prefix in the specified S3 bucket
    with the specified suffix.

    Args:
        prefix (str): The prefix to filter objects in the S3 bucket.
        bucket (str): The name of the S3 bucket.
        suffix (str, optional): The suffix to filter object keys (default is '.nc').

    Returns:
        list[str]: A list of object keys under the specified prefix and with the specified suffix.
    """
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    s3_obj = []

    for page in pages:
        for object in page["Contents"]:
            if object["Key"].endswith(suffix):
                s3_obj.append(object["Key"])

    s3.close()
    return s3_obj


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
            print(f"No objects found with prefix '{prefix}' in bucket '{bucket_name}'.")
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

        print(f"Deleted {len(delete_response['Deleted'])} objects.")

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
