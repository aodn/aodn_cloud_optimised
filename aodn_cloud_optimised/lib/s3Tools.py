import boto3


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
