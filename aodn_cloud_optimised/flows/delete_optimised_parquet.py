"""
Prefect flow for deleting cloud-optimised parquet files that correspond to
source files which have been removed from the source fileset.

Flow architecture:
+-----------------------------------------------------------------+
|                   flow: delete_optimised_parquet                |
+-----------------------------------------------------------------+
                                 |
                                 v
+-----------------------------------------------------------------+
|                   task: list_dataset_bucket                     |
|  Lists all S3 object paths under `dataset_s3_path` using boto3  |
+-----------------------------------------------------------------+
                                 |
                                 v  list[cloudpathlib.S3Path]
+-----------------------------------------------------------------+
|             task: find_matched_delete_s3_paths                  |
|  Filters candidate paths against `delete_file_names` + regex    |
+-----------------------------------------------------------------+
                                 |
                                 v  matched_paths
+-----------------------------------------------------------------+
|                   task: delete_matched_files                    |
|  Deletes matched files in batches of 1,000 (or previews if dry) |
+-----------------------------------------------------------------+

Notes:
- Restricted to paths ending in `.parquet`.
- Deletes are only allowed against the environment's `optimised-bucket` block.
- Callers are responsible for resolving the S3 paths; no other Prefect block
  coupling beyond the `optimised-bucket` input validation.
"""

import re

import boto3
import cloudpathlib
import polars
import prefect
import prefect.flow_runs
import prefect.input
import prefect_aws


class ProceedInput(prefect.input.RunInput):
    """Required to attach a description to the interactive pause."""

    proceed: bool


@prefect.task
def list_dataset_bucket(
    dataset_s3_path: cloudpathlib.S3Path,
) -> list[cloudpathlib.S3Path]:
    """List all S3 object paths under the given dataset prefix.

    Uses S3 pagination so arbitrarily large prefixes are handled correctly.

    Args:
        dataset_s3_path: The root S3 path/prefix to search.

    Returns:
        A list of :class:`cloudpathlib.S3Path` instances for every object
        found under the prefix.
    """
    logger = prefect.get_run_logger()
    client = boto3.client("s3")

    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=dataset_s3_path.bucket,
        Prefix=dataset_s3_path.key,
        PaginationConfig={"PageSize": 1000},
    )

    dataset_s3_paths: list[cloudpathlib.S3Path] = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            s3_uri = f"s3://{dataset_s3_path.bucket}/{obj['Key']}"
            dataset_s3_paths.append(dataset_s3_path.client.S3Path(s3_uri))

    logger.info(f"found {len(dataset_s3_paths)} paths under {dataset_s3_path}")
    return dataset_s3_paths


@prefect.task
def find_matched_delete_s3_paths(
    dataset_s3_paths: list[cloudpathlib.S3Path],
    delete_file_names: list[str],
    pattern_template: str = r"-\d+\.parquet$",
) -> list[cloudpathlib.S3Path]:
    """Filter dataset S3 paths for files matching targeted file names.

    Builds a single combined regex from ``delete_file_names`` and
    ``pattern_template``, then applies it to every URI in
    ``dataset_s3_paths``.

    Args:
        dataset_s3_paths: Candidate :class:`~cloudpathlib.S3Path` instances.
        delete_file_names: Base names (or identifiers) of the source files
            that have been removed.
        pattern_template: Regex suffix applied alongside each base name to
            identify the matching parquet shards.  Defaults to
            ``r"-\\d+\\.parquet$"``.

    Returns:
        A list of :class:`~cloudpathlib.S3Path` instances that match the
        deletion criteria.
    """
    logger = prefect.get_run_logger()

    # Safely escape input file names for regex
    escaped_names = [re.escape(name) for name in delete_file_names]

    # Build a single combined regex OR pattern, e.g.:
    #   (file1|file2|file3)-\d+\.parquet$
    combined_pattern = f"({'|'.join(escaped_names)}){pattern_template}"

    df = polars.DataFrame(
        data={
            "s3_path": dataset_s3_paths,
            "s3_uri": [p.as_uri() for p in dataset_s3_paths],
        }
    )
    matched_df = df.filter(
        polars.col("s3_uri").str.contains(pattern=combined_pattern)
    )
    matched_delete_s3_paths: list[cloudpathlib.S3Path] = matched_df[
        "s3_path"
    ].to_list()

    logger.info(
        f"found {len(matched_delete_s3_paths)} matches out of "
        f"{len(dataset_s3_paths)} candidate paths"
    )
    return matched_delete_s3_paths


def _chunked(iterable: list, size: int = 1000):
    """Yield successive ``size``-length slices from *iterable*."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


@prefect.task
def delete_matched_files(
    dataset_s3_path: cloudpathlib.S3Path,
    matched_delete_s3_paths: list[cloudpathlib.S3Path],
    pause_flow_run: bool = True,
    dryrun: bool = True,
) -> None:
    """Delete matched S3 objects in batches of up to 1,000.

    The bucket name is taken from ``dataset_s3_path``.  If ``dryrun`` is
    ``True`` the planned deletions are logged but no S3 API call is made.
    When ``pause_flow_run`` is ``True`` the flow is paused and the operator
    must confirm before any deletion proceeds.

    Args:
        dataset_s3_path: Primary S3 path defining the target bucket.
        matched_delete_s3_paths: S3 paths to delete.
        pause_flow_run: When ``True``, pause the flow and wait for operator
            confirmation before deleting.  Defaults to ``True``.
        dryrun: When ``True``, log planned deletions without making API
            calls.  Defaults to ``True``.
    """
    logger = prefect.get_run_logger()

    if not matched_delete_s3_paths:
        logger.info("No files matched for deletion.")
        return

    if pause_flow_run:
        description_md = (
            f"## Proceed\n"
            f"Do you wish to proceed with the deletion of "
            f"`{len(matched_delete_s3_paths)}` files?\n"
            f"## Sample\n"
            f"{matched_delete_s3_paths[:5]}"
        )
        prefect.flow_runs.pause_flow_run(
            wait_for_input=ProceedInput.with_initial_data(
                description=description_md,
                proceed=False,
            ),
            timeout=600,  # 10 minutes
        )

    if dryrun:
        logger.info("`dryrun` mode active; exiting without deletion.")
        return

    client = boto3.client("s3")
    keys = [p.key for p in matched_delete_s3_paths]
    total_deleted = 0

    for batch_keys in _chunked(keys, size=1000):
        response = client.delete_objects(
            Bucket=dataset_s3_path.bucket,
            Delete={
                "Objects": [{"Key": key} for key in batch_keys],
                "Quiet": True,
            },
        )

        errors = response.get("Errors", [])
        if errors:
            logger.error(
                f"Failed to delete {len(errors)} objects in bucket "
                f"'{dataset_s3_path.bucket}': {errors}"
            )
            raise RuntimeError(
                f"S3 batch deletion encountered errors in bucket "
                f"'{dataset_s3_path.bucket}'."
            )

        total_deleted += len(batch_keys)

    logger.info(
        f"Successfully deleted {total_deleted} files from bucket "
        f"'{dataset_s3_path.bucket}'."
    )


@prefect.flow
def delete_optimised_parquet(
    dataset_s3_path: cloudpathlib.S3Path,
    delete_file_names: list[str],
    dryrun: bool = True,
    pause_flow_run: bool = True,
) -> None:
    """Orchestrate scanning, filtering, and batch deleting unwanted parquet files.

    Input validation enforces that:

    * ``dataset_s3_path`` targets the environment's ``optimised-bucket``
      Prefect block.
    * ``dataset_s3_path`` ends with ``.parquet`` (restricts operation to
      parquet datasets).

    Args:
        dataset_s3_path: Root :class:`~cloudpathlib.S3Path` location for the
            dataset (must end with ``.parquet``).
        delete_file_names: Base names of the source files that have been
            removed and whose corresponding parquet shards should be deleted.
        dryrun: When ``True`` (default), preview matched files without
            performing any deletions.
        pause_flow_run: When ``True`` (default), pause and request operator
            confirmation before deleting.
    """
    # --- Input validation ---------------------------------------------------
    valid_bucket = prefect_aws.S3Bucket.load(name="optimised-bucket")
    if dataset_s3_path.bucket != valid_bucket.bucket_name:
        raise RuntimeError(
            f"expecting `dataset_s3_path` to have bucket "
            f"`{valid_bucket.bucket_name}`: got `{dataset_s3_path.bucket}`"
        )
    if not dataset_s3_path.key.strip("/").endswith(".parquet"):
        raise RuntimeError(
            f"expecting `dataset_s3_path` to end with `.parquet`: "
            f"got `{dataset_s3_path.suffix}`"
        )
    # ------------------------------------------------------------------------

    dataset_s3_paths = list_dataset_bucket(dataset_s3_path=dataset_s3_path)

    matched_delete_s3_paths = find_matched_delete_s3_paths(
        dataset_s3_paths=dataset_s3_paths,
        delete_file_names=delete_file_names,
    )

    delete_matched_files(
        dataset_s3_path=dataset_s3_path,
        matched_delete_s3_paths=matched_delete_s3_paths,
        pause_flow_run=pause_flow_run,
        dryrun=dryrun,
    )
