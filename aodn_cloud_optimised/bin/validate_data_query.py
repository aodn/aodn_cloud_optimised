#! /usr/var/env python3
import datetime
import re
import subprocess
import sys

import aodn_cloud_optimised.lib.DataQuery

# Regex pattern to find and capture the version string
VERSION_PATTERN = re.compile(r'^__version__\s*=\s*"(.*)"\s*$')


def get_file_content(ref: str, file_path: str) -> str | None:
    """
    Retrieves the content of a specific file at a given git reference.
    """

    result = subprocess.run(
        ["git", "show", f"{ref}:{file_path}"],
        capture_output=True,
        check=True,
        text=True,
    )
    return result.stdout


def extract_version(content: str) -> str | None:
    """
    Extracts the __version__ string from the file content using a regex.
    """

    for line in content.splitlines():
        match = VERSION_PATTERN.match(line.strip())
        if match:
            return match.group(1)

    return None


def ensure_main_branch_available() -> None:
    """
    Ensures origin/main is available locally (for CI).
    """
    try:
        subprocess.run(
            ["git", "rev-parse", "origin/main"],
            capture_output=True,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        try:
            subprocess.run(
                ["git", "fetch", "origin", "main"],
                capture_output=True,
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print(
                "Error: Could not fetch origin/main.",
                file=sys.stderr,
            )
            raise


def main():
    """
    Validation of DataQuery global vars and version enforcement.

    If DataQuery.py was changed, __version__ must be incremented.
    - Local pre-commit: staged version > HEAD version
    - CI: HEAD version > origin/main version
    """

    # Check staged changes (local pre-commit hook)
    staged_diff = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout

    if "aodn_cloud_optimised/lib/DataQuery.py" in staged_diff.split("\n"):
        # Local pre-commit: DataQuery.py is staged, compare staged vs HEAD
        try:
            staged_content = get_file_content(
                ref="", file_path="aodn_cloud_optimised/lib/DataQuery.py"
            )
            staged_version = extract_version(staged_content)
            head_content = get_file_content(
                ref="HEAD", file_path="aodn_cloud_optimised/lib/DataQuery.py"
            )
            head_version = extract_version(head_content)
        except subprocess.CalledProcessError as e:
            print(f"Error reading DataQuery.py: {e}", file=sys.stderr)
            raise

        if not staged_version or not head_version:
            raise ValueError("Could not extract __version__ from DataQuery.py")

        if staged_version <= head_version:
            raise ValueError(
                f"DataQuery.__version__ must be bumped. "
                f"Staged: {staged_version}, HEAD: {head_version}"
            )
        return

    # CI: check committed changes vs origin/main
    ensure_main_branch_available()

    committed_diff = subprocess.run(
        ["git", "diff", "origin/main", "HEAD", "--name-only"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout

    if "aodn_cloud_optimised/lib/DataQuery.py" in committed_diff.split("\n"):
        # CI: DataQuery.py changed, compare HEAD vs origin/main
        try:
            head_content = get_file_content(
                ref="HEAD", file_path="aodn_cloud_optimised/lib/DataQuery.py"
            )
            head_version = extract_version(head_content)
            main_content = get_file_content(
                ref="origin/main", file_path="aodn_cloud_optimised/lib/DataQuery.py"
            )
            main_version = extract_version(main_content)
        except subprocess.CalledProcessError as e:
            print(f"Error reading DataQuery.py: {e}", file=sys.stderr)
            raise

        if not head_version or not main_version:
            raise ValueError("Could not extract __version__ from DataQuery.py")

        if head_version <= main_version:
            raise ValueError(
                f"DataQuery.__version__ must be bumped. "
                f"PR: {head_version}, Main: {main_version}"
            )

    # Check the variables align to expected
    assert aodn_cloud_optimised.lib.DataQuery.REGION == "ap-southeast-2"
    assert (
        aodn_cloud_optimised.lib.DataQuery.ENDPOINT_URL
        == "https://s3.ap-southeast-2.amazonaws.com"
    )
    assert (
        aodn_cloud_optimised.lib.DataQuery.BUCKET_OPTIMISED_DEFAULT
        == "aodn-cloud-optimised"
    )
    assert aodn_cloud_optimised.lib.DataQuery.ROOT_PREFIX_CLOUD_OPTIMISED_PATH == ""
    assert aodn_cloud_optimised.lib.DataQuery.DEFAULT_TIME == datetime.datetime(
        1900, 1, 1
    )

    print(aodn_cloud_optimised.lib.DataQuery.__version__)


if __name__ == "__main__":
    main()
