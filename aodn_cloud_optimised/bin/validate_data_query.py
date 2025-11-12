import datetime
import pathlib
import re
import subprocess

import aodn_cloud_optimised.lib.DataQuery

# Regex pattern to find and capture the version string
VERSION_PATTERN = re.compile(r'^__version__\s*=\s*"(.*)"\s*$')


def get_file_content(ref: str, file_path: str) -> str | None:
    """
    Retrieves the content of a specific file at a given git reference.
    """

    # Use git show to get the file content
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

            # Return the captured group (the version string)
            return match.group(1)

    return None


def main():
    """
    validation of Data Query global vars.

    These often get tinkered with in local development of notebooks.

    We must enforce they are set back to normal before commits.
    """

    # Check DataQuery.py is in the git diff
    stdout = subprocess.run(
        args=["git", "diff", "--cached", "--name-only"],
        check=True,
        text=True,
        capture_output=True,
    ).stdout

    # Exit if DataQuery.py is not in the changes
    if "aodn_cloud_optimised/lib/DataQuery.py" not in stdout.split("\n"):
        exit()

    # Extract versions
    staged_content = get_file_content(
        ref="", file_path="aodn_cloud_optimised/lib/DataQuery.py"
    )
    staged_version = extract_version(staged_content)
    head_content = get_file_content(
        ref="HEAD", file_path="aodn_cloud_optimised/lib/DataQuery.py"
    )
    head_version = extract_version(head_content)

    if staged_version == head_version:
        raise ValueError(
            f"DataQuery.__version__ must be updated. Bump from `{head_version}`"
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
