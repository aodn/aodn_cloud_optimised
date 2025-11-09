#!/usr/bin/env python3
"""
Script to generate a dataset configuration from a NetCDF file stored in S3.

This script performs the following tasks:

1. Generates a JSON schema from the NetCDF file located in the S3 bucket.
2. Reads and merges the validation schema template with the generated schema.
3. Populates the dataset configuration with additional metadata including dataset name, metadata UUID, logger name,
   cloud-optimised format, cluster options, and batch size.
4. Writes the dataset configuration to the module path as a JSON file. READY TO BE ADDED TO GITHUB
5. A script is also created under the bin folder of the module (to be updated manually) and the entry is added to the pyproject.toml file
6. Create a Jupyter Notebook from template (TO BE MODIFIED)
7. Updates the notebooks/README.md with a new entry
8. Optionally, fills up the AWS registry with Geonetwork metadata if a UUID is provided.

Usage:
    cloud_optimised_create_dataset_config -f <NetCDF file object key> -c <cloud optimised format> -d <dataset name> [-b <S3 bucket name>] [-u <Geonetwork Metadata UUID>]

Arguments:
    -f, --file: Object key for the NetCDF file (required).
    -b, --bucket: S3 bucket name (optional, defaults to the value from config).
    -c, --cloud-format: Cloud optimised format, either "zarr" or "parquet" (required).
    -u, --uuid: Geonetwork Metadata UUID (optional).
    -d, --dataset-name: Name of the dataset (required, no spaces or underscores).

Example:
    cloud_optimised_create_dataset_config \
        -f IMOS/SOOP/SOOP-TRV/VMQ9273_Solander/By_Cruise/Cruise_START-20100225T073727Z_END-20100225T131607Z/chlorophyll/IMOS_SOOP-TRV_B_20100225T073727Z_VMQ9273_FV01_END-20100225T131607Z.nc \
        -d vessel_trv_realtime_qc \
        -u 8af21108-c535-43bf-8dab-c1f45a26088c \
        -c parquet
"""

import argparse
import importlib.resources
import importlib.util
import json
import os
import pathlib
import uuid
from collections import OrderedDict
from importlib.resources import files

import nbformat
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from s3path import PureS3Path
from termcolor import colored

from aodn_cloud_optimised.bin.create_aws_registry_dataset import (
    populate_dataset_config_with_geonetwork_metadata,
)
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
    merge_dicts,
)
from aodn_cloud_optimised.lib.schema import (
    generate_json_schema_from_s3_netcdf,
    nullify_netcdf_variables,
)

TO_REPLACE_PLACEHOLDER = "FILL UP MANUALLY - CHECK DOCUMENTATION"


def validate_dataset_name(value):
    """
    Validate the provided dataset name.

    Args:
        value (str): The dataset name to validate.

    Returns:
        str: The validated dataset name.

    Raises:
        argparse.ArgumentTypeError: If the dataset name is not a string.
        argparse.ArgumentTypeError: If the dataset name contains spaces.
        argparse.ArgumentTypeError: If the dataset name does not contain at least one underscore.
    """
    if not isinstance(value, str):
        raise argparse.ArgumentTypeError("Dataset name must be a string.")
    if " " in value:
        raise argparse.ArgumentTypeError("Dataset name must not contain spaces.")
    if "_" not in value:
        raise argparse.ArgumentTypeError(
            "Dataset name must contain at least one underscore."
        )
    return value


def validate_uuid(value):
    """
    Validate the provided UUID.

    Args:
        value (str): The UUID string to validate.

    Returns:
        str: The validated UUID string.

    Raises:
        argparse.ArgumentTypeError: If the UUID string is not in a valid format.
    """
    try:
        uuid.UUID(value)
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid UUID format.")
    return value


def generate_template_value(schema):
    """
    Generate a template value based on the provided JSON schema.

    Args:
        schema (dict): The JSON schema to generate a template value for.

    Returns:
        Any: The generated template value based on the schema type. Possible types include:
            - str: A placeholder string for manual filling.
            - int: An integer value of 0.
            - bool: A boolean value of False.
            - list: A list containing a template value based on the array items' schema.
            - dict: An OrderedDict with keys and template values based on the object properties' schema.
            - None: If the schema type is not recognized.
    """
    schema_type = schema["type"]

    if schema_type == "string":
        return TO_REPLACE_PLACEHOLDER
    elif schema_type == "integer":
        return 0
    elif schema_type == "boolean":
        return False
    elif schema_type == "array":
        return [generate_template_value(schema["items"])]
    elif schema_type == "object":
        return OrderedDict(
            (key, generate_template_value(subschema))
            for key, subschema in schema.get("properties", {}).items()
        )

        # return {key: generate_template_value(subschema) for key, subschema in schema.get('properties', {}).items()}
    else:
        return None


def generate_template(schema):
    """
    Generate a template dictionary based on the provided schema.

    Args:
        schema (dict): The schema dictionary containing a "properties" key,
                       where each value is a subschema defining the structure of the template.

    Returns:
        dict: A dictionary with keys from the schema's "properties", and values generated
              using the `generate_template_value` function based on the subschema.

    Notes:
        - The template values are generated using the `generate_template_value` function.
    """
    template = {}
    for key, subschema in schema["properties"].items():
        # print(f"debug - {key} \n {subschema}")
        template[key] = generate_template_value(subschema)

    return template


def get_module_path():
    """
    Retrieve the file system path of the 'aodn_cloud_optimised' module.

    Returns:
        str: The file system path to the 'aodn_cloud_optimised' module.

    Raises:
        ModuleNotFoundError: If the module 'aodn_cloud_optimised' is not found.
    """
    module_name = "aodn_cloud_optimised"
    spec = importlib.util.find_spec(module_name)
    module_path = spec.submodule_search_locations[0]

    return module_path


def create_dataset_script_deprecated(dataset_name, dataset_json, nc_file_path, bucket):
    """
    Create a Python script to run a cloud optimised creation command with the specified parameters.

    Args:
        dataset_name (str): The name of the dataset to be used in the script file name.
        dataset_json (str): The path to the dataset configuration JSON file.
        nc_file_path (str): The path to the NetCDF file (used to determine the directory path).
        bucket (str): The name of the S3 bucket where the NetCDF file is located.

    Returns:
        str: The file system path to the created script.
    """
    script_content = """#!/usr/bin/env python3
import subprocess
import os


def main():
    config_name = os.path.splitext(os.path.basename(__file__))[0]
    command = ["generic_cloud_optimised_creation", "--config", config_name]

    subprocess.run(command, check=True)
    """

    module_path = get_module_path()

    script_path = os.path.join(module_path, "bin", f"{dataset_name}.py")
    with open(script_path, "w") as script_file:
        script_file.write(script_content)
    os.chmod(script_path, 0o755)  # Make the script executable
    return script_path


def create_dataset_script(dataset_name, dataset_json, nc_file_path, bucket):
    """
    Create a symlink to generic_launcher.py named after the dataset in the bin folder.

    Args:
        dataset_name (str): The name of the dataset to be used in the symlink file name.
        dataset_json (str): Unused, kept for compatibility.
        nc_file_path (str): Unused, kept for compatibility.
        bucket (str): Unused, kept for compatibility.

    Returns:
        str: The file system path to the created symlink.
    """
    module_path = get_module_path()
    bin_dir = os.path.join(module_path, "bin")
    os.makedirs(bin_dir, exist_ok=True)

    symlink_path = os.path.join(bin_dir, f"{dataset_name}.py")
    target_name = "generic_launcher.py"  # relative path within bin directory

    # Remove existing symlink/file if exists
    if os.path.exists(symlink_path) or os.path.islink(symlink_path):
        os.remove(symlink_path)

    # Create relative symlink (the symlink will point to "generic_launcher.py" relative to its own location)
    os.symlink(target_name, symlink_path)
    return symlink_path


def update_pyproject_toml(dataset_name):
    """
    Update the `pyproject.toml` file to include a new script entry under the `[tool.poetry.scripts]` section.

    Args:
        dataset_name (str): The name of the dataset to be used for the new script entry.

    Notes:
        - If the `[tool.poetry.scripts]` section is not present in the `pyproject.toml`, it will be created.
        - If the script entry already exists, no changes will be made.
        - The script entries will be sorted alphabetically.
    """
    # Locate the pyproject.toml file in the parent directory of the module path
    module_path = get_module_path()
    pyproject_path = os.path.abspath(
        os.path.join(module_path, os.pardir, "pyproject.toml")
    )
    script_entry = f'cloud_optimised_{dataset_name} = "aodn_cloud_optimised.bin.{dataset_name}:main"\n'

    with open(pyproject_path, "r") as pyproject_file:
        lines = pyproject_file.readlines()

    # Initialize variables
    scripts_section = []
    in_scripts_section = False
    section_found = False

    for line in lines:
        if line.strip() == "[tool.poetry.scripts]":
            in_scripts_section = True
            section_found = True
            scripts_section.append(line)
            continue
        if in_scripts_section:
            if line.strip() == "" or line.startswith("["):
                in_scripts_section = False
            else:
                scripts_section.append(line)

    # Check if the script entry already exists
    entry_exists = any(script_entry.strip() in line for line in scripts_section)
    if entry_exists:
        return

    # Add the new script entry
    scripts_section.append(script_entry)

    # Sort the script entries alphabetically
    sorted_scripts = [scripts_section[0]] + sorted(
        scripts_section[1:], key=lambda x: x.split("=")[0].strip()
    )

    # Write the updated pyproject.toml
    with open(pyproject_path, "w") as pyproject_file:
        in_scripts_section = False
        for line in lines:
            if line.strip() == "[tool.poetry.scripts]":
                in_scripts_section = True
                pyproject_file.write(line)
                for script in sorted_scripts[1:]:
                    pyproject_file.write(script)
            elif in_scripts_section and (line.strip() == "" or line.startswith("[")):
                in_scripts_section = False
                pyproject_file.write(line)
            elif not in_scripts_section:
                pyproject_file.write(line)

        # If the [tool.poetry.scripts] section was not found, add it at the end
        if not section_found:
            pyproject_file.write("\n[tool.poetry.scripts]\n")
            for script in sorted_scripts[1:]:
                pyproject_file.write(script)


def update_readme(dataset_name):
    """
    Update the `README.md` file to include a new notebook entry under the "AODN Notebooks" section.

    Args:
        dataset_name (str): The name of the dataset to be used for the new notebook entry.

    Notes:
        - If the "AODN Notebooks" section is not present in the `README.md`, it will be created.
        - If the notebook entry already exists, no changes will be made.
        - The notebook entries will be sorted alphabetically.
    """
    # Locate the README file
    module_path = get_module_path()
    readme_path = os.path.abspath(
        os.path.join(module_path, os.pardir, "notebooks", "README.md")
    )
    notebook_url = f"- [{dataset_name}.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/{dataset_name}.ipynb)\n"

    with open(readme_path, "r") as readme_file:
        lines = readme_file.readlines()

    # Initialize variables
    notebooks_section = []
    in_notebooks_section = False

    for line in lines:
        if line.strip() == "# AODN Notebooks":
            in_notebooks_section = True
            notebooks_section.append(line)
            continue
        if in_notebooks_section:
            if line.strip() == "" or line.startswith("#"):
                in_notebooks_section = False
            else:
                notebooks_section.append(line)

    # Check if the notebook entry already exists
    entry_exists = any(notebook_url.strip() in line for line in notebooks_section)
    if entry_exists:
        return

    # Add the new notebook entry
    notebooks_section.append(notebook_url)

    # Sort the notebook entries alphabetically
    sorted_notebooks = [notebooks_section[0]] + sorted(
        notebooks_section[1:], key=lambda x: x.split(".ipynb")[0].strip()
    )

    # Write the updated README.md
    with open(readme_path, "w") as readme_file:
        in_notebooks_section = False
        for line in lines:
            if line.strip() == "# AODN Notebooks":
                in_notebooks_section = True
                readme_file.write(line)
                for notebook in sorted_notebooks[1:]:
                    readme_file.write(notebook)
            elif in_notebooks_section and (line.strip() == "" or line.startswith("#")):
                in_notebooks_section = False
                readme_file.write(line)
            elif not in_notebooks_section:
                readme_file.write(line)

        # If the # AODN Notebooks section was not found, add it at the end
        if not any("# AODN Notebooks" in line for line in lines):
            readme_file.write("\n# AODN Notebooks\n")
            for notebook in sorted_notebooks[1:]:
                readme_file.write(notebook)


def next_steps(dataset_name):
    """
    Print the next steps for working with the new dataset, including tasks related to configuration files, scripts, notebooks, and AWS Registry.

    Args:
        dataset_name (str): The name of the dataset for which the next steps are to be printed.

    """
    json_config_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("config")
        .joinpath("dataset")
        .joinpath(f"{dataset_name}.json")
    )

    script_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("bin")
        .joinpath(f"{dataset_name}.py")
    )

    notebook_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("../")
        .joinpath("notebooks")
        .joinpath(f"{dataset_name}.ipynb")
    )

    # Title
    print(colored("What to do next?", "white", "on_red", attrs=["bold", "underline"]))
    print("")

    # Step 1
    print(
        colored(
            "1) Do a `git status` to see the new/modified files",
            "yellow",
            attrs=["bold"],
        )
    )
    print("")

    # Step 2
    print(
        colored(
            f"2) Open the new dataset configuration JSON file ",
            "yellow",
            attrs=["bold"],
        )
        + colored(f"{json_config_path}", "cyan", attrs=["bold"])
    )
    print(
        colored("   2.1) Update all occurrences of ", "yellow")
        + colored(f"'{TO_REPLACE_PLACEHOLDER}'", "cyan", attrs=["bold"])
    )
    print(
        colored(
            "   2.2) Check AWS registry metadata in the dataset configuration", "yellow"
        )
    )
    print(
        colored(
            "   2.3) The above can be done semi-auto with the Excel IMOSPortalcollections on teams (exported as CSV), and called with `cloud_optimised_create_aws_registry_dataset -a -c IMOSPortalcollections(CloudOptimisedPublication).csv`",
            "yellow",
        )
    )
    print("")

    # Step 3
    print(
        colored("3) Open the newly created python script ", "yellow", attrs=["bold"])
        + colored(f"{script_path}", "cyan", attrs=["bold"])
    )
    print(
        colored(
            "   3.1) Modify the cluster to local for testing, as well as the destination bucket and input path if necessary",
            "yellow",
        )
    )
    print(
        colored(
            "   3.2) Run the script locally on a sample, then with a remote cluster on more files. Do the next step and repeat",
            "yellow",
        )
    )
    print(
        colored(
            "   3.3) Check the log messages to add missing variables to the dataset configuration",
            "yellow",
        )
    )
    print(
        colored(
            "   3.4) Check the batch size, worker, and scheduler on Coiled to optimize the processing",
            "yellow",
        )
    )
    print("")

    # Step 4
    print(
        colored(
            "4) Create/Modify the notebook for this dataset ", "yellow", attrs=["bold"]
        )
        + colored(f"{notebook_path}", "cyan", attrs=["bold"])
    )
    print("")

    # Step 5
    print(
        colored(
            "5) Create a PR with the JSON config, Python script, pyproject.toml, and Jupyter notebook",
            "yellow",
            attrs=["bold"],
        )
    )
    print("")

    # Step 6
    print(colored("6) Create the AWS Registry configuration", "yellow", attrs=["bold"]))
    print(
        colored(
            "   https://aodn-cloud-optimised.readthedocs.io/en/latest/module-overview.html#create-aws-registry-dataset-entry",
            "cyan",
            attrs=["underline"],
        )
    )
    print(
        colored(
            "   6.1) Submit a PR to https://github.com/awslabs/open-data-registry with the new registry file created",
            "yellow",
        )
    )

    print(
        colored(
            "   6.1) Submit a PR to https://github.com/awslabs/open-data-registry with the new registry file created",
            "yellow",
        )
    )

    print(
        colored(
            "\n\nhttps://aodn-cloud-optimised.readthedocs.io/en/latest/ For more info",
            "white",
            "on_red",
            attrs=["bold", "underline"],
        )
    )


def create_notebook(dataset_name):
    module_path = get_module_path()
    notebook_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("../")
        .joinpath("notebooks")
        .joinpath(f"{dataset_name}.ipynb")
    )

    notebook_parquet_template_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("../")
        .joinpath("notebooks")
        .joinpath(f"template_parquet.ipynb")
    )

    notebook_parquet_zarr_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("../")
        .joinpath("notebooks")
        .joinpath(f"template_zarr.ipynb")
    )
    dataset_config = load_dataset_config(
        str(
            files("aodn_cloud_optimised.config.dataset").joinpath(
                f"{dataset_name}.json"
            )
        )
    )
    cloud_optimised_format = dataset_config.get("cloud_optimised_format")
    metadata_uuid = dataset_config.get("metadata_uuid", "")
    metadata_url = f"[in the AODN metadata catalogue](https://catalogue-imos.aodn.org.au/geonetwork/srv/eng/catalog.search#/metadata/{metadata_uuid})"
    notebook_url = f"[GitHub](https://github.com/aodn/aodn_cloud_optimised/tree/main/notebooks/{dataset_name}.ipynb)"

    if not os.path.exists(notebook_path):
        if cloud_optimised_format == "parquet":
            cloud_format_url = "[Parquet](https://parquet.apache.org)"
            with open(notebook_parquet_template_path) as f:
                template_nb = nbformat.read(f, as_version=4)

        elif cloud_optimised_format == "zarr":
            cloud_format_url = "[Zarr](https://zarr.dev/)"
            with open(notebook_parquet_zarr_path) as f:
                template_nb = nbformat.read(f, as_version=4)
        else:
            raise ValueError(
                f"ERROR: {cloud_optimised_format} is not a supported format (zarr/parquet)"
            )

        # Create a copy of the template notebook
        new_nb = nbformat.v4.new_notebook()
        new_nb.cells = template_nb.cells.copy()

        # Find the first code cell and modify its content
        for cell in new_nb.cells:
            if cell.cell_type == "code":
                cell.source = f'dataset_name = "{dataset_name}"'
                break
        for cell in new_nb.cells:
            if cell.cell_type == "markdown":
                cell.source = (
                    f"## Access {dataset_name.replace('_', ' ').title()} ({cloud_optimised_format.title()})\n"
                    f"This Jupyter notebook demonstrates how to access and plot {dataset_name} data, available as a {cloud_format_url} dataset stored on S3.\n\n"
                    f"🔗 More information about the dataset is available {metadata_url}.\n\n"
                    f"📌 The source of truth for this notebook is maintained on {notebook_url}.\n"
                )
                break

        # Save the new notebook
        with open(notebook_path, "w") as f:
            nbformat.write(new_nb, f)


def validate_s3fs_opts(value: str) -> dict:
    """Validate and parse the s3fs options JSON string."""
    try:
        opts = json.loads(value)
        if not isinstance(opts, dict):
            raise ValueError("s3fs options must be a JSON object")
        # validate by trying to instantiate
        s3fs.S3FileSystem(**opts)
        return opts
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Invalid s3fs options: {e}")


def main():
    """
    Script to generate a dataset configuration from a NetCDF or CSV file stored in S3.

    This script performs the following tasks:

    1. a. Generates a JSON schema from the NetCDF/CSV file located in the S3 bucket.
       b. Generates a NetCDF file template with fill with NaNs alongside the json schema. Used to restore missing dimensions
    2. Reads and merges the validation schema template with the generated schema.
    3. Populates the dataset configuration with additional metadata including dataset name, metadata UUID, logger name,
       cloud-optimised format, cluster options, and batch size.
    4. Writes the dataset configuration to the module path as a JSON file. READY TO BE ADDED TO GITHUB
    5. A script is also created under the bin folder of the module (to be updated manually) and the entry is added to the pyproject.toml file
    6. Create a Jupyter Notebook from template (TO BE MODIFIED)
    7. Updates the notebooks/README.md with a new entry
    8. Optionally, fills up the AWS registry with Geonetwork metadata if a UUID is provided.

    Usage:
        cloud_optimised_create_dataset_config -f <NetCDF/CSV/PARQUET file object key> -c <cloud optimised format> -d <dataset name> [-b <S3 bucket name>] [-u <Geonetwork Metadata UUID>]

    Arguments:
        -f, --file: Object key for the file (required).
        -b, --bucket: S3 bucket name (optional, defaults to the value from config).
        -c, --cloud-format: Cloud optimised format, either "zarr" or "parquet" (required).
        -u, --uuid: Geonetwork Metadata UUID (optional).
        -d, --dataset-name: Name of the dataset (required, no spaces or underscores).
        --csv-opts: pandas read_csv optional arguments as json format
        --s3fs-opts: Optional JSON string of arguments for s3fs.S3FileSystem,
                            e.g. '{"key": "minioadmin", "secret": "minioadmin", "client_kwargs": {"endpoint_url": "http://localhost:9000"}}'

    Example:
        cloud_optimised_create_dataset_config \
            -f IMOS/SOOP/SOOP-TRV/VMQ9273_Solander/By_Cruise/Cruise_START-20100225T073727Z_END-20100225T131607Z/chlorophyll/IMOS_SOOP-TRV_B_20100225T073727Z_VMQ9273_FV01_END-20100225T131607Z.nc \
            -d vessel_trv_realtime_qc \
            -u 8af21108-c535-43bf-8dab-c1f45a26088c \
            -c parquet
            --s3fs-opts '{"key": "minioadmin", "secret": "minioadmin", "client_kwargs": {"endpoint_url": "http://localhost:9000"}}'
    """
    # Load the default BUCKET_RAW_DEFAULT
    default_bucket = load_variable_from_config("BUCKET_RAW_DEFAULT")

    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Generate JSON schema from S3 NetCDF file."
    )
    parser.add_argument(
        "-f",
        "--file",
        required=True,
        help="S3 object key or full s3:// URI for the NetCDF/CSV file.",
    )
    parser.add_argument(
        "-b",
        "--bucket",
        required=False,
        default=default_bucket,
        help="S3 bucket name. Default is the value from config.",
    )
    parser.add_argument(
        "-c",
        "--cloud-format",
        required=True,
        choices=["zarr", "parquet"],
        help="Cloud optimised format",
    )
    parser.add_argument(
        "-u",
        "--uuid",
        required=False,
        type=validate_uuid,
        help="Geonetwork Metadata UUID",
    )
    parser.add_argument(
        "-d",
        "--dataset-name",
        required=True,
        type=validate_dataset_name,
        help="Name of the dataset (no spaces or underscores)",
    )
    parser.add_argument(
        "--csv-opts",
        "--pandas-read-csv-config",
        dest="csv_opts",
        required=False,
        help="JSON string of options to pass to pandas.read_csv when input is CSV.",
    )

    parser.add_argument(
        "--s3fs-opts",
        dest="s3fs_opts",
        required=False,
        type=validate_s3fs_opts,
        help="JSON string of options to pass to s3fs.S3FileSystem for custom S3 endpoints/authentication.",
    )

    # Parse arguments
    args = parser.parse_args()

    # Construct the S3 file path

    # Handle S3 path
    if args.file.startswith("s3://"):
        fp = args.file
        s3_path = PureS3Path.from_uri(fp)
        bucket = s3_path.bucket
        obj_key = str(s3_path.key)
    else:
        obj_key = args.file
        bucket = args.bucket
        fp = PureS3Path.from_uri(f"s3://{args.bucket}").joinpath(args.file).as_uri()

    # Create an empty NetCDF with NaN variables alongside the JSON files. Acts as the source of truth for restoring missing dimensions.
    # only useful for Zarr to concatenate NetCDF together with missing var/dim in some NetCDF files
    if args.cloud_format == "zarr":
        nc_nullify_path = nullify_netcdf_variables(fp, args.dataset_name)

    # optionals s3fs options
    if args.s3fs_opts:
        fs = s3fs.S3FileSystem(**args.s3fs_opts)
    else:
        fs = s3fs.S3FileSystem(
            anon=False,
        )

    # Route by file type
    obj_key_suffix = pathlib.Path(obj_key.lower()).suffix
    match obj_key_suffix:
        case ".nc":

            # Generate JSON schema from the NetCDF file
            temp_file_path = generate_json_schema_from_s3_netcdf(
                fp, cloud_format=args.cloud_format, s3_fs=fs
            )
            with open(temp_file_path, "r") as file:
                dataset_config_schema = json.load(file)
            os.remove(temp_file_path)

        case ".csv":

            csv_opts = json.loads(args.csv_opts) if args.csv_opts else {}
            with fs.open(fp, "rb") as f:
                df = pd.read_csv(f, **csv_opts)

            dataset_config_schema = {"type": "object", "properties": {}}
            for col, dtype in df.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    js_type = "integer"
                elif pd.api.types.is_float_dtype(dtype):
                    js_type = "number"
                elif pd.api.types.is_bool_dtype(dtype):
                    js_type = "boolean"
                elif pd.api.types.is_object_dtype(dtype) | pd.api.types.is_string_dtype(
                    dtype
                ):
                    js_type = "string"
                else:
                    raise NotImplementedError(
                        f"found dtype that did not fit into configured categories: `{dtype}`"
                    )

                dataset_config_schema["properties"][col] = {"type": js_type}

        case ".parquet":

            with fs.open(fp, "rb") as f:
                schema = pq.read_schema(f)
                dataset_config_schema = dict()

                for field in schema:
                    dataset_config_schema[field.name] = {"type": str(field.type)}

        # Default: Raise NotImplemented
        case _:
            raise NotImplementedError(
                f"input file type `{obj_key_suffix}` not implemented"
            )

    dataset_config = {"schema": dataset_config_schema}
    # Define the path to the validation schema file
    json_validation_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("config")
        .joinpath(f"schema_validation_{args.cloud_format}.json")
    )

    # Read the validation schema from the file
    with open(json_validation_path, "r") as f:
        validation_schema = json.load(f)

    # Generate the template based on the validation schema
    template = generate_template(validation_schema)

    dataset_config = merge_dicts(template, dataset_config)

    # default values
    dataset_config["dataset_name"] = args.dataset_name
    dataset_config["metadata_uuid"] = args.uuid
    dataset_config["logger_name"] = args.dataset_name
    dataset_config["cloud_optimised_format"] = args.cloud_format

    # run_settings
    dataset_config["run_settings"]["coiled_cluster_options"] = {
        "n_workers": [1, 20],
        "scheduler_vm_types": "m7i-flex.large",
        "worker_vm_types": "m7i-flex.large",
        "allow_ingress_from": "me",
        "compute_purchase_option": "spot_with_fallback",
        "worker_options": {"nthreads": 4, "memory_limit": "8GB"},
    }
    dataset_config["run_settings"]["batch_size"] = 5
    dataset_config["run_settings"]["clear_existing_data"] = True
    dataset_config["run_settings"]["raise_error"] = False
    dataset_config["run_settings"]["cluster"] = {
        "mode": f"{TO_REPLACE_PLACEHOLDER}",
        "restart_every_path": False,
    }
    parent_s3_path = PureS3Path.from_uri(fp).parent.as_uri()
    dataset_config["run_settings"]["paths"] = [
        {"s3_uri": parent_s3_path, "filter": [".*\\.nc"], "year_range": []}
    ]

    if args.s3fs_opts:
        dataset_config.setdefault("run_settings", {})["s3_bucket_opts"] = {
            "input_data": {
                "bucket": bucket,  # ← comes from parsed path
                "s3_fs_opts": args.s3fs_opts,  # ← user-provided json
            },
            "output_data": {
                "bucket": f"{TO_REPLACE_PLACEHOLDER}",  # ← fixed or could also be config-driven
                "s3_fs_opts": args.s3fs_opts,
            },
        }

    if "spatial_extent" in dataset_config:
        dataset_config["spatial_extent"]["spatial_resolution"] = 5

    if obj_key.lower().endswith(".csv") and args.cloud_format == "parquet":
        pandas_read_csv_config_default = {
            "delimiter": ";",
            "header": 0,
            "index_col": f"{TO_REPLACE_PLACEHOLDER}",
            "parse_dates": [f"{TO_REPLACE_PLACEHOLDER}"],
            "na_values": ["N/A", "NaN"],
            "encoding": "utf-8",
        }
        dataset_config["pandas_read_cvs_config"] = pandas_read_csv_config_default

    if args.cloud_format == "parquet":
        schema_transformation_parquet_str = """
        {
        "drop_variables": [],
        "add_variables": {
            "filename": {
            "source": "@filename",
            "schema": {
                "type": "string",
                "units": "1",
                "long_name": "Filename of the source file"
            }
            },
            "timestamp": {
            "source": "@partitioning:time_extent",
            "schema": {
                "type": "int64",
                "units": "1",
                "long_name": "Partition timestamp"
            }
            },
            "polygon": {
            "source": "@partitioning:spatial_extent",
            "schema": {
                "type": "string",
                "units": "1",
                "long_name": "Spatial partition polygon"
            }
            }
        },
        "partitioning": [
            {
            "source_variable": "timestamp",
            "type": "time_extent",
            "time_extent": {
                "time_varname": "FILL UP MANUALLY - CHECK DOCUMENTATION",
                "partition_period": "M"
            }
            },
            {
            "source_variable": "polygon",
            "type": "spatial_extent",
            "spatial_extent": {
                "lat_varname": "FILL UP MANUALLY - CHECK DOCUMENTATION",
                "lon_varname": "FILL UP MANUALLY - CHECK DOCUMENTATION",
                "spatial_resolution": 5
            }
            }
        ],
        "global_attributes": {
            "delete": [
            "geospatial_lat_max",
            "geospatial_lat_min",
            "geospatial_lon_max",
            "geospatial_lon_min",
            "date_created"
            ],
            "set": {
            "title": "FILL UP MANUALLY - CHECK DOCUMENTATION"
            }
        }
        }
        """
        schema_transformation_parquet = json.loads(schema_transformation_parquet_str)
        # default partition keys
        dataset_config["schema_transformation"] = schema_transformation_parquet

        dataset_config["run_settings"]["force_previous_parquet_deletion"] = False

    module_path = get_module_path()

    # write json config to module path
    with open(f"{module_path}/config/dataset/{args.dataset_name}.json", "w") as f:
        json.dump(dataset_config, f, indent=2)

    create_dataset_script(args.dataset_name, f"{args.dataset_name}.json", fp, bucket)
    update_pyproject_toml(args.dataset_name)

    # fill up aws registry with GN3 uuid
    if args.uuid:
        populate_dataset_config_with_geonetwork_metadata(f"{args.dataset_name}.json")

    update_readme(args.dataset_name)
    create_notebook(args.dataset_name)
    next_steps(args.dataset_name)


if __name__ == "__main__":
    main()
