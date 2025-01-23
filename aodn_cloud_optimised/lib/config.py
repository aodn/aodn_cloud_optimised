import json
import os
from collections import OrderedDict
from importlib.resources import files

import yaml

from aodn_cloud_optimised.lib.common import list_json_files


def merge_dicts(parent, child):
    """
    Merge two dictionaries, giving priority to the child dictionary.

    :param parent: The parent dictionary.
    :param child: The child dictionary whose values will override those of the parent.
    :return: The merged dictionary with child's values taking precedence.
    """
    for key, value in child.items():
        if key == "schema" and value:  # dont merge child and parent schema
            parent[key] = value
        elif isinstance(value, dict) and key in parent:
            parent[key] = merge_dicts(parent[key], value)
        else:
            parent[key] = value
    return parent


def load_config(file_path):
    """
    Load a configuration file in either YAML or JSON format.

    :param file_path: Path to the configuration file.
    :return: The loaded configuration as a dictionary.
    :raises ValueError: If the file format is unsupported.
    :raises FileNotFoundError: If the file is not found.
    """
    try:
        with open(file_path, "r") as file:
            if file_path.endswith(".yaml"):
                return yaml.safe_load(file)
            elif file_path.endswith(".json"):
                return json.load(
                    file
                )  # remove this as it's breaking the metadata for parquet, object_pairs_hook=OrderedDict)
            else:
                raise ValueError(
                    "Unsupported file format. Please provide either a YAML or JSON file."
                )
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{file_path}' not found.")


def load_variable_from_file(file_path, variable_name) -> str:
    """
    Load a specific variable from a configuration file, considering parent configurations if specified.

    :param file_path: Path to the configuration file.
    :param variable_name: Name of the variable to retrieve.
    :return: The value of the specified variable.
    :raises KeyError: If the variable is not found in the configuration file.
    """
    # Load the child configuration
    variables = load_config(file_path)

    # Check for a parent configuration file
    parent_config_path = variables.get("parent_config")
    if parent_config_path:
        # Construct the full path to the parent configuration file
        parent_config_path = os.path.join(
            os.path.dirname(file_path), parent_config_path
        )
        # Load the parent configuration
        parent_variables = load_config(parent_config_path)
        # Merge the parent and child configurations
        variables = merge_dicts(parent_variables, variables)

    # Retrieve the variable
    if variable_name in variables:
        return variables[variable_name]
    else:
        raise KeyError(
            f"Variable '{variable_name}' not found in the file '{file_path}'."
        )


def load_variable_from_config(variable_name) -> str:
    """
    Load a specific variable from the common library configuration file.

    :param variable_name: Name of the variable to retrieve.
    :return: The value of the specified variable.
    :raises KeyError: If the variable is not found in the configuration file.
    """
    # Obtain the file path using the context manager
    with (
        files("aodn_cloud_optimised")
        .joinpath("config")
        .joinpath("common.json") as common_config_path
    ):
        return load_variable_from_file(str(common_config_path), variable_name)


def load_dataset_config(config_path) -> dict:
    """
    Load a dataset configuration, considering parent configurations if specified.

    :param config_path: Path to the dataset configuration file.
    :return: The loaded dataset configuration as a dictionary.
    :raises FileNotFoundError: If the parent configuration file is not found.
    :raises ValueError: If there is an error loading the parent configuration file.
    """
    # Load the child configuration
    dataset_config = load_config(config_path)

    # Check for a parent configuration file
    parent_config_path = dataset_config.get("parent_config")
    if parent_config_path:
        # Construct the full path to the parent configuration file which is in the same directory
        parent_config_path = os.path.join(
            os.path.dirname(config_path), parent_config_path
        )
        # Load the parent configuration
        try:
            parent_dataset_config = load_config(parent_config_path)
            # Merge the parent and child configurations
            dataset_config = merge_dicts(parent_dataset_config, dataset_config)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Parent configuration file '{parent_config_path}' not found."
            )
        except ValueError as e:
            raise ValueError(
                f"Error loading parent configuration file '{parent_config_path}': {e}"
            )

    return dataset_config


def get_notebook_url(uuid: str):
    """
    Retrieve the jupyter notebook url based on a metadata uuid value

    :param uuid: string of the metadata uuid to search for
    :return: the equivalent url of the jupyter notebook
    :raise ValueError: if the uuid is not part of the AODN Cloud Otimised configuration
    """
    json_directory = str(files("aodn_cloud_optimised.config.dataset")._paths[0])

    json_files = list_json_files(json_directory)
    if json_files:
        for json_file in json_files:
            json_path = str(
                files("aodn_cloud_optimised.config.dataset").joinpath(json_file)
            )
            uuid_file = load_variable_from_file(json_path, "metadata_uuid")
            if uuid_file == uuid:
                dataset_config_key = load_variable_from_file(
                    json_path, "aws_opendata_registry"
                )

                return dataset_config_key["DataAtWork"]["Tutorials"][0]["URL"]

        return ValueError(
            f"metadata uuid {uuid} is unknown by the AODN Cloud Optimised library"
        )

    else:
        return ValueError("No json config files to look for")
