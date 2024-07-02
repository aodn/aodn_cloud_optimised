import json
import yaml
import os
from collections import OrderedDict
from importlib.resources import path


def merge_dicts(parent, child):
    """Merge two dictionaries, giving priority to the child dictionary."""
    for key, value in child.items():
        if isinstance(value, dict) and key in parent:
            parent[key] = merge_dicts(parent[key], value)
        else:
            parent[key] = value
    return parent


def load_config(file_path):
    """Load a configuration file in either YAML or JSON format."""
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
    # Obtain the file path using the context manager
    with path("aodn_cloud_optimised.config", "common.json") as common_config_path:
        return load_variable_from_file(str(common_config_path), variable_name)


def load_dataset_config(config_path) -> dict:
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
