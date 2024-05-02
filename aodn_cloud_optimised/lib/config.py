import json
import yaml
import os
import importlib.resources
from collections import OrderedDict
from importlib.resources import path


def load_variable_from_file(file_path, variable_name) -> str:
    try:
        with open(file_path, 'r') as file:
            if file_path.endswith('.yaml'):
                variables = yaml.safe_load(file)
            elif file_path.endswith('.json'):
                variables = json.load(file, object_pairs_hook=OrderedDict)
            else:
                raise ValueError("Unsupported file format. Please provide either a YAML or JSON file.")

        if variable_name in variables:
            return variables[variable_name]
        else:
            raise KeyError(f"Variable '{variable_name}' not found in the file '{file_path}'.")
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{file_path}' not found.")


def load_variable_from_config(variable_name) -> str:
    # Obtain the file path using the context manager
    with path("aodn_cloud_optimised.config", "common.json") as common_config_path:
        return load_variable_from_file(str(common_config_path), variable_name)


def load_dataset_config(config_path) -> dict:
    try:
        with open(config_path, 'r') as file:
            if config_path.endswith('.json'):
                dataset_config = json.load(file)
            else:
                raise ValueError("Unsupported file format. Please provide either a YAML or JSON file.")

            return dataset_config
    except Exception as e:
        raise TypeError(f"{e}")

