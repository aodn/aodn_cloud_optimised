import os
from importlib.resources import files


def list_json_files(dataset_dir=None):
    """
    List all JSON files in the specified directory or in the
    'aodn_cloud_optimised.config.dataset' package directory by default.

    Excludes files ending with '_main.json' and 'dataset_template.json'.

    Args:
        dataset_dir (str or Path, optional): Directory path to search for JSON files.
            If None, uses the 'aodn_cloud_optimised.config.dataset' package directory.

    Returns:
        list: Sorted list of JSON file names.
    """
    if dataset_dir is None:
        dataset_dir = files("aodn_cloud_optimised.config.dataset")
        file_iter = dataset_dir.iterdir()
    else:
        file_iter = os.scandir(dataset_dir)

    json_files = [
        entry.name
        for entry in file_iter
        if entry.name.endswith(".json")
        and not (
            entry.name.endswith("_main.json") or entry.name == "dataset_template.json"
        )
    ]
    json_files.sort()
    return json_files


def list_dataset_config(dataset_dir=None):
    """
    List dataset configuration JSON files.

    This is a wrapper around `list_json_files`, providing a more domain-specific name.

    Args:
        dataset_dir (str or Path, optional): Directory path to search for JSON files.
            If None, uses the 'aodn_cloud_optimised.config.dataset' package directory.

    Returns:
        list: Sorted list of JSON file names.
    """
    return list_json_files(dataset_dir)
