import os


def list_json_files(directory):
    """
    List all JSON files in the specified directory, excluding files ending with '_main.json' and 'template.json'.

    Args:
        directory (str): Directory path to search for JSON files.

    Returns:
        list: List of JSON file names.
    """
    json_files = [
        f
        for f in os.listdir(directory)
        if f.endswith(".json")
        and not (f.endswith("_main.json") or f == "dataset_template.json")
    ]
    json_files.sort()
    return json_files
