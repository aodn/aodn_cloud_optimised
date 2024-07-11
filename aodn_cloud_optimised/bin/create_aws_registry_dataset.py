#!/usr/bin/env python3
import argparse
import os
import tempfile
from argparse import RawTextHelpFormatter
from importlib.resources import files

from aodn_cloud_optimised.lib.CommonHandler import CommonHandler
from aodn_cloud_optimised.lib.config import load_dataset_config


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


def convert_to_opendata_registry(json_file, output_directory):
    """
    Convert a JSON file to AWS OpenData Registry format and save in the specified output directory.

    Args:
        json_file (str): Name of the JSON file to convert.
        output_directory (str): Directory to save the converted YAML file.

    Returns:
        None
    """
    dataset_config = load_dataset_config(
        str(files("aodn_cloud_optimised.config.dataset").joinpath(json_file))
    )
    handler = CommonHandler(dataset_config=dataset_config)
    handler.create_metadata_aws_registry(target_directory=output_directory)


def main():
    """
    Main function to convert JSON files to AWS OpenData Registry format.

    The script can be run in three ways:
    1. Convert a specific JSON file to AWS OpenData Registry format.
    2. Convert all JSON files in the directory.
    3. Run interactively to list all available JSON files and prompt the user to choose one to convert.

    Args (optional):
        -f, --file (str): Name of a specific JSON file to convert.
        -d, --directory (str): Output directory to save converted YAML files.
        -a, --all: Convert all JSON files in the directory.

    If the directory is not specified, a temporary directory is created.
    """
    parser = argparse.ArgumentParser(
        description="""
        Create AWS OpenData Registry YAML files from the dataset configuration, ready to be added to the OpenData Github
        repository.
        The script can be run in three ways:
            1. Convert a specific JSON file to YAML using '-f' or '--file'.
            2. Convert all JSON files in the directory using '-a' or '--all'.
            3. Run interactively to list all available JSON files and prompt
               the user to choose one to convert.
               """,
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument("-f", "--file", help="Name of a specific JSON file to convert.")
    parser.add_argument(
        "-d", "--directory", help="Output directory to save converted YAML files."
    )
    parser.add_argument(
        "-a",
        "--all",
        default=False,
        action="store_true",
        help="Convert all JSON files in the directory.",
    )

    args = parser.parse_args()

    json_directory = str(files("aodn_cloud_optimised.config.dataset")._paths[0])

    if args.all:
        json_files = list_json_files(json_directory)
        if json_files:
            output_dir = args.directory or tempfile.mkdtemp()
            for file in json_files:
                convert_to_opendata_registry(file, output_dir)
        else:
            print(f"No JSON files found in {json_directory}.")
    elif args.file:
        output_dir = args.directory or tempfile.mkdtemp()
        convert_to_opendata_registry(args.file, output_dir)
    else:
        json_files = list_json_files(json_directory)
        if json_files:
            print("Available dataset configuration files:")
            for idx, file in enumerate(json_files, start=1):
                print(f"{idx}. {file}")
            choice = input("Enter the number of the dataset to convert: ")
            try:
                choice_idx = int(choice) - 1
                if 0 <= choice_idx < len(json_files):
                    output_dir = args.directory or tempfile.mkdtemp()
                    convert_to_opendata_registry(json_files[choice_idx], output_dir)
                else:
                    print("Invalid choice. Aborting.")
            except ValueError:
                print("Invalid input. Aborting.")


if __name__ == "__main__":
    main()
