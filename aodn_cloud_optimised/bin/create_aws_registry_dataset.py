#!/usr/bin/env python3
"""
Script to convert configuration JSON files to AWS OpenData Registry format.

The script can be run in different ways:

1. Convert a specific JSON file to AWS OpenData Registry format.
2. Convert all JSON files in the directory.
3. Run interactively to list all available JSON files and prompt the user to choose one to convert.

Important:
    If the -g option is provided, the script will download metadata from the GeoNetwork metadata
    record and prompt the user to choose to replace existing values or not.


Args (optional):
    -f, --file (str): Name of a specific JSON file to convert.
    -d, --directory (str): Output directory to save converted YAML files.
    -a, --all: Convert all JSON files in the directory.
    -g, --geonetwork: Retrieve metadata fields from GeoNetwork3 metadata record

If the directory is not specified, a temporary directory is created.
"""

import argparse
import difflib
import io
import json
import os
import tempfile
from argparse import RawTextHelpFormatter
from importlib.resources import files

import pandas as pd
import requests
import xmltodict
from colorama import Fore, Style, init

from aodn_cloud_optimised.lib.common import list_json_files
from aodn_cloud_optimised.lib.CommonHandler import CommonHandler
from aodn_cloud_optimised.lib.config import (
    load_config,
    load_dataset_config,
    load_variable_from_config,
)


def retrieve_geonetwork_metadata(
    uuid, geonetwork_base_url="https://catalogue-imos.aodn.org.au/geonetwork"
):
    """
    Retrieves metadata from GeoNetwork using the provided UUID.

    This function fetches the metadata in XML format from GeoNetwork,
    parses it, and extracts specific fields such as title, abstract,
    keywords, and UUID.

    Args:
        uuid (str): The UUID of the metadata record to retrieve.
        geonetwork_base_url (str, optional): The base URL of the GeoNetwork
            instance. Defaults to "https://catalogue-imos.aodn.org.au/geonetwork".

    Returns:
        dict: A dictionary containing the title, abstract, keywords, and UUID
        of the metadata record. Returns None if the request fails.

    Raises:
        requests.exceptions.RequestException: If the request to GeoNetwork fails.
        xmltodict.expat.ExpatError: If the XML parsing fails.
    """

    url = f"{geonetwork_base_url}/srv/api/records/{uuid}/formatters/xml?approved=true"

    try:
        # Make a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve XML data. Error: {e}")
        return None

    # Parse XML data from in-memory bytes buffer
    xml_data = response.content
    xml_file = io.BytesIO(xml_data)
    try:
        res = xmltodict.parse(xml_file.read(), encoding="utf-8")
    except xmltodict.expat.ExpatError as e:
        print(f"Failed to parse XML data. Error: {e}")
        return None

    uuid = res["mdb:MD_Metadata"]["mdb:metadataIdentifier"]["mcc:MD_Identifier"][
        "mcc:code"
    ]["gco:CharacterString"]
    title = res["mdb:MD_Metadata"]["mdb:identificationInfo"][
        "mri:MD_DataIdentification"
    ]["mri:citation"]["cit:CI_Citation"]["cit:title"]["gco:CharacterString"]
    abstract = res["mdb:MD_Metadata"]["mdb:identificationInfo"][
        "mri:MD_DataIdentification"
    ]["mri:abstract"]["gco:CharacterString"]

    metadata = {"title": title, "abstract": abstract, "uuid": uuid}

    return metadata


# Initialize Colorama for cross-platform color support
init(autoreset=True)


def update_nested_dict_key(dataset_config, keys, new_value):
    """
    Updates a key in a nested dictionary.

    If the key is empty, it is populated with the new value.
    If the key is not empty, the user is prompted to choose whether to overwrite it.

    Args:
        dataset_config (dict): The nested dictionary to update.
        keys (list): A list of keys representing the path to the target key.
        new_value (str): The new value to set.

    Returns:
        dict: The updated dictionary.
    """
    # Traverse the nested dictionary to get to the target key
    d = dataset_config
    for key in keys[:-1]:
        d = d.setdefault(key, {})

    # Get the current value of the target key
    target_key = keys[-1]
    current_value = d.get(target_key, "")

    # Convert current and new values to strings for comparison
    current_value_str = (
        json.dumps(current_value, indent=4)
        if isinstance(current_value, dict)
        else str(current_value)
    )
    new_value_str = (
        json.dumps(new_value, indent=4)
        if isinstance(new_value, dict)
        else str(new_value)
    )

    if current_value_str == new_value_str:
        print(
            f"{Fore.GREEN}{target_key} is already set to the new value. No update needed.{Style.RESET_ALL}"
        )
        return dataset_config

    if not current_value:
        d[target_key] = new_value
        print(
            f"{Fore.GREEN}{target_key} was empty, set to: {new_value}{Style.RESET_ALL}"
        )
    else:
        print(f"{Fore.YELLOW}Current {target_key}:{Style.RESET_ALL}")
        print(current_value)
        print(f"{Fore.CYAN}New {target_key}:{Style.RESET_ALL}")
        print(new_value)

        # Compute the diff between current and new value using difflib
        diff = difflib.unified_diff(
            current_value_str.splitlines(), new_value_str.splitlines(), lineterm=""
        )

        print(
            f"{Fore.MAGENTA}Difference between current and new {target_key}:{Style.RESET_ALL}"
        )
        for line in diff:
            if line.startswith("-"):
                print(Fore.RED + line + Style.RESET_ALL)
            elif line.startswith("+"):
                print(Fore.GREEN + line + Style.RESET_ALL)
            elif line.startswith("@"):
                print(Fore.CYAN + line + Style.RESET_ALL)
            else:
                print(line)

        choice = (
            input(f"Do you want to overwrite the current {target_key}? (Y/N): ")
            .strip()
            .lower()
        )

        if choice in ["y", "yes"]:
            d[target_key] = new_value
            print(f"{Fore.GREEN}{target_key} has been overwritten.{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}{target_key} was not overwritten.{Style.RESET_ALL}")

    return dataset_config


def populate_dataset_config_with_metadata_from_csv(json_file, csv_path):
    json_path = str(files("aodn_cloud_optimised.config.dataset").joinpath(json_file))
    dataset_config = load_dataset_config(json_path)

    csv_data = pd.read_csv(
        csv_path,
        index_col="Cloud_Optimised_Collection_Name",
        encoding="ISO-8859-1",
        na_filter=False,
    )

    dataset_name = dataset_config["dataset_name"]
    try:
        csv_dataset = csv_data.loc[dataset_name]
    except Exception as err:
        print(f"{dataset_name} NOT FOUND in CSV file")
        return

    if not csv_dataset["AWS_Title"] == "":
        dataset_config = update_nested_dict_key(
            dataset_config,
            ["aws_opendata_registry", "Name"],
            csv_dataset["AWS_Title"],
        )
    else:
        Warning(f"AWS_Title for {dataset_name} is missing from {csv_path}")

    if not csv_dataset["AWS_Tags"] == "":
        aws_tags = [keyword.strip() for keyword in csv_dataset["AWS_Tags"].split(";")]
        dataset_config = update_nested_dict_key(
            dataset_config, ["aws_opendata_registry", "Tags"], aws_tags
        )
    else:
        Warning(f"AWS_Tags for {dataset_name} is missing from {csv_path}")

    if not csv_dataset["AWS_Citation"] == "":
        dataset_config = update_nested_dict_key(
            dataset_config,
            ["aws_opendata_registry", "Citation"],
            csv_dataset["AWS_Citation"],
        )
    else:
        Warning(f"AWS_Citation for {dataset_name} is missing from {csv_path}")

    # dataset config coming from load_dataset_config is the result of parent and child configuration. When writing back
    # the configuration, we only want to write the child data back
    dataset_config_child = load_config(json_path)
    # Overwrite the original JSON file with the modified dataset_config
    with open(json_path, "w") as f:
        dataset_config_child["aws_opendata_registry"] = dataset_config[
            "aws_opendata_registry"
        ]
        json.dump(dataset_config_child, f, indent=2)

    print(f"Updated JSON file saved at: {json_path}")


def populate_dataset_config_with_geonetwork_metadata(json_file):
    """ """

    json_path = str(files("aodn_cloud_optimised.config.dataset").joinpath(json_file))
    dataset_config = load_dataset_config(json_path)

    uuid = dataset_config.get("metadata_uuid", None)
    if uuid is None:
        raise ValueError(
            "No metadata UUID found in the given JSON file. Process Aborted"
        )

    gn3_metadata = retrieve_geonetwork_metadata(uuid)

    if gn3_metadata is None:
        print("No Geonetwork metadata available")
        return

    dataset_config = update_nested_dict_key(
        dataset_config, ["aws_opendata_registry", "Name"], gn3_metadata["title"]
    )
    dataset_config = update_nested_dict_key(
        dataset_config,
        ["aws_opendata_registry", "Description"],
        gn3_metadata["abstract"],
    )
    dataset_config = update_nested_dict_key(
        dataset_config,
        ["aws_opendata_registry", "Documentation"],
        f"https://catalogue-imos.aodn.org.au/geonetwork/srv/eng/catalog.search#/metadata/{uuid}",
    )

    dataset_config = update_nested_dict_key(
        dataset_config, ["aws_opendata_registry", "Contact"], "info@aodn.org.au"
    )
    dataset_config = update_nested_dict_key(
        dataset_config, ["aws_opendata_registry", "ManagedBy"], "AODN"
    )
    dataset_config = update_nested_dict_key(
        dataset_config, ["aws_opendata_registry", "UpdateFrequency"], "As Needed"
    )
    dataset_config = update_nested_dict_key(
        dataset_config,
        ["aws_opendata_registry", "License"],
        "http://creativecommons.org/licenses/by/4.0/",
    )
    dataset_config = update_nested_dict_key(
        dataset_config,
        ["aws_opendata_registry", "Citation"],
        "IMOS [year-of-data-download], [Title], [data-access-URL], accessed [date-of-access]",
    )

    data_at_work = {
        "Tutorials": [
            {
                "Title": f"Accessing {dataset_config['aws_opendata_registry']['Name']}",
                "URL": f"https://nbviewer.org/github/aodn/aodn_cloud_optimised/blob/main/notebooks/{dataset_config['dataset_name']}.ipynb",
                "NotebookURL": f"https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/{dataset_config['dataset_name']}.ipynb",
                "AuthorName": "Laurent Besnard",
                "AuthorURL": "https://github.com/aodn/aodn_cloud_optimised",
            },
            {
                "Title": f"Accessing and search for any AODN dataset",
                "URL": f"https://nbviewer.org/github/aodn/aodn_cloud_optimised/blob/main/notebooks/GetAodnData.ipynb",
                "NotebookURL": f"https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/GetAodnData.ipynb",
                "AuthorName": "Laurent Besnard",
                "AuthorURL": "https://github.com/aodn/aodn_cloud_optimised",
            },
        ]
    }

    dataset_path_arn = os.path.join(
        load_variable_from_config("BUCKET_OPTIMISED_DEFAULT"),
        load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH"),
        dataset_config["dataset_name"] + "." + dataset_config["cloud_optimised_format"],
    )

    dataset_location = [
        {
            "Description": f"Cloud Optimised AODN dataset of {dataset_config['aws_opendata_registry']['Name']}",
            "ARN": f"arn:aws:s3:::{dataset_path_arn}",
            "Region": "ap-southeast-2",
            "Type": "S3 Bucket",
        },
    ]

    dataset_config = update_nested_dict_key(
        dataset_config, ["aws_opendata_registry", "DataAtWork"], data_at_work
    )

    dataset_config = update_nested_dict_key(
        dataset_config, ["aws_opendata_registry", "Resources"], dataset_location
    )

    # dataset config coming from load_dataset_config is the result of parent and child configuration. When writing back
    # the configuration, we only want to write the child data back
    dataset_config_child = load_config(json_path)
    # Overwrite the original JSON file with the modified dataset_config
    with open(json_path, "w") as f:
        dataset_config_child["aws_opendata_registry"] = dataset_config[
            "aws_opendata_registry"
        ]
        json.dump(dataset_config_child, f, indent=2)

    print(f"Updated JSON file saved at: {json_path}")


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

    The script can be run in different ways:

    1. Convert a specific JSON file to AWS OpenData Registry format.
    2. Convert all JSON files in the directory.
    3. Run interactively to list all available JSON files and prompt the user to choose one to convert.

    Important:
        If the -g option is provided, the script will download metadata from the GeoNetwork metadata
        record and prompt the user to choose to replace existing values or not.


    Args (optional):
        -f, --file (str): Name of a specific JSON file to convert.
        -d, --directory (str): Output directory to save converted YAML files.
        -a, --all: Convert all JSON files in the directory.
        -g, --geonetwork: Retrieve metadata fields from GeoNetwork3 metadata record

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
    parser.add_argument(
        "-g",
        "--geonetwork",
        default=False,
        action="store_true",
        help="Retrieve metadata from Geonetwork instance to populate OpenData Registry format. Interactive mode",
    )

    parser.add_argument(
        "-c",
        "--csv-path",
        help="Add specific metadata from an external CSV file",
    )

    args = parser.parse_args()

    json_directory = str(files("aodn_cloud_optimised.config.dataset")._paths[0])

    if args.all:
        json_files = list_json_files(json_directory)
        if json_files:
            output_dir = args.directory or tempfile.mkdtemp()
            for file in json_files:
                if args.geonetwork:
                    populate_dataset_config_with_geonetwork_metadata(file)

                if args.csv_path:
                    if os.path.exists(args.csv_path):
                        populate_dataset_config_with_metadata_from_csv(
                            file, args.csv_path
                        )
                    else:
                        raise ValueError(f"{args.csv_path} does not exist")
                convert_to_opendata_registry(file, output_dir)
        else:
            print(f"No JSON files found in {json_directory}.")
    elif args.file:
        output_dir = args.directory or tempfile.mkdtemp()
        if args.geonetwork:
            populate_dataset_config_with_geonetwork_metadata(args.file)
        if args.csv_path:
            if os.path.exists(args.csv_path):
                populate_dataset_config_with_metadata_from_csv(args.file, args.csv_path)
            else:
                raise ValueError(f"{args.csv_path} does not exist")

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
                    if args.geonetwork:
                        populate_dataset_config_with_geonetwork_metadata(
                            json_files[choice_idx]
                        )

                    if args.csv_path:
                        if os.path.exists(args.csv_path):
                            populate_dataset_config_with_metadata_from_csv(
                                json_files[choice_idx], args.csv_path
                            )
                        else:
                            raise ValueError(f"{args.csv_path} does not exist")

                    convert_to_opendata_registry(json_files[choice_idx], output_dir)
                else:
                    print("Invalid choice. Aborting.")
            except ValueError:
                print("Invalid input. Aborting.")


if __name__ == "__main__":
    main()
