#!/usr/bin/env python3
import subprocess


def main():
    # Define the command with the predefined arguments
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/ANFOG/slocum_glider",
        "--dataset-config",
        "anfog_slocum_glider.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
