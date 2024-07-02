#!/usr/bin/env python3
import subprocess


def main():
    # Define the command with the predefined arguments
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/OC/LJCO/WQM-daily/",
        "--dataset-config",
        "srs_oc_ljco_wqm_daily.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
