#!/usr/bin/env python3
import subprocess


def main():
    # Define the command with the predefined arguments
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024",
        "--dataset-config",
        "acorn_gridded_qc_turq.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
