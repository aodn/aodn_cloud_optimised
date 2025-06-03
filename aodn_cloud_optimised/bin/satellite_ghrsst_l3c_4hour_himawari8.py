#!/usr/bin/env python3
import subprocess


def main():
    for i, year in enumerate(range(2015, 2025)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/SST/ghrsst/L3C-4h/h08/{year}",
            # "--filters",
            # "FILTER_STRING_1",
            # "FILTER_STRING_1",
            "--dataset-config",
            "satellite_ghrsst_l3c_4hour_himawari8.json",
            "--clear-existing-data",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)
