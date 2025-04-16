#!/usr/bin/env python3
import subprocess


def main():

    for i, year in enumerate(range(2023, 2026)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/OC/gridded/noaa20/P1D/{year}",
            "--filters",
            "aust.chl_oc3.nc",
            # "FILTER_STRING_1",
            "--dataset-config",
            "satellite_chlorophylla_oc3_1day_noaa20.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)
