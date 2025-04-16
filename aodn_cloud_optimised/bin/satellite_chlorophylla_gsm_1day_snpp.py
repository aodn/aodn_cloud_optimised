#!/usr/bin/env python3
import subprocess


def main():

    for i, year in enumerate(range(2022, 2026)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/OC/gridded/snpp/P1D/{year}",
            "--filters",
            "aust.chl_gsm.nc",
            # "FILTER_STRING_1",
            "--dataset-config",
            "satellite_chlorophylla_gsm_1day_snpp.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)
