#!/usr/bin/env python3
import subprocess


def main():
    for i, year in enumerate(range(1992, 2026)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/SST/ghrsst/L3S-6d/dn/{year}",
            # "--filters",
            # "FILTER_STRING_1",
            # "FILTER_STRING_1",
            "--dataset-config",
            "satellite_ghrsst_l3s_6day_daynighttime_single_sensor_australia.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)
