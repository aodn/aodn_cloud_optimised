#!/usr/bin/env python3
import subprocess


def main():
    for i, year in enumerate(range(2009, 2025)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/ACORN/gridded_1h-avg-wind-map_QC/SAG/{year}",
            # "--filters",
            # ".nc",
            "--dataset-config",
            "radar_SouthAustraliaGulfs_wind_delayed_qc.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        subprocess.run(command, check=True)
