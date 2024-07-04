#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024/01/",
        "--dataset-config",
        "radar_TurquoiseCoast_velocity_hourly_average_delayed_qc.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
