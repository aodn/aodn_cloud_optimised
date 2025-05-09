#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/OceanCurrent/GSLA/NRT/",
        "--dataset-config",
        "model_sea_level_anomaly_gridded_realtime.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)
