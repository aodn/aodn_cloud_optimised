#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SOOP/SOOP-ASF/",
        "--filters",
        "meteorological_sst_observations",
        "--dataset-config",
        "vessel_air_sea_flux_sst_meteo_realtime.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)
