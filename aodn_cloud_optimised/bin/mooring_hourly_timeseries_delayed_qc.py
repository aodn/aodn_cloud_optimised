#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/ANMN/NSW",
        "IMOS/ANMN/PA",
        "IMOS/ANMN/QLD",
        "IMOS/ANMN/SA",
        "IMOS/ANMN/WA",
        "--filters",
        "_hourly-timeseries_",
        "FV02",
        "--dataset-config",
        "mooring_hourly_timeseries_delayed_qc.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)
