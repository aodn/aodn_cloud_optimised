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
        "/CTD_timeseries/",
        "FV01",
        "--dataset-config",
        "anmn_ctd_ts_fv01.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
