#!/usr/bin/env python3
import subprocess


def main():
    # Define the command with the predefined arguments
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/ANMN/NSW",
        "IMOS/ANMN/PA",
        "IMOS/ANMN/QLD",
        "IMOS/ANMN/SA",
        "IMOS/ANMN/WA",
        "--filters",
        "'/Temperature/', 'FV01'",
        "--dataset-config",
        "anmn_temperature_logger_ts_fv01.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
