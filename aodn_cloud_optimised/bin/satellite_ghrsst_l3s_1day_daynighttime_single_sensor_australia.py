#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2019",
        "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2020",
        "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2021",
        "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2022",
        "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2023",
        "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2024",
        "--dataset-config",
        "satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
