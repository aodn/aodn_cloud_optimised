#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2023",
        "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2022",
        "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2021",
        "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2020",
        "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2019",
        "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2018",
        "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2024",
        # "--filters",
        # "FILTER_STRING_1",
        # "FILTER_STRING_1",
        "--dataset-config",
        "satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_southernocean.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
