#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/SST/ghrsst/L3C-1d/ngt/h08/",
        # "--filters",
        # "FILTER_STRING_1",
        # "FILTER_STRING_1",
        "--dataset-config",
        "satellite_ghrsst_l3c_1day_nighttime_himawari8.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
