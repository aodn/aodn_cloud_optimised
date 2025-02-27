#!/usr/bin/env python3
import subprocess


def main():
    imos_paths = [f"IMOS/SRS/SST/ghrsst/L4/RAMSSA/{year}" for year in range(2006, 2026)]

    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/",
        *imos_paths,
        # "--filters",
        # "FILTER_STRING_1",
        # "FILTER_STRING_1",
        "--dataset-config",
        "satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
