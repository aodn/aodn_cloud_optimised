#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/SST/ghrsst/L4/GAMSSA/",
        # "--filters",
        # "FILTER_STRING_1",
        # "FILTER_STRING_1",
        "--dataset-config",
        "satellite_ghrsst_l4_gamssa_1day_multi_sensor_world.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
