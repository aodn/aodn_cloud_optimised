#!/usr/bin/env python3
import subprocess


def main():
    # for i, year in enumerate(range(1992, 2025)):
    for i, year in enumerate(range(2019, 2025)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/SST/ghrsst/L3S-1m/day/{year}",
            # "--filters",
            # "FILTER_STRING_1",
            # "FILTER_STRING_1",
            "--dataset-config",
            "satellite_ghrsst_l3s_1month_daytime_single_sensor_australia.json",
            "--cluster-mode",
            "remote",
        ]

        # Add --clear-existing-data for the first iteration only
        # if i == 0:
        # command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
