#!/usr/bin/env python3
import subprocess


def main():

    for i, year in enumerate(range(1992, 2025)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/SST/ghrsst/L3S-1dS/dn/{year}",
            # "IMOS/SRS/SST/ghrsst/L3S-1dS/dn/",
            # "--filters",
            # "200505",
            # "FILTER_STRING_1",
            # "FILTER_STRING_1",
            "--dataset-config",
            "satellite_ghrsst_l3s_1day_daynighttime_single_sensor_southernocean.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
