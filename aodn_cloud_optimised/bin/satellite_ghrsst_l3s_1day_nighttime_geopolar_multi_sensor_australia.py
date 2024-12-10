#!/usr/bin/env python3
import subprocess


def main():
    # starts in 2015
    for i, year in enumerate(range(2018, 2023)):
        # Prepare the command
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/SST/ghrsst/L3SGM-1d/ngt/{year}",
            # "--filters",
            # ".nc",
            "--dataset-config",
            "satellite_ghrsst_l3s_1day_nighttime_geopolar_multi_sensor_australia.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        # if i == 0:
        #     command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
