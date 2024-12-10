#!/usr/bin/env python3
import subprocess


def main():

    for i, year in enumerate(range(2002, 2023)):
        # Prepare the command
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/OC/gridded/aqua/P1D/{year}",
            "--filters",
            "aust.owtd_csiro.nc",
            "--dataset-config",
            "satellite_optical_water_type_1day_aqua.json",
            "--cluster-mode",
            "coiled",
        ]

        # # Add --clear-existing-data for the first iteration only
        # if i == 0:
        #     command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
