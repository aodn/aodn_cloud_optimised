#!/usr/bin/env python3
import subprocess


def main():

    # No carder data post 2022
    for i, year in enumerate(range(2021, 2023)):
        # Prepare the command
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/OC/gridded/aqua/P1D/{year}",
            "--filters",
            "aust.chl_carder.nc",
            "--dataset-config",
            "satellite_chlorophylla_carder_1day_aqua.json",
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
