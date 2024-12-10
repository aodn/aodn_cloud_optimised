#!/usr/bin/env python3
import subprocess


def main():

    for i, year in enumerate(range(2015, 2025)):
        # Prepare the command
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/OC/gridded/aqua/P1D/{year}",
            "--filters",
            "aust.K_490.nc",
            "--dataset-config",
            "satellite_diffuse_attenuation_coefficent_1day_aqua.json",
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
