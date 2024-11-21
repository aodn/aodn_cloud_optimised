#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/OC/gridded/aqua/P1D/",
        "--filters",
        "aust.K_490.nc",
        "--dataset-config",
        "satellite_diffuse_attenuation_coefficent_1day_aqua.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
