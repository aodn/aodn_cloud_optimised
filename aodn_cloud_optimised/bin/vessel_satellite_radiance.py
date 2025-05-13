#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        # "IMOS/SRS/OC/radiometer/VMQ9273_Solander",
        # "IMOS/SRS/OC/radiometer/VLHJ_Southern-Surveyor/2012",
        "IMOS/SRS/OC/radiometer/",
        "--filters",
        "FV01",
        # "20200917T234928Z",
        "--dataset-config",
        "vessel_satellite_radiance.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
