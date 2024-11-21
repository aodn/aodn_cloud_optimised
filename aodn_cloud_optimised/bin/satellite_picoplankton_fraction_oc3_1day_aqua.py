#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/OC/gridded/aqua/P1D/",
        "--filters",
        "aust.picop_brewin2012in.nc",
        "--dataset-config",
        "satellite_picoplankton_fraction_oc3_1day_aqua.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
