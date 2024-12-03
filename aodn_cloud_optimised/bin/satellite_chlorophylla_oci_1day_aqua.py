#!/usr/bin/env python3
import subprocess


def main():
    imos_paths = [f"IMOS/SRS/OC/gridded/aqua/P1D/{year}" for year in range(2002, 2025)]

    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        *imos_paths,
        "--filters",
        "aust.chl_oci.nc",
        "--dataset-config",
        "satellite_chlorophylla_oci_1day_aqua.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
