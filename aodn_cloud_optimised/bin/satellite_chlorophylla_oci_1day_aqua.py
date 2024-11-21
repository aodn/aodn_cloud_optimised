#!/usr/bin/env python3
import subprocess


## issue opening files in batch with h5netcdf. The ds does not have any variables, To investigate


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/OC/gridded/aqua/P1D/",
        "--filters",
        "aust.chl_oci.nc",
        "--dataset-config",
        "satellite_chlorophylla_oci_1day_aqua.json",
        "--clear-existing-data",
        "--cluster-mode",
        "local",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
