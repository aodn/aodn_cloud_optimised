#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        # "IMOS/AUV/Tasmania202302/r20230301_025402_NG06_m13_bicheno_offshore_grids_18/hydro_netcdf/",
        # "IMOS/AUV/Tasmania202302/",
        "IMOS/AUV/",
        # "IMOS/AUV/Ningaloo201904/r20190403_051623_SS04_tantabiddi_slope/hydro_netcdf",
        "--filters",
        "/hydro_netcdf/",
        # "FILTER_STRING_1",
        "--dataset-config",
        "autonomous_underwater_vehicle.json",
        "--clear-existing-data",
        "--cluster-mode",
        "local",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
