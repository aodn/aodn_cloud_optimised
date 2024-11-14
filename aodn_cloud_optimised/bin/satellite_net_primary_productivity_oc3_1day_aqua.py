#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/OC/gridded/aqua/P1D/",
        "--filters",
        "aust.npp_vgpm_eppley_oc3.nc",
        "--dataset-config",
        "satellite_net_primary_productivity_oc3_1day_aqua.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
