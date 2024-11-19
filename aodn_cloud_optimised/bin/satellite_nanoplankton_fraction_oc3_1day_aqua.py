#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/OC/gridded/aqua/P1D/2009",
        "IMOS/SRS/OC/gridded/aqua/P1D/2010",
        "IMOS/SRS/OC/gridded/aqua/P1D/2011",
        "IMOS/SRS/OC/gridded/aqua/P1D/2012",
        "IMOS/SRS/OC/gridded/aqua/P1D/2013",
        "IMOS/SRS/OC/gridded/aqua/P1D/2014",
        "IMOS/SRS/OC/gridded/aqua/P1D/2015",
        "IMOS/SRS/OC/gridded/aqua/P1D/2016",
        "IMOS/SRS/OC/gridded/aqua/P1D/2017",
        "IMOS/SRS/OC/gridded/aqua/P1D/2018",
        "IMOS/SRS/OC/gridded/aqua/P1D/2019",
        "IMOS/SRS/OC/gridded/aqua/P1D/2020",
        "IMOS/SRS/OC/gridded/aqua/P1D/2021",
        "IMOS/SRS/OC/gridded/aqua/P1D/2022",
        "IMOS/SRS/OC/gridded/aqua/P1D/2023",
        "IMOS/SRS/OC/gridded/aqua/P1D/2024",
        "--filters",
        "aust.nanop_brewin2012in.nc",
        "--dataset-config",
        "satellite_nanoplankton_fraction_oc3_1day_aqua.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
