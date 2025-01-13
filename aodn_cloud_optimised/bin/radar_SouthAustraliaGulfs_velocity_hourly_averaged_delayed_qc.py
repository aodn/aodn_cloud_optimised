#!/usr/bin/env python3
import subprocess

#  Contact the data provider. The following files don't have a consistent grid with the rest of the dataset:
# [<File-like object S3FileSystem, imos-data/IMOS/ACORN/gridded_1h-avg-current-map_QC/SAG/2013/05/10/IMOS_ACORN_V_20130510T073000Z_SAG_FV01_1-hour-avg.nc>,
# <File-like object S3FileSystem, imos-data/IMOS/ACORN/gridded_1h-avg-current-map_QC/SAG/2013/08/09/IMOS_ACORN_V_20130809T233000Z_SAG_FV01_1-hour-avg.nc>,
# <File-like object S3FileSystem, imos-data/IMOS/ACORN/gridded_1h-avg-current-map_QC/SAG/2013/08/18/IMOS_ACORN_V_20130818T043000Z_SAG_FV01_1-hour-avg.nc>]
# .... march 2014


def main():
    # for i, year in enumerate(range(2007, 2025)):
    for i, year in enumerate(range(2017, 2025)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/ACORN/gridded_1h-avg-current-map_QC/SAG/{year}",
            # "--filters",
            # ".nc",
            "--dataset-config",
            "radar_SouthAustraliaGulfs_velocity_hourly_averaged_delayed_qc.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        # if i == 0:
        #     command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
