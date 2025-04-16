#!/usr/bin/env python3
import subprocess

# Files with inconsistent LATITUDE values:
# <File-like object S3FileSystem, imos-data/IMOS/ACORN/gridded_1h-avg-current-map_QC/NWA/2022/03/12/IMOS_ACORN_V_20220312T183000Z_NWA_FV01_1-hour-avg.nc>
# <File-like object S3FileSystem, imos-data/IMOS/ACORN/gridded_1h-avg-current-map_QC/NWA/2022/03/31/IMOS_ACORN_V_20220331T073000Z_NWA_FV01_1-hour-avg.nc>
# Contact Data provider


def main():
    for i, year in enumerate(range(2021, 2025)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/ACORN/gridded_1h-avg-current-map_QC/NWA/{year}",
            # "--filters",
            # ".nc",
            "--dataset-config",
            "radar_NorthWestShelf_velocity_hourly_averaged_delayed_qc.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)
