#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/ACORN/gridded_1h-avg-current-map_QC/BONC/",
        # "--filters",
        # "FILTER_STRING_1",
        # "FILTER_STRING_1",
        "--dataset-config",
        "radar_BonneyCoast_velocity_hourly_averaged_delayed_qc.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
