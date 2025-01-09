#!/usr/bin/env python3
import subprocess


def main():
    # starts in 2009, no data in 2013, 2014
    for i, year in enumerate(range(2009, 2025)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/{year}",
            # "--filters",
            # ".nc",
            "--dataset-config",
            "radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json",
            "--cluster-mode",
            "coiled",
        ]

        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
