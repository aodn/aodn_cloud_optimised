#!/usr/bin/env python3
import subprocess


def main():
    # dataset starts in 2015
    for i, year in enumerate(range(2015, 2025)):
        # Prepare the command
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/SST/ghrsst/L3C-1d/ngt/h08/{year}",
            # "--filters",
            # ".nc",
            "--dataset-config",
            "satellite_ghrsst_l3c_1day_nighttime_himawari8.json",
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
