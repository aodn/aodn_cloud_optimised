#!/usr/bin/env python3
import subprocess


def main():
    imos_paths = [f"IMOS/SRS/SST/ghrsst/L3S-1d/dn/{year}" for year in range(1992, 2025)]

    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn",
        *imos_paths,
        # "--filters",
        # "200005",
        "--dataset-config",
        "satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
