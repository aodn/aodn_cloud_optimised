#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SOOP/SOOP-ASF/",
        "--filters",
        "flux_product",
        "--dataset-config",
        "vessel_air_sea_flux_product_delayed.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()