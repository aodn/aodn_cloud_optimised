#!/usr/bin/env python3
import subprocess


# TODO: blow up of scheduler memory after 27/71 batches. very annoying why memory is not flushes... Quick fix is to increate the memory scheduler, go from small to large
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
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
