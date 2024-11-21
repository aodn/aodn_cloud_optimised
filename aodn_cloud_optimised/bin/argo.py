#!/usr/bin/env python3
import subprocess


def main():
    # splitting the path in a few commands so that the clusters are being recreated to avoid memory issues:
    # task 1
    # command = [
    #     "generic_cloud_optimised_creation",
    #     "--paths",
    #     "IMOS/Argo/dac/csiro",
    #     "IMOS/Argo/dac/kordi",
    #     "IMOS/Argo/dac/bodc",
    #     "IMOS/Argo/dac/csio",
    #     "IMOS/Argo/dac/incois",
    #     "IMOS/Argo/dac/jma",
    #     "--suffix",
    #     "_prof.nc",
    #     "--dataset-config",
    #     "argo.json",
    #     # "--clear-existing-data",
    #     "--force-previous-parquet-deletion",
    #     "--cluster-mode",
    #     "coiled",
    # ]
    #
    # # Run the command
    # subprocess.run(command, check=True)

    # task 2
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/Argo/dac/coriolis",
        "--suffix",
        "_prof.nc",
        "--dataset-config",
        "argo.json",
        "--force-previous-parquet-deletion",
        "--cluster-mode",
        "coiled",
    ]
    subprocess.run(command, check=True)

    # task 2
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/Argo/dac/aoml",
        "IMOS/Argo/dac/nmdis",
        "IMOS/Argo/dac/meds",
        "IMOS/Argo/dac/kma",
        "--suffix",
        "_prof.nc",
        "--dataset-config",
        "argo.json",
        "--force-previous-parquet-deletion",
        "--cluster-mode",
        "coiled",
    ]
    subprocess.run(command, check=True)

    # task 3
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/Argo/dac/nmdis",
        "IMOS/Argo/dac/meds",
        "IMOS/Argo/dac/kma",
        "--suffix",
        "_prof.nc",
        "--dataset-config",
        "argo.json",
        "--force-previous-parquet-deletion",
        "--cluster-mode",
        "coiled",
    ]
    subprocess.run(command, check=True)
