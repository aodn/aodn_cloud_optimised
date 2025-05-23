#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/",
        "Bureau_of_Meteorology/WAVE-BUOYS/REALTIME/",
        "Deakin_University/WAVE-BUOYS/REALTIME",
        "--dataset-config",
        "wave_buoy_realtime_nonqc.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)
