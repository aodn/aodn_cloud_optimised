#!/usr/bin/env python3
import subprocess


def main():

    for i, year in enumerate(range(2018, 2025)):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/{year}",
            # "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2021",  # 2021031* 202102* required a t3.large scheduler, otherwise it would fail!!
            # "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2024",
            # "--filters",
            # "/2018121",
            # "/2019020",  # watch out for 20190205 when sst got emptied because of map_blocks?
            # "/2022021",  # failing if not enough memory available on the machine running the cluster!! not the scheduler
            # "/202103", # required a t3.xlarge for 20 days! failed with 30days
            "--dataset-config",
            "satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_southernocean.json",
            "--cluster-mode",
            "coiled",
        ]
        #
        # Add --clear-existing-data for the first iteration only
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)
