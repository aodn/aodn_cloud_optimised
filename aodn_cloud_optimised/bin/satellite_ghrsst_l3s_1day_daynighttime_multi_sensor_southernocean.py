#!/usr/bin/env python3
import subprocess


def main():

    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/",
        # "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2021",  # 2021031* 202102* required a t3.large scheduler, otherwise it would fail!!
        # "IMOS/SRS/SST/ghrsst/L3SM-1dS/dn/2024",
        # "--filters",
        # "/2018121",
        # "/2019020",  # watch out for 20190205 when sst got emptied because of map_blocks?
        # "/2022021",  # failing if not enough memory available on the machine running the cluster!! not the scheduler
        # "/202103", # required a t3.xlarge for 20 days! failed with 30days
        "--dataset-config",
        "satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_southernocean.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
