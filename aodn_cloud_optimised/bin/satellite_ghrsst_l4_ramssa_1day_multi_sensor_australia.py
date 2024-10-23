#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/SST/ghrsst/L4/RAMSSA/",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2006",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2007",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2008",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2009",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2010",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2011",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2012",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2013",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2014",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2015",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2016",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2017",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2018",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2019",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2020",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2021",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2022",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2023",
        # "IMOS/SRS/SST/ghrsst/L4/RAMSSA/2024",
        # "--filters",
        # "FILTER_STRING_1",
        # "FILTER_STRING_1",
        "--dataset-config",
        "satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
