#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/SRS/SST/ghrsst/L3S-1d/dn",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/1992",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/1993",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/1994",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/1995",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/1996",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/1997",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/1998",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/1999",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2000",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2001",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2002",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2003",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2004",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2005",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2006",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2007",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2008",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2009",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2010",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2011",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2012",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2013",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2014",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2015",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2016",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2017",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2018",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2019",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2020",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2021",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2022",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2023",
        # "IMOS/SRS/SST/ghrsst/L3S-1d/dn/2024",
        # "--filters",
        # "200005",
        "--dataset-config",
        "satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
