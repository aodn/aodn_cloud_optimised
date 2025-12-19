[![Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/aodn/aodn_cloud_optimised/)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/aodn/aodn_cloud_optimised/main?filepath=notebooks)

A curated list of Jupyter notebooks to query all AODN cloud optimised datasets.

# Run remotely

The notebooks can be run remotely using the following services:

- [Google Colab](https://colab.research.google.com/)
- [Nectar Research Cloud](https://jupyterhub.rc.nectar.org.au/)
- [Binder](https://mybinder.org/)

You can also click on the Binder or Colab button above to spin the environment and execute the notebooks (note that Binder is free with limited resources)

# Run locally

Assuming you have [Mamba/Conda](https://github.com/conda-forge/miniforge) already installed on your machine.

```bash
# clone the repository
git clone https://github.com/aodn/aodn_cloud_optimised.git
cd aodn_cloud_optimised/notebooks

# create the conda environment from the environment.yml file available in the notebook folder (not the repository root folder)
mamba env create --file=environment.yml
```

Then activate the environment named `AodnCloudOptimisedQuery`:

```bash
mamba activate AodnCloudOptimisedQuery
```

Finally, start Jupyter Notebook accessible on all network interfaces at port 8888:

```bash
jupyter notebook --ip=0.0.0.0 --port=8888
```

### 💡 Windows tip: Use WSL with Mamba

If you're on Windows, we recommend running this project inside **WSL (Windows Subsystem for Linux)** for better compatibility and support with Linux-based Python tools.

To install WSL, open **PowerShell as Administrator** and run:

```powershell
wsl --install
```

More information available [here](https://github.com/conda-forge/miniforge?tab=readme-ov-file#windows-subsystem-for-linux-wsl)

# Generic method to call any AODN parquet dataset

- [GetAodnData.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/GetAodnData.ipynb)

# AODN Notebooks directly loadable into Google Colab

- [animal_acoustic_tracking_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/animal_acoustic_tracking_delayed_qc.ipynb)
- [animal_ctd_satellite_relay_tagging_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/animal_ctd_satellite_relay_tagging_delayed_qc.ipynb)
- [argo.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/argo.ipynb)
- [autonomous_underwater_vehicle.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/autonomous_underwater_vehicle.ipynb)
- [diver_benthic_cover_in_situ_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/diver_benthic_cover_in_situ_qc.ipynb)
- [diver_cryptobenthic_fish_abundance_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/diver_cryptobenthic_fish_abundance_qc.ipynb)
- [diver_mobile_macroinvertebrate_abundance_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/diver_mobile_macroinvertebrate_abundance_qc.ipynb)
- [diver_off_transect_species_observations_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/diver_off_transect_species_observations_qc.ipynb)
- [diver_photoquadrat_score_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/diver_photoquadrat_score_qc.ipynb)
- [diver_reef_fish_abundance_biomass_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/diver_reef_fish_abundance_biomass_qc.ipynb)
- [diver_site_information_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/diver_site_information_qc.ipynb)
- [diver_survey_metadata_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/diver_survey_metadata_qc.ipynb)
- [model_sea_level_anomaly_gridded_realtime.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/model_sea_level_anomaly_gridded_realtime.ipynb)
- [mooring_acidification_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_acidification_delayed_qc.ipynb)
- [mooring_acidification_realtime_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_acidification_realtime_qc.ipynb)
- [mooring_ctd_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_ctd_delayed_qc.ipynb)
- [mooring_hourly_timeseries_delayed_qc](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_hourly_timeseries_delayed_qc.ipynb)
- [mooring_satellite_altimetry_calibration_validation.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_satellite_altimetry_calibration_validation.ipynb)
- [mooring_southern_ocean_surface_fluxes_realtime_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_southern_ocean_surface_fluxes_realtime_qc.ipynb)
- [mooring_southern_ocean_surface_properties_realtime_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_southern_ocean_surface_properties_realtime_qc.ipynb)
- [mooring_temperature_logger_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_temperature_logger_delayed_qc.ipynb)
- [mooring_timeseries_realtime_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_timeseries_realtime_qc.ipynb)
- [mooring_wave_timeseries_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/mooring_wave_timeseries_delayed_qc.ipynb)
- [radar_BonneyCoast_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_BonneyCoast_velocity_hourly_averaged_delayed_qc.ipynb)
- [radar_CapricornBunkerGroup_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_CapricornBunkerGroup_velocity_hourly_averaged_delayed_qc.ipynb)
- [radar_CapricornBunkerGroup_wave_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_CapricornBunkerGroup_wave_delayed_qc.ipynb)
- [radar_CapricornBunkerGroup_wind_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_CapricornBunkerGroup_wind_delayed_qc.ipynb)
- [radar_CoffsHarbour_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_CoffsHarbour_velocity_hourly_averaged_delayed_qc.ipynb)
- [radar_CoffsHarbour_wave_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_CoffsHarbour_wave_delayed_qc.ipynb)
- [radar_CoffsHarbour_wind_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_CoffsHarbour_wind_delayed_qc.ipynb)
- [radar_CoralCoast_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_CoralCoast_velocity_hourly_averaged_delayed_qc.ipynb)
- [radar_Newcastle_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_Newcastle_velocity_hourly_averaged_delayed_qc.ipynb)
- [radar_NorthWestShelf_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_NorthWestShelf_velocity_hourly_averaged_delayed_qc.ipynb)
- [radar_RottnestShelf_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_RottnestShelf_velocity_hourly_averaged_delayed_qc.ipynb)
- [radar_RottnestShelf_wave_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_RottnestShelf_wave_delayed_qc.ipynb)
- [radar_RottnestShelf_wind_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_RottnestShelf_wind_delayed_qc.ipynb)
- [radar_SouthAustraliaGulfs_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_SouthAustraliaGulfs_velocity_hourly_averaged_delayed_qc.ipynb)
- [radar_SouthAustraliaGulfs_wave_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_SouthAustraliaGulfs_wave_delayed_qc.ipynb)
- [radar_SouthAustraliaGulfs_wind_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_SouthAustraliaGulfs_wind_delayed_qc.ipynb)
- [radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.ipynb)
- [satellite_chlorophylla_carder_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_carder_1day_aqua.ipynb)
- [satellite_chlorophylla_gsm_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_gsm_1day_aqua.ipynb)
- [satellite_chlorophylla_gsm_1day_noaa20.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_gsm_1day_noaa20.ipynb)
- [satellite_chlorophylla_gsm_1day_snpp.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_gsm_1day_snpp.ipynb)
- [satellite_chlorophylla_oc3_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_oc3_1day_aqua.ipynb)
- [satellite_chlorophylla_oc3_1day_noaa20.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_oc3_1day_noaa20.ipynb)
- [satellite_chlorophylla_oc3_1day_snpp.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_oc3_1day_snpp.ipynb)
- [satellite_chlorophylla_oci_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_oci_1day_aqua.ipynb)
- [satellite_chlorophylla_oci_1day_noaa20.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_oci_1day_noaa20.ipynb)
- [satellite_chlorophylla_oci_1day_snpp.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_chlorophylla_oci_1day_snpp.ipynb)
- [satellite_diffuse_attenuation_coefficent_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_diffuse_attenuation_coefficent_1day_aqua.ipynb)
- [satellite_diffuse_attenuation_coefficent_1day_noaa20.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_diffuse_attenuation_coefficent_1day_noaa20.ipynb)
- [satellite_diffuse_attenuation_coefficent_1day_snpp.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_diffuse_attenuation_coefficent_1day_snpp.ipynb)
- [satellite_ghrsst_l3c_1day_nighttime_himawari8.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3c_1day_nighttime_himawari8.ipynb)
- [satellite_ghrsst_l3c_4hour_himawari8.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3c_4hour_himawari8.ipynb)
- [satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_southernocean.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_southernocean.ipynb)
- [satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_1day_daynighttime_single_sensor_southernocean.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1day_daynighttime_single_sensor_southernocean.ipynb)
- [satellite_ghrsst_l3s_1day_daytime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1day_daytime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_1day_nighttime_geopolar_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1day_nighttime_geopolar_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_1day_nighttime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1day_nighttime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_1month_daynighttime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1month_daynighttime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_1month_daytime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1month_daytime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_1month_daytime_single_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1month_daytime_single_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_1month_nighttime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_1month_nighttime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_3day_daynighttime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_3day_daynighttime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_3day_daynighttime_single_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_3day_daynighttime_single_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_3day_daytime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_3day_daytime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_3day_nighttime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_3day_nighttime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_6day_daynighttime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_6day_daynighttime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_6day_daynighttime_single_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_6day_daynighttime_single_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_6day_daytime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_6day_daytime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l3s_6day_nighttime_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l3s_6day_nighttime_multi_sensor_australia.ipynb)
- [satellite_ghrsst_l4_gamssa_1day_multi_sensor_world.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l4_gamssa_1day_multi_sensor_world.ipynb)
- [satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.ipynb)
- [satellite_nanoplankton_fraction_oc3_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_nanoplankton_fraction_oc3_1day_aqua.ipynb)
- [satellite_net_primary_productivity_gsm_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_net_primary_productivity_gsm_1day_aqua.ipynb)
- [satellite_net_primary_productivity_oc3_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_net_primary_productivity_oc3_1day_aqua.ipynb)
- [satellite_optical_water_type_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_optical_water_type_1day_aqua.ipynb)
- [satellite_picoplankton_fraction_oc3_1day_aqua.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/satellite_picoplankton_fraction_oc3_1day_aqua.ipynb)
- [slocum_glider_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/slocum_glider_delayed_qc.ipynb)
- [srs_oc_ljco_wqm_daily.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/srs_oc_ljco_wqm_daily.ipynb)
- [station_nrs_wireless_sensor_network_realtime_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/station_nrs_wireless_sensor_network_realtime_qc.ipynb)
- [station_wireless_sensor_network_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/station_wireless_sensor_network_delayed_qc.ipynb)
- [vessel_air_sea_flux_product_delayed.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_air_sea_flux_product_delayed.ipynb)
- [vessel_air_sea_flux_sst_meteo_realtime.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_air_sea_flux_sst_meteo_realtime.ipynb)
- [vessel_co2_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_co2_delayed_qc.ipynb)
- [vessel_fishsoop_realtime_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_fishsoop_realtime_qc.ipynb)
- [vessel_nrs_ctd_profiles_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_nrs_ctd_profiles_delayed_qc.ipynb)
- [vessel_satellite_radiance_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_satellite_radiance_delayed_qc.ipynb)
- [vessel_satellite_radiance_derived_product.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_satellite_radiance_derived_product.ipynb)
- [vessel_sst_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_sst_delayed_qc.ipynb)
- [vessel_sst_realtime_nonqc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_sst_realtime_nonqc.ipynb)
- [vessel_trv_realtime_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_trv_realtime_qc.ipynb)
- [vessel_xbt_delayed_qc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_xbt_delayed_qc.ipynb)
- [vessel_xbt_realtime_nonqc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/vessel_xbt_realtime_nonqc.ipynb)
- [wave_buoy_realtime_nonqc.ipynb](https://githubtocolab.com/aodn/aodn_cloud_optimised/blob/main/notebooks/wave_buoy_realtime_nonqc.ipynb)
