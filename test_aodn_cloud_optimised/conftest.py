import os

# Dataset config files that intentionally contain placeholders/templates and are
# expected to fail strict DatasetConfig validation in config sweeps.
PLACEHOLDER_DATASET_CONFIG_FILES = frozenset(
    {
        "dataset_template.json",
        "mooring_wave_timeseries_delayed_qc.json",
        "radar_wave_delayed_qc_no_I_J_version_main.json",
        "satellite_ghrsst_l3c_4hour_himawari8.json",
        "satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_southernocean.json",
        "satellite_nanoplankton_fraction_oc3_1day_aqua.json",
        "satellite_optical_water_type_1day_snpp.json",
        "satellite_picoplankton_fraction_oc3_1day_aqua.json",
        "satellite_sst_1day_aqua.json",
        "satellite_sst_1day_snpp.json",
        "station_wireless_sensor_network_delayec_qc.json",
    }
)

# On macOS with Homebrew, libudunits2 lives in /opt/homebrew/lib which is not
# in the default ctypes search path.  Set DYLD_LIBRARY_PATH before any test
# module is imported so that cfunits (imported transitively by dataset_config)
# can find the library at collection time.
if os.uname().sysname == "Darwin":
    existing = os.environ.get("DYLD_LIBRARY_PATH", "")
    homebrew_lib = "/opt/homebrew/lib"
    if homebrew_lib not in existing.split(":"):
        os.environ["DYLD_LIBRARY_PATH"] = (
            f"{homebrew_lib}:{existing}" if existing else homebrew_lib
        )
