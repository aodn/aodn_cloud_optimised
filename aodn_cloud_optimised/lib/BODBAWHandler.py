from typing import Generator, Tuple

import numpy as np
import pandas as pd
import xarray as xr

from .GenericParquetHandler import GenericHandler


class BODBAWHandler(GenericHandler):
    """Handler for IMOS Bio-Optical Database of Australian Waters (BODBAW) NetCDF files.

    BODBAW files use a CF indexed ragged array structure:

    - ``station`` dimension  — unique sampling locations, carrying ``LATITUDE``,
      ``LONGITUDE``, and ``station_name``.
    - ``profile`` dimension  — individual casts; carries ``TIME`` and a
      ``station_index`` coordinate that links each profile back to its station.
    - ``obs`` dimension      — depth observations; linked to profiles via the
      ``row_size`` variable (contiguous ragged array).
    - ``wavelength`` dimension (absorption / backscattering files only) — spectral
      axis for 2-D data variables with shape ``(obs, wavelength)``.

    ``xr.open_dataset(...).to_dataframe()`` cannot flatten this structure correctly
    because ``TIME``, ``LATITUDE``, and ``LONGITUDE`` live on different dimensions.
    This handler performs the expansion manually and, when configured with
    ``"spectral_flatten": true`` in ``netcdf_read_config``, additionally explodes
    the wavelength axis into a long-format ``wavelength`` column so that every row
    in the output DataFrame corresponds to a single (time, lat, lon, depth, wavelength)
    observation.

    Configuration option (in ``netcdf_read_config``):
        ``spectral_flatten`` (bool, default False): When True, 2-D ``(obs, wavelength)``
            variables are melted into a long-format table with a ``wavelength`` column.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def preprocess_data(
        self, netcdf_fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """Flatten a BODBAW NetCDF file into a tidy DataFrame.

        Args:
            netcdf_fp: Path (str) or open S3 file object for the source NetCDF.

        Yields:
            tuple[pd.DataFrame, xr.Dataset]: Flattened DataFrame and a
                corresponding xarray Dataset reconstructed from it.
        """
        netcdf_read_config = self.dataset_config.get("netcdf_read_config", {})
        spectral_flatten = netcdf_read_config.get("spectral_flatten", False)

        try:
            with xr.open_dataset(netcdf_fp, engine="h5netcdf") as ds:
                df = self._flatten(ds, spectral_flatten=spectral_flatten)
        except Exception as exc:
            # Only retry with scipy for file-format errors, not logic errors.
            if "not a valid NetCDF" not in str(exc) and "not the signature" not in str(
                exc
            ):
                raise
            self.logger.warning(
                f"{self.uuid_log}: h5netcdf engine failed; retrying with scipy."
            )
            if hasattr(netcdf_fp, "seek"):
                netcdf_fp.seek(0)
            with xr.open_dataset(netcdf_fp, engine="scipy") as ds:
                df = self._flatten(ds, spectral_flatten=spectral_flatten)

        # Reconstruct an xarray Dataset so the rest of the pipeline can check
        # variable attributes.
        ds_out = df.to_xarray()

        yield df, ds_out

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _flatten(self, ds: xr.Dataset, *, spectral_flatten: bool) -> pd.DataFrame:
        """Core flattening logic.

        Expands the ragged station→profile→obs hierarchy into a flat table,
        then optionally explodes the wavelength dimension.

        Args:
            ds: The opened xarray Dataset.
            spectral_flatten: When True, 2-D ``(obs, wavelength)`` variables are
                exploded into long format.

        Returns:
            Flat pandas DataFrame.
        """
        station_index = ds["station_index"].values  # shape (n_profiles,)
        # station_index is 1-indexed per the CF DSG convention used in BODBAW files
        station_index = station_index - 1

        row_size = ds["row_size"].values.copy()  # shape (n_profiles,)
        n_obs_ds = int(ds.sizes["obs"])

        # Guard: negative or corrupted row_size values observed in some files.
        # 91/92 backscattering files have a systematic upstream data issue where the
        # last row_size element is a large negative integer (e.g. -484).  The positive
        # elements sum to (n_obs_ds - 1) observations, so clipping the negative and
        # recomputing the last element gives the correct ragged-array mapping.
        if (row_size < 0).any() or int(row_size.sum()) != n_obs_ds:
            neg_indices = np.where(row_size < 0)[0]
            neg_values = row_size[neg_indices].tolist()
            self.logger.warning(
                f"{self.uuid_log}: row_size contains corrupted values "
                f"(negative at indices {neg_indices.tolist()}: {neg_values}). "
                f"Positive elements sum to {int(np.clip(row_size, 0, None).sum())} "
                f"vs obs dimension {n_obs_ds}. Correcting last element."
            )
            row_size = np.clip(row_size, 0, None)
            remainder = n_obs_ds - int(row_size[:-1].sum())
            row_size[-1] = max(0, remainder)

        n_obs = int(row_size.sum())
        n_profiles = len(row_size)

        # Build obs-level arrays for the coordinate dimensions
        time_vals = ds["TIME"].values  # (n_profiles,)
        lat_vals = ds["LATITUDE"].values  # (n_stations,)
        lon_vals = ds["LONGITUDE"].values  # (n_stations,)

        obs_time = np.empty(n_obs, dtype=time_vals.dtype)
        obs_lat = np.empty(n_obs, dtype=lat_vals.dtype)
        obs_lon = np.empty(n_obs, dtype=lon_vals.dtype)

        obs_ends = np.cumsum(row_size)
        obs_starts = np.concatenate([[0], obs_ends[:-1]])

        for p in range(n_profiles):
            si = station_index[p]
            sl = slice(obs_starts[p], obs_ends[p])
            obs_time[sl] = time_vals[p]
            obs_lat[sl] = lat_vals[si]
            obs_lon[sl] = lon_vals[si]

        # station_name — decode bytes if needed
        raw_names = ds["station_name"].values  # (n_stations,)
        if raw_names.dtype.kind in ("S", "O"):  # bytes or object
            decoded_names = np.array(
                [
                    (
                        n.decode("utf-8").strip()
                        if isinstance(n, bytes)
                        else str(n).strip()
                    )
                    for n in raw_names
                ]
            )
        else:
            decoded_names = raw_names.astype(str)

        obs_station = np.empty(n_obs, dtype=object)
        for p in range(n_profiles):
            si = station_index[p]
            sl = slice(obs_starts[p], obs_ends[p])
            obs_station[sl] = decoded_names[si]

        # Assemble base DataFrame (obs-level, flat variables only)
        df = pd.DataFrame(
            {
                "TIME": obs_time,
                "LATITUDE": obs_lat,
                "LONGITUDE": obs_lon,
                "station_name": obs_station,
                "DEPTH": ds["DEPTH"].values,
            }
        )

        # Separate 1-D (obs,) variables from 2-D (obs, wavelength) variables
        flat_vars = []
        spectral_vars = []  # list of (varname, values_2d)
        wavelength_vals = None

        for varname, var in ds.data_vars.items():
            if varname in ("station_name", "row_size"):
                continue  # already handled / index bookkeeping
            if var.dims == ("obs",):
                flat_vars.append(varname)
            elif (
                len(var.dims) == 2 and var.dims[0] == "obs" and "wavelength" in var.dims
            ):
                spectral_vars.append((varname, var.values))
                if wavelength_vals is None:
                    wavelength_vals = ds["wavelength"].values

        # Add flat data vars
        for varname in flat_vars:
            df[varname] = ds[varname].values

        if not spectral_flatten or not spectral_vars:
            # No spectral explosion needed (or not requested)
            return df

        # ------------------------------------------------------------------
        # Spectral explosion: repeat each obs row once per wavelength
        # ------------------------------------------------------------------
        n_wl = len(wavelength_vals)

        # Tile the flat DataFrame (each row repeated n_wl times)
        df_tiled = df.loc[df.index.repeat(n_wl)].reset_index(drop=True)
        df_tiled["wavelength"] = np.tile(wavelength_vals, n_obs)

        # Add each spectral variable
        for varname, values_2d in spectral_vars:
            # values_2d shape: (n_obs, n_wl) → flatten in C order to match tiling
            df_tiled[varname] = values_2d.ravel()

        return df_tiled
