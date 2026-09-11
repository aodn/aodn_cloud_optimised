from typing import Generator, List, Tuple

import pandas as pd
import xarray as xr

from .GenericParquetHandler import GenericHandler

PROFILE_DIM = "N_PROF"
LEVEL_DIM = "N_LEVELS"


class ArgoHandler(GenericHandler):
    """Handler for Argo (and MEOP) aggregated profile files (``*_prof.nc``).

    The source files are 2D aggregations of many profiles (``N_PROF`` x
    ``N_LEVELS``). They are flattened to a tabular representation where each row
    is a single measurement level, and the per-profile metadata (CYCLE_NUMBER,
    PLATFORM_NUMBER, JULD ...) is repeated across all the levels of its profile.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # TODO: rename JULD variable to TIME? or just copy it so that it's more consistent with other dataset?

    @staticmethod
    def _split_variables(ds: xr.Dataset) -> Tuple[List[str], List[str]]:
        """Split the dataset variables into the two groups making up a row.

        :param ds: the source ``*_prof.nc`` dataset.
        :return: (measurement variables on (N_PROF, N_LEVELS),
                  per-profile metadata variables on (N_PROF,)).
            Variables on any other dimension (N_PARAM, N_CALIB, N_HISTORY,
            DATE_TIME ...) are ignored as they cannot be mapped onto a level.
        """
        measurement_vars = [
            str(name)
            for name, var in ds.variables.items()
            if var.dims == (PROFILE_DIM, LEVEL_DIM)
        ]
        profile_vars = [
            str(name)
            for name, var in ds.variables.items()
            if var.dims == (PROFILE_DIM,)
        ]
        return measurement_vars, profile_vars

    @staticmethod
    def _validate_positions(ds: xr.Dataset, path: str) -> None:
        """Ensure the file has at least one usable position.

        A very small amount of floats were  published with LATITUDE and
        LONGITUDE which have ``_FillValue`` (99999) and POSITION_QC as 9, which
        xarray decodes to NaN. We reject those files

        :param ds: the source ``*_prof.nc`` dataset.
        :param path: source file path, used for the error message.
        :raises ValueError: if LATITUDE/LONGITUDE are missing or entirely NaN.
        """
        missing = [name for name in ("LATITUDE", "LONGITUDE") if name not in ds]
        if missing:
            raise ValueError(f"{path} has no {' and '.join(missing)} variable")

        empty = [
            name for name in ("LATITUDE", "LONGITUDE") if bool(ds[name].isnull().all())
        ]
        if empty:
            raise ValueError(
                f"{path} has no valid position: {' and '.join(empty)} is entirely "
                "NaN (fill value) for every profile. The file cannot be spatially "
                "partitioned and is skipped."
            )

    @staticmethod
    def _decode_platform_number(values: pd.Series) -> pd.Series:
        """Normalise PLATFORM_NUMBER, stored as a space padded byte string, to an int."""
        return values.apply(
            lambda x: int(x.decode("UTF-8").strip()) if isinstance(x, bytes) else x
        )

    @staticmethod
    def _to_xarray(df: pd.DataFrame) -> xr.Dataset:
        """Convert the flattened dataframe back to a Dataset, restoring attributes.

        ``DataFrame.to_xarray()`` drops both the global and the variable
        attributes, so they are re-applied from the dataframe.
        """
        ds = df.to_xarray()
        ds.attrs.update(df.attrs)
        for name in df.columns:
            ds[name].attrs.update(df[name].attrs)
        return ds

    def preprocess_data(
        self, fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """
        Preprocess a NetCDF file containing aggregated profile data.

        This method reads a profile NetCDF file (typically named with a *_prof.nc suffix),
        which is an aggregation of multiple profile files, and returns a generator
        yielding a tuple of a pandas DataFrame and an xarray Dataset.

        :param fp: Path to the input NetCDF file, or an open S3 file object (using s3fs) of an Argo *_prof.nc file.
        :return: Generator yielding tuples of (DataFrame, Dataset) where DataFrame contains the profile data
                 and Dataset is the corresponding xarray Dataset.
        """
        if not fp.path.endswith("_prof.nc"):
            raise ValueError(f"{fp.path} is not an aggregated profile file (*_prof.nc)")

        with xr.open_dataset(fp, engine="scipy") as ds:
            self._validate_positions(ds, fp.path)

            measurement_vars, profile_vars = self._split_variables(ds)
            if not measurement_vars:
                raise ValueError(
                    f"{fp.path} has no variable on ({PROFILE_DIM}, {LEVEL_DIM}); "
                    "this is not a valid aggregated profile file"
                )

            # aligns every variable onto the full dimension product  (N_PROF, N_LEVELS)
            columns = measurement_vars + profile_vars
            df = (
                ds[columns]
                .to_dataframe()
                .reset_index(drop=True)
                .reindex(columns=columns)
            )

            df["PLATFORM_NUMBER"] = self._decode_platform_number(df["PLATFORM_NUMBER"])

            # NOTE: do NOT call self.convert_df_bytes_to_str() here. For MEOP data
            # JULD is an object dtype when read with the scipy engine, and the
            # conversion turns the datetimes back into strings, yielding NaT times.

            df.attrs = dict(ds.attrs)
            for name in df.columns:
                df[name].attrs = dict(ds[name].attrs)

            yield df, self._to_xarray(df)
