from typing import Generator, Tuple

import pandas as pd
import xarray as xr

from .GenericParquetHandler import GenericHandler


class AnmnHourlyTsHandler(GenericHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def preprocess_data(
        self, netcdf_fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """
        Preprocess a NetCDF file containing Mooring Hourly timeseries product data.

        This method reads a NetCDF file, typically used for Mooring Hourly timeseries products,
        and processes it to yield a tuple of a pandas DataFrame and an xarray Dataset.

        The DataFrame contains the profile data with instrument information merged based on
        the 'instrument_index'. This method ensures proper handling of the dataset using
        a context manager and checks for expected dimensions and variables.

        :param netcdf_fp: Path to the input NetCDF file, or an open S3 file object (using s3fs).
        :return: Generator yielding tuples of (DataFrame, Dataset) where DataFrame contains
                 the profile data with instrument information, and Dataset is the corresponding
                 xarray Dataset.
        """

        # Use open_dataset as a context manager to ensure proper handling of the dataset
        with xr.open_dataset(netcdf_fp, engine="h5netcdf") as ds:
            # Convert xarray to pandas DataFrame
            assert set(ds.dims) == {
                "OBSERVATION",
                "INSTRUMENT",
            }, f"Unexpected dimensions {set(ds.dims)}"

            df = ds.drop_dims("INSTRUMENT").to_dataframe()
            instrument_info = ds.drop_dims("OBSERVATION").to_dataframe()

            assert df.shape[1] + instrument_info.shape[1] == len(
                ds.variables
            ), "Some variable depends on both dimensions"

            df = df.join(instrument_info, on="instrument_index")

            assert df.shape[1] == len(
                ds.variables
            ), "Something went wrong with the join"

            # Decode strings from bytes
            for col, dtype in df.dtypes.items():
                if dtype == object:
                    df[col] = df[col].astype(str)

            yield df, ds
