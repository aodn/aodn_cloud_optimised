from typing import Tuple, Generator

import pandas as pd
import xarray as xr

from .GenericParquetHandler import GenericHandler


class AnmnHourlyTsHandler(GenericHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # TODO: rename JULD variable to TIME? or just copy it so that it's more consistent with other dataset?

    def preprocess_data(
        self, netcdf_fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        if self.is_valid_netcdf(netcdf_fp):
            # Use open_dataset as a context manager to ensure proper handling of the dataset
            with xr.open_dataset(netcdf_fp) as ds:
                # Convert xarray to pandas DataFrame
                assert set(ds.dims) == {
                    "OBSERVATION",
                    "INSTRUMENT",
                }, f"Unexpected dimensions {ds.dims.keys()}"

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
