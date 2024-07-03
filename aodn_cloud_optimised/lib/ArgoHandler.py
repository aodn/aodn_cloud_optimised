from typing import Tuple, Generator

import numpy as np
import pandas as pd
import xarray as xr

from .GenericParquetHandler import GenericHandler


class ArgoHandler(GenericHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # TODO: rename JULD variable to TIME? or just copy it so that it's more consistent with other dataset?

    def preprocess_data(
        self, netcdf_fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """
        Preprocess a NetCDF file containing aggregated profile data.

        This method reads a profile NetCDF file (typically named with a *_prof.nc suffix),
        which is an aggregation of multiple profile files, and returns a generator
        yielding a tuple of a pandas DataFrame and an xarray Dataset.

        :param netcdf_fp: Path to the input NetCDF file, or an open S3 file object (using s3fs) of an Argo *_prof.nc file.
        :return: Generator yielding tuples of (DataFrame, Dataset) where DataFrame contains the profile data
                 and Dataset is the corresponding xarray Dataset.
        """

        if not netcdf_fp.path.endswith("_prof.nc"):
            raise ValueError

        with xr.open_dataset(netcdf_fp) as ds:
            # create dataframe
            prof_variables = []
            param_variables = []
            date_info_variables = []
            prof_info_variables = []

            df_profile_data = pd.DataFrame()
            n_profiles = ds["PRES"].shape[1]

            for varname in ds.keys():
                # find profile variables

                if len(ds[varname].dims) == 2:

                    # condition on variables containing profile data (PSAL, PRES ...)
                    if (
                        ds[varname].dims[0] == "N_PROF"
                        and ds[varname].dims[1] == "N_LEVELS"
                    ):
                        prof_variables.append(varname)
                        temporary_df = ds[varname].values.reshape(ds[varname].size)

                        df_profile_data[varname] = temporary_df

                    elif (
                        ds[varname].dims[0] == "N_PROF"
                        and ds[varname].dims[1] == "N_PARAM"
                    ):
                        param_variables.append(varname)  # this is not used

                if len(ds[varname].dims) == 1:
                    if ds[varname].dims[0] == "DATE_TIME":
                        date_info_variables.append(varname)  # this is not used

                    # condition on variables containing profile metadata (CYCLE_NUMBER, PLATFORM NUMBER ...)
                    # data is repeated to match profile data
                    elif ds[varname].dims[0] == "N_PROF":
                        prof_info_variables.append(varname)
                        repeat_array = np.transpose([ds[varname].values] * n_profiles)
                        temporary_df = repeat_array.reshape(repeat_array.size)

                        df_profile_data[varname] = temporary_df

            df_profile_data["PLATFORM_NUMBER"] = df_profile_data[
                "PLATFORM_NUMBER"
            ].apply(
                lambda x: int(x.decode("UTF-8").strip()) if isinstance(x, bytes) else x
            )
            df_profile_data = self.convert_df_bytes_to_str(df_profile_data)

            # since we modified the dataframe, let's put it back into the xarray dataset
            ds = df_profile_data.to_xarray()

            yield df_profile_data, ds
