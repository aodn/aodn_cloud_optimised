import inspect
import typing

import pandas as pd
import polars as pl
import pydantic


class CSVConfigModel(pydantic.BaseModel):
    pandas_read_csv_config: dict[str, typing.Any] | None = pydantic.Field(default=None)
    polars_read_csv_config: dict[str, typing.Any] | None = pydantic.Field(default=None)

    @pydantic.model_validator(mode="after")
    def validate_csv_configs(self):
        """Ensure only one CSV config is set, and its keys are valid for the respective reader."""
        pandas_cfg = self.pandas_read_csv_config
        polars_cfg = self.polars_read_csv_config

        # Rule 1: mutual exclusivity
        if pandas_cfg and polars_cfg:
            raise ValueError(
                "Only one of pandas_read_csv_config or polars_read_csv_config can be provided."
            )

        # Rule 2: validate keys
        if pandas_cfg:
            valid_pandas_keys = set(inspect.signature(pd.read_csv).parameters.keys())
            invalid = [k for k in pandas_cfg if k not in valid_pandas_keys]
            if invalid:
                raise ValueError(
                    f"Invalid pandas_read_csv_config keys: {invalid}. "
                    f"Valid options: {sorted(valid_pandas_keys)}"
                )

        if polars_cfg:
            valid_polars_keys = set(inspect.signature(pl.read_csv).parameters.keys())
            invalid = [k for k in polars_cfg if k not in valid_polars_keys]
            if invalid:
                raise ValueError(
                    f"Invalid polars_read_csv_config keys: {invalid}. "
                    f"Valid options: {sorted(valid_polars_keys)}"
                )

        return self
