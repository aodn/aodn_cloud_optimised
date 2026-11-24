from typing import Generator, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import xarray as xr

from .GenericParquetHandler import GenericHandler


class AatamsSatelliteTaggingHandler(GenericHandler):
    """Handler for IMOS AATAMS satellite tagging NRT CSV files.

    These CSV files contain **Packed-String Columns** — cells that hold multiple
    comma-separated numeric values representing a profile's depth levels or
    time-series observations (e.g. ``temp_vals = "5.476,5.467,5.443"``).

    This handler performs **String Column Expansion**: each source row containing
    N packed values is expanded into N output rows, with all scalar columns
    repeated. Packed-string columns are detected automatically — any
    ``object``-dtype column whose non-null values parse as comma-separated
    floats is treated as packed.

    Validation of these packed-string columns: Profile groups are validated independently
    based on relevant values for each row. For example,
    ``n_temp`` applies to ``temp_dbar``/``temp_vals``/``qc_temp``,
    ``n_sal`` applies to ``sal_dbar``/``sal_vals``/ ``sal_corrected_vals`` / ``qc_sal``,
    ``n_oxy`` applies to ``oxy_dbar``/``oxy_vals``,
    ``n_photo`` applies to ``photo_dbar``/``photo_vals``,
    ``n_fluoro`` applies to ``fluoro_dbar``/``fluoro_vals``.
    Different groups may therefore have different lengths in the same source row.
    For instance Fluoro is only measured until a certain depth and may have less data point than
    temperature for the same profile.

    The handler works for all eight animal tracking satellite tagging CSV types
    (ctd, dive, gps, haulout, metadata, diag, summary, ssmoutputs).
    For flat CSV types that contain no packed-string columns, auto-detection finds nothing
    to expand and the file passes through unchanged.

    Note: after running ``cloud_optimised_create_dataset_config`` on a packed CSV,
    the generated schema will show ``string`` for columns that are actually
    numeric after expansion.  These types **must be manually corrected** in the
    JSON config before processing.
    """

    PACKED_PROFILE_GROUPS = {
        "n_temp": {"temp_dbar", "temp_vals", "qc_temp"},
        "n_cond": {"cond_dbar", "cond_vals"},
        "n_sal": {"sal_dbar", "sal_vals", "sal_corrected_vals", "qc_sal"},
        "n_fluoro": {"fluoro_dbar", "fluoro_vals"},
        "n_oxy": {"oxy_dbar", "oxy_vals"},
        "n_photo": {"photo_dbar", "photo_vals"},
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def preprocess_data_csv(
        self, csv_fp
    ) -> Generator[Tuple[pd.DataFrame, xr.Dataset], None, None]:
        """Read a CSV, expand any packed-string columns, and yield (df, ds).

        Delegates CSV I/O to the parent class, then applies String Column
        Expansion on the resulting DataFrame before reconstructing the
        xarray Dataset.

        Args:
            csv_fp: Path (str) or open S3 file object for the source CSV.

        Yields:
            tuple[pd.DataFrame, xr.Dataset]: Expanded DataFrame and a
                corresponding xarray Dataset with schema attributes applied.

        Raises:
            ValueError: If any row has non-null packed columns with mismatched
                value counts (entire file is rejected).
        """
        for df, _ds in super().preprocess_data_csv(csv_fp):
            # The parent reads with index_col set to the time column; reset it to a
            # plain column before expansion so duplicate timestamps (one per depth
            # level) do not break pd.DataFrame.explode or xr.Dataset.from_dataframe.
            df = df.reset_index()
            df = self._expand_packed_string_columns(df)

            ds = xr.Dataset.from_dataframe(df)
            for var in ds.variables:
                if var in self.schema:
                    attrs = {k: v for k, v in self.schema[var].items() if k != "type"}
                    ds[var].attrs = attrs

            yield df, ds

    @staticmethod
    def _holds_strings(series: pd.Series) -> bool:
        """Return whether *series* can hold strings, across pandas versions.

        pandas 2 reads text columns as ``object``; pandas 3 infers a dedicated
        ``str`` dtype, for which ``dtype == object`` is False. Testing only for
        ``object`` therefore made every text column invisible under pandas 3, so
        packed profiles were never exploded and reached the parquet writer as
        strings against a schema declaring double.
        """
        return series.dtype == object or pd.api.types.is_string_dtype(series.dtype)

    def _detect_packed_string_columns(self, df: pd.DataFrame) -> List[str]:
        """Return names of columns that contain comma-separated numeric values.

        A column is classified as packed if:
        - it holds strings (``object`` on pandas 2, ``str`` on pandas 3)
        - at least one non-null value contains a comma
        - the first comma-containing value splits into float-parseable parts
        """
        packed = []
        for col in df.columns:
            if not self._holds_strings(df[col]):
                continue
            non_null = df[col].dropna()
            if non_null.empty:
                continue
            has_comma = non_null.astype(str).str.contains(",", na=False)
            if not has_comma.any():
                continue
            first_packed = non_null[has_comma].iloc[0]
            try:
                [float(x.strip()) for x in str(first_packed).split(",") if x.strip()]
                packed.append(col)
            except ValueError:
                pass
        return packed

    @staticmethod
    def _parse_packed_value(value) -> object:
        """Convert a packed string to a list of floats, or return the value unchanged."""
        if not isinstance(value, str):
            return value  # NaN or already-numeric → leave as-is
        parts = [x.strip() for x in value.split(",") if x.strip()]
        return [float(p) for p in parts]

    @staticmethod
    def _expected_packed_count(value) -> int | None:
        """Return an integer profile count, or ``None`` if it is missing."""
        if pd.isna(value):
            return None
        try:
            count = float(value)
        except (TypeError, ValueError):
            return None
        if not count.is_integer():
            return None
        return int(count)

    def _expand_packed_string_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect and expand packed-string columns in *df*.

        Packed profile types are handled independently:

        1. Parse and detect comma-packed columns.
        2. Validate columns within each ``n_*`` profile group.
        3. Check each actual group length against its ``n_*`` count.
        4. Pad the profile groups in every row to the row's maximum length.
        5. Explode all packed columns together and reset the index.
        """
        packed_cols = self._detect_packed_string_columns(df)

        if not packed_cols:
            # No comma-packed columns to explode, but a file may still store a
            # single-level profile as a lone string (e.g. fluoro_dbar = "12.5").
            # Coerce those to numeric so they do not leak through as strings.
            return self._coerce_numeric_string_columns(df)

        self.logger.info(
            f"{self.uuid_log}: Detected packed-string columns for expansion: {packed_cols}"
        )

        # --- Step 1: parse packed-string columns into lists of floats ---
        for col in packed_cols:
            df[col] = df[col].apply(self._parse_packed_value)

        # --- Step 2: determine profile lengths and validate each profile group ---
        length_df = pd.DataFrame(
            {
                col: df[col].apply(lambda v: len(v) if isinstance(v, list) else None)
                for col in packed_cols
            },
            index=df.index,
        )

        mapped_columns = {
            col
            for _count_col, group_columns in self.PACKED_PROFILE_GROUPS.items()
            for col in group_columns
            if col in packed_cols
        }
        validation_errors = []

        for idx in df.index:
            row = df.loc[idx]

            # Validate and collect the expected length of every mapped profile group.
            for count_col, group_columns in self.PACKED_PROFILE_GROUPS.items():
                group_cols = [col for col in group_columns if col in packed_cols]
                if not group_cols:
                    continue

                actual_lengths = {
                    int(length_df.at[idx, col])
                    for col in group_cols
                    if pd.notna(length_df.at[idx, col])
                }

                if len(actual_lengths) > 1:
                    validation_errors.append(
                        {
                            "row": idx,
                            "group": count_col,
                            "reason": "packed columns have mismatched lengths",
                            "lengths": {
                                col: length_df.at[idx, col] for col in group_cols
                            },
                        }
                    )
                    continue

                if count_col in df.columns:
                    expected = self._expected_packed_count(row[count_col])
                    if expected is not None and expected < 0:
                        validation_errors.append(
                            {"row": idx, "group": count_col, "reason": "negative count"}
                        )
                    elif expected is not None and actual_lengths:
                        actual = next(iter(actual_lengths))
                        if actual != expected:
                            validation_errors.append(
                                {
                                    "row": idx,
                                    "group": count_col,
                                    "reason": f"actual length {actual} != count {expected}",
                                }
                            )

            # Preserve validation for any packed numeric column not in the known groups.
            unmapped_cols = [col for col in packed_cols if col not in mapped_columns]
            if unmapped_cols:
                unmapped_lengths = {
                    int(length_df.at[idx, col])
                    for col in unmapped_cols
                    if pd.notna(length_df.at[idx, col])
                }
                if len(unmapped_lengths) > 1:
                    validation_errors.append(
                        {
                            "row": idx,
                            "group": "unmapped",
                            "reason": "packed columns have mismatched lengths",
                            "lengths": {
                                col: length_df.at[idx, col] for col in unmapped_cols
                            },
                        }
                    )

        if validation_errors:
            bad_count = len(validation_errors)
            msg = (
                f"{self.uuid_log}: Packed-string column length mismatch detected in "
                f"{bad_count} validation case(s). File rejected. Row details: {validation_errors}"
            )
            self.logger.error(msg)
            raise ValueError(msg)

        # --- Step 3: pad every profile group to the row's maximum length ---
        # Expand each row to the maximum of:
        # - its declared n_* counts
        # - its actual packed-list lengths
        # This pads shorter profile groups with NaN so all lists can be exploded together.
        row_n = []
        for idx in df.index:
            expected_lengths = {
                int(length_df.at[idx, col])
                for col in packed_cols
                if pd.notna(length_df.at[idx, col])
            }

            for count_col, group_columns in self.PACKED_PROFILE_GROUPS.items():
                if not any(col in packed_cols for col in group_columns):
                    continue
                if count_col not in df.columns:
                    continue
                expected = self._expected_packed_count(df.at[idx, count_col])
                if expected is not None:
                    expected_lengths.add(expected)

            row_n.append(max(expected_lengths) if expected_lengths else None)

        row_n_series = pd.Series(row_n, index=df.index, dtype="object")

        for col in packed_cols:

            def pad_or_keep(value, n):
                if n is None:
                    return value
                if isinstance(value, list):
                    if len(value) >= int(n):
                        return value[: int(n)]
                    return value + [float("nan")] * (int(n) - len(value))
                return [float("nan")] * int(n)

            df[col] = [
                pad_or_keep(df.at[idx, col], row_n_series.at[idx]) for idx in df.index
            ]

        # --- Step 4: explode the padded lists and reset the DataFrame index ---
        df = df.explode(packed_cols).reset_index(drop=True)

        # --- Step 5: convert exploded values back to numeric types ---
        for col in packed_cols:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass

        # Also coerce any remaining string columns the schema declares numeric
        # (e.g. single-level profiles stored as a lone string in some files).
        return self._coerce_numeric_string_columns(df)

    def _schema_numeric_type(self, col: str):
        """Return the pyarrow type for *col* if the schema declares it numeric.

        Returns ``None`` when the column is absent from the configured
        ``pyarrow_schema`` or is declared as a non-numeric type (e.g. string,
        timestamp), so callers can skip it.
        """
        try:
            field_type = self.pyarrow_schema.field(col).type
        except KeyError:
            return None
        if pa.types.is_integer(field_type) or pa.types.is_floating(field_type):
            return field_type
        return None

    def _coerce_numeric_string_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce string columns the schema declares numeric into numbers.

        Some deployments store a single-level profile as a lone string (e.g.
        ``fluoro_dbar = "12.5"``) rather than a comma-packed list, so
        :meth:`_detect_packed_string_columns` does not explode it and pandas
        keeps the column as text.  Left untouched it lands in the parquet
        file as a string while the same column is ``double`` in other files,
        producing the ``double ↔ large_string`` cross-file conflict.

        Conversion is applied only to columns the dataset schema declares as an
        integer or floating type, and only when every non-null value parses
        cleanly — if a real value would be lost (e.g. an un-exploded packed
        string), the column is left as-is and a warning is logged so the
        per-column-tolerant schema cast can deal with it downstream.
        """
        for col in df.columns:
            if not self._holds_strings(df[col]):
                continue
            if self._schema_numeric_type(col) is None:
                continue
            coerced = pd.to_numeric(df[col], errors="coerce")
            lost = coerced.isna() & df[col].notna()
            if lost.any():
                self.logger.warning(
                    f"{self.uuid_log}: Column '{col}' is declared numeric but holds "
                    f"{int(lost.sum())} non-numeric value(s); leaving as-is."
                )
                continue
            df[col] = coerced

        return df
