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
    repeated.  Packed-string columns are detected automatically — any
    ``object``-dtype column whose non-null values parse as comma-separated
    floats is treated as packed.

    Validation: if any row has two or more non-null packed columns whose value
    counts differ, the **entire file is rejected** with a logger error.  This is
    consistent with how Data Provider Bugs are treated elsewhere in the pipeline.

    The handler works for all seven AATAMS CSV types (ctd, dive, haulout,
    metadata, diag, summary, ssmoutputs).  For flat CSV types that contain no
    packed-string columns, auto-detection finds nothing to expand and the file
    passes through unchanged.

    Note: after running ``cloud_optimised_create_dataset_config`` on a packed CSV,
    the generated schema will show ``string`` for columns that are actually
    numeric after expansion.  These types **must be manually corrected** in the
    JSON config before processing.
    """

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

    def _detect_packed_string_columns(self, df: pd.DataFrame) -> List[str]:
        """Return names of columns that contain comma-separated numeric values.

        A column is classified as packed if:
        - its dtype is ``object``
        - at least one non-null value contains a comma
        - the first comma-containing value splits into float-parseable parts
        """
        packed = []
        for col in df.columns:
            if df[col].dtype != object:
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

    def _expand_packed_string_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect and expand packed-string columns in *df*.

        Steps:
        1. Detect packed-string columns.
        2. Parse string cells to lists of floats.
        3. Validate per-row consistency (all non-null packed columns must have
           the same length).
        4. Fill NaN packed cells with ``[NaN] * n`` so ``pd.DataFrame.explode``
           works correctly for rows where only some parameters are present.
        5. Explode and reset the index.
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

        # --- Step 2: parse strings → lists of floats ---
        for col in packed_cols:
            df[col] = df[col].apply(self._parse_packed_value)

        # --- Step 3: per-row length validation ---
        length_df = pd.DataFrame(
            {
                col: df[col].apply(lambda v: len(v) if isinstance(v, list) else None)
                for col in packed_cols
            },
            index=df.index,
        )

        def _unique_non_null_lengths(row: pd.Series):
            non_null = row.dropna()
            return set(non_null.astype(int)) if not non_null.empty else set()

        unique_per_row = length_df.apply(_unique_non_null_lengths, axis=1)
        bad_mask = unique_per_row.apply(lambda s: len(s) > 1)

        if bad_mask.any():
            bad_info = length_df[bad_mask].to_dict(orient="index")
            msg = (
                f"{self.uuid_log}: Packed-string column length mismatch detected in "
                f"{bad_mask.sum()} row(s). File rejected. Row details: {bad_info}"
            )
            self.logger.error(msg)
            raise ValueError(msg)

        # --- Step 4: fill NaN packed cells with [NaN] * n ---
        row_n = length_df.max(axis=1)  # NaN if all packed cols are NaN for that row

        for col in packed_cols:
            null_mask = df[col].apply(lambda v: not isinstance(v, list))
            needs_fill = null_mask & row_n.notna()
            if needs_fill.any():
                df.loc[needs_fill, col] = row_n[needs_fill].apply(
                    lambda n: [float("nan")] * int(n)
                )

        # --- Step 5: explode and reset index ---
        df = df.explode(packed_cols).reset_index(drop=True)

        # Convert float columns that look integer-valued back to numeric
        # (explode leaves list items as Python objects; ensure proper dtype)
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
        """Coerce ``object`` columns the schema declares numeric into numbers.

        Some deployments store a single-level profile as a lone string (e.g.
        ``fluoro_dbar = "12.5"``) rather than a comma-packed list, so
        :meth:`_detect_packed_string_columns` does not explode it and pandas
        keeps the column as ``object``.  Left untouched it lands in the parquet
        file as a string while the same column is ``double`` in other files,
        producing the ``double ↔ large_string`` cross-file conflict.

        Conversion is applied only to columns the dataset schema declares as an
        integer or floating type, and only when every non-null value parses
        cleanly — if a real value would be lost (e.g. an un-exploded packed
        string), the column is left as-is and a warning is logged so the
        per-column-tolerant schema cast can deal with it downstream.
        """
        for col in df.columns:
            if df[col].dtype != object:
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
