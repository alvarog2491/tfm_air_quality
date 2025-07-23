from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from common.utils.file_utils import load_json_file
from etl_pipeline import ETLStep


class DataCleaningStep(ETLStep):
    """Clean and validate the dataset with multiple in-place transformations."""

    def __init__(self):
        super().__init__(__name__)

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """Clean 'output_df' by removing islands, filtering years, handling nulls, duplicates, and casting types."""
        self.log_start()
        if "output_df" not in dataframes:
            raise ValueError(
                "'output_df' is missing. Run feature engineering before cleaning."
            )

        df = dataframes["output_df"]

        self._remove_island_observations(df)
        self._filter_timeframe(df)
        self._handle_null_values(df)
        self._handle_duplicated_rows(df)
        self._convert_to_appropriate_dtypes(df)

        self.log_success(f"Dataset cleaned: {len(df)} records")

    def _remove_island_observations(self, df: pd.DataFrame) -> None:
        """Remove rows for island provinces."""
        islands = [
            "Santa Cruz de Tenerife",
            "Las Palmas",
            "Illes Balears",
            "Ceuta",
            "Melilla",
        ]
        before = len(df)
        df.drop(df[df["Province"].isin(islands)].index, inplace=True)
        removed = before - len(df)
        self.logger.info(f"Removed {removed} island observations")

    def _filter_timeframe(self, df: pd.DataFrame) -> None:
        """Keep rows where 'Year' is between 2000 and 2021 inclusive."""
        before = len(df)
        if pd.api.types.is_datetime64_any_dtype(df["Year"]):
            mask = df["Year"].dt.year.between(2000, 2021)
        else:
            mask = df["Year"].between(2000, 2021)
        df.drop(index=df[~mask].index, inplace=True)
        removed = before - len(df)
        self.logger.info(f"Removed {removed} records outside 2000-2021 timeframe")

    def _handle_null_values(self, df: pd.DataFrame) -> None:
        """Remove rows with <5% nulls per column; log info or warning otherwise."""
        for col in df.columns:
            null_pct = df[col].isnull().mean() * 100
            if null_pct == 0:
                self.logger.info(f"No nulls in '{col}'")
            elif null_pct < 5:
                self.logger.info(
                    f"Removing rows with nulls in '{col}' ({null_pct:.2f}%)"
                )
                df.drop(index=df[df[col].isna()].index, inplace=True)
            else:
                self.logger.warning(
                    f"Nulls >5% in '{col}' ({null_pct:.2f}%), kept for imputation"
                )

    def _handle_duplicated_rows(self, df: pd.DataFrame) -> None:
        """Drop duplicate rows if any."""
        count = df.duplicated().sum()
        if count == 0:
            self.logger.info("No duplicate rows found.")
        else:
            self.logger.info(f"Removing {count} duplicate rows.")
            df.drop_duplicates(inplace=True)

    def _convert_to_appropriate_dtypes(self, df: pd.DataFrame) -> None:
        """Cast columns to types defined in external JSON config."""
        config_path = Path(__file__).parent.parent / "config" / "feature_types.json"
        dtypes = load_json_file(config_path)
        for col, dtype in dtypes.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
