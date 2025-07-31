from pathlib import Path
from typing import Any, Dict

import pandas as pd

from common.utils.file_utils import load_json_file
from etl_pipeline import ETLStep


class DataCleaningStep(ETLStep):
    """
    Clean and validate the dataset by applying several in-place preprocessing steps.
    """

    def __init__(self):
        super().__init__(__name__)
        
        # Try to load configuration, fall back to defaults if not available
        try:
            from config.config_manager import get_config
            self.config = get_config()
            self.processing_config = self.config.get_processing_config()
        except ImportError:
            self.logger.warning("Configuration manager not available, using hardcoded values")
            self.config = None
            self.processing_config = {}

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Execute the cleaning pipeline on 'output_df', including island removal, timeframe filtering,
        null handling, duplicate removal, and dtype casting.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary containing input dataframes.
            context (Dict[str, Any]): Additional metadata or configuration (unused here).

        Raises:
            ValueError: If 'output_df' is missing from the dataframes dictionary.
        """
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
        """
        Remove rows corresponding to excluded regions from configuration.

        Args:
            df (pd.DataFrame): DataFrame to filter.
        """
        excluded_regions = self.processing_config.get("excluded_regions", [])
        
        if not excluded_regions:
            self.logger.warning("No excluded regions configured, skipping region filtering")
            return
        
        before = len(df)
        df.drop(df[df["Province"].isin(excluded_regions)].index, inplace=True)  # type: ignore
        removed = before - len(df)
        self.logger.info(f"Removed {removed} records from excluded regions: {excluded_regions}")

    def _filter_timeframe(self, df: pd.DataFrame) -> None:
        """
        Filter rows to keep only those within the configured time range.

        Args:
            df (pd.DataFrame): DataFrame to filter.
        """
        time_range = self.processing_config.get("time_range", {})
        start_year = time_range.get("start_year")
        end_year = time_range.get("end_year")
        
        if not start_year or not end_year:
            self.logger.warning("No time range configured, skipping timeframe filtering")
            return
        
        before = len(df)
        if pd.api.types.is_datetime64_any_dtype(df["Year"]):
            mask = df["Year"].dt.year.between(start_year, end_year)  # type: ignore
        else:
            mask = df["Year"].between(start_year, end_year)  # type: ignore
        df.drop(index=df[~mask].index, inplace=True)
        removed = before - len(df)
        self.logger.info(f"Removed {removed} records outside {start_year}–{end_year} timeframe")

    def _handle_null_values(self, df: pd.DataFrame) -> None:
        """
        Remove rows with nulls if the percentage is below 5%. Otherwise, keep and log a warning.

        Args:
            df (pd.DataFrame): DataFrame to process.
        """
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
        """
        Drop duplicated rows from the DataFrame, if any.

        Args:
            df (pd.DataFrame): DataFrame to deduplicate.
        """
        count = df.duplicated().sum()
        if count == 0:
            self.logger.info("No duplicate rows found.")
        else:
            self.logger.info(f"Removing {count} duplicate rows.")
            df.drop_duplicates(inplace=True)

    def _convert_to_appropriate_dtypes(self, df: pd.DataFrame) -> None:
        """
        Cast columns to data types defined in an external JSON configuration.

        Args:
            df (pd.DataFrame): DataFrame to cast.
        """
        config_path = Path(__file__).parent.parent / "config" / "feature_types.json"
        dtypes = load_json_file(config_path)
        for col, dtype in dtypes.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
