from pathlib import Path
import logging
from typing import Optional, Dict
import pandas as pd
from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    """
    Abstract base class for data extractors.
    Provides shared logic for managing file paths and logging DataFrame information.
    """

    def __init__(self, name: str, data_path: Path):
        """
        Initialize the extractor.
        """
        self.name = name
        self.data_path = data_path
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self, dataframes: Dict[str, pd.DataFrame], format: str = "") -> None:
        """
        Abstract method for extracting data into a dictionary of DataFrames.

        Args:
            dataframes: Dictionary to store the extracted DataFrame(s).
            format: File format to extract (e.g., "csv", "json").

        Raises:
            FileNotFoundError: If the required file is not found.
            ValueError: If the extracted DataFrame is empty.
        """
        pass

    def _log_dataframe_info(self, df: pd.DataFrame) -> None:
        """
        Log summary information about the provided DataFrame.

        Includes null values, duplicated rows, empty rows, and overall memory usage.

        Args:
            df: DataFrame to log.
        """
        self._log_null_values(df)
        self._log_duplicated_rows(df)
        self._log_empty_rows(df)
        self._log_info(df)

        rows, cols = df.shape
        memory_usage = df.memory_usage(deep=True).sum() / 1024**2  # MB
        self.logger.info(
            f"Successfully loaded: {rows:,} rows and {cols} columns "
            f"(~{memory_usage:.1f} MB memory usage)"
        )

    def _log_null_values(self, df: pd.DataFrame) -> None:
        """
        Log the count of null values per column in the DataFrame, if any.

        Args:
            df: DataFrame to check for null values.
        """
        null_counts = df.isnull().sum()
        if null_counts.any():
            self.logger.warning(
                f"Null values detected in columns: {null_counts[null_counts > 0].to_dict()}"
            )

    def _log_duplicated_rows(self, df: pd.DataFrame) -> None:
        """
        Log the number of duplicated rows in the DataFrame.

        Args:
            df: DataFrame to check for duplicates.
        """
        duplicated_count = df.duplicated().sum()
        if duplicated_count:
            self.logger.warning(f"Duplicated rows found: {duplicated_count}")

    def _log_info(self, df: pd.DataFrame) -> None:
        """
        Log the DataFrame's structure and metadata using `df.info()`.

        Args:
            df: DataFrame to describe.
        """
        self.logger.info("DataFrame information:")
        self.logger.info(df.info())

    def _log_empty_rows(self, df: pd.DataFrame) -> None:
        """
        Log the number of rows with more than 70% missing values.

        Args:
            df: DataFrame to evaluate.
        """
        empty_rows = df.isnull().mean(axis=1) > 0.7
        if empty_rows.any():
            self.logger.warning(
                "%d rows contain more than 70%% missing values.", empty_rows.sum()
            )
