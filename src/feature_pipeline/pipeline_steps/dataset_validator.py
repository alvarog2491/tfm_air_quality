import logging
import pandas as pd
from pathlib import Path
from typing import Dict
from common.utils import file_utils

class DatasetValidator:
    """
    DatasetValidator

    Provides static methods to validate a pandas DataFrame for:
    - Non-empty content
    - Absence of null values
    - Correct data types
    """

    logger = logging.getLogger("DatasetValidator")

    @staticmethod
    def validate_all(df: pd.DataFrame) -> None:
        """
        Run all dataset validations.

        Args:
            df (pd.DataFrame): The dataset to validate.

        Raises:
            ValueError: If any validation fails.
        """
        if df.empty:
            raise ValueError("Dataset is empty")

        DatasetValidator.validate_null_values(df)
        DatasetValidator.validate_data_types(df)

        DatasetValidator.logger.info("Dataset validation passed.")

    @staticmethod
    def validate_null_values(df: pd.DataFrame) -> None:
        """
        Check if the DataFrame contains null values.

        Raises:
            ValueError: If null values are found.
        """
        if df.isnull().values.any():
            raise ValueError("Dataset contains null values.")
        DatasetValidator.logger.info("No null values found.")

    @staticmethod
    def validate_data_types(df: pd.DataFrame) -> None:
        """
        Check if DataFrame columns have expected data types.

        Args:
            df (pd.DataFrame): The dataset.

        Raises:
            ValueError: If any column has an unexpected data type.
        """
        # Load data types from JSON config
        json_path = Path(__file__).parent.parent / 'config' / 'feature_types.json'
        expected_dtypes = file_utils.load_json_file(json_path)

        for column, expected_dtype in expected_dtypes.items():
            if column in df.columns:
                actual_dtype = str(df[column].dtype)
                if actual_dtype != expected_dtype:
                    raise ValueError(
                        f"Column '{column}' has dtype '{actual_dtype}' — expected '{expected_dtype}'"
                    )
        DatasetValidator.logger.info("All columns have correct data types.")
