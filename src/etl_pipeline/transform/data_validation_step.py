import pandas as pd
from etl_pipeline import ETLStep
from pathlib import Path
from common.utils.file_utils import load_json_file
from typing import Dict, Any


class DataValidationStep(ETLStep):
    """Validate the final dataset."""

    def __init__(self):
        super().__init__(__name__)

    def execute(
        self, dataframes: Dict[str, pd.DataFrame], context: Dict[str, Any]
    ) -> None:
        """
        Run all validations on 'output_df'.

        Raises:
            ValueError: If 'output_df' missing or validations fail.
        """
        self.log_start()
        if "output_df" not in dataframes:
            raise ValueError(
                "'output_df' is missing. Run feature engineering before validation."
            )

        df = dataframes["output_df"]

        self._validate_nulls(df)
        self._validate_dtypes(df)
        self._validate_duplicates(df)

        self.log_success("Dataset validation passed")

    def _validate_nulls(self, df: pd.DataFrame) -> None:
        """
        Raise if null values found.
        """
        if df.isnull().values.any():
            raise ValueError("Dataset contains null values.")
        self.logger.info("No null values found")

    def _validate_dtypes(self, df: pd.DataFrame) -> None:
        """
        Check columns have expected dtypes.
        """
        config_path = Path(__file__).parent.parent / "config" / "feature_types.json"
        expected_dtypes = load_json_file(config_path)

        for col, expected_dtype in expected_dtypes.items():
            if col in df.columns:
                actual_dtype = str(df[col].dtype)
                if actual_dtype != expected_dtype:
                    raise ValueError(
                        f"Column '{col}' has dtype '{actual_dtype}' instead of '{expected_dtype}'"
                    )
        self.logger.info("All columns have correct data types")

    def _validate_duplicates(self, df: pd.DataFrame) -> None:
        """
        Raise if duplicated rows found.
        """
        if df.duplicated().any():
            raise ValueError("Dataset contains duplicated rows.")
        self.logger.info("No duplicated rows found")
