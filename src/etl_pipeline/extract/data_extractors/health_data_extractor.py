from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from .base_extractor import BaseExtractor


class HealthDataExtractor(BaseExtractor):
    """
    Extractor for health-related data: respiratory diseases and life expectancy.

    Loads and returns two separate DataFrames from raw CSV files.
    """

    _COLUMN_DTYPES: Dict[str, str] = {
        "Causa de muerte": "category",
        "Sexo": "category",
        "Provincias": "category",
        "Total": "float64",
    }

    def __init__(self, data_path: Path):
        """
        Initialize the extractor with the path to the data directory.
        """
        super().__init__(__name__, data_path=data_path)

    def extract(self, dataframes: Dict[str, pd.DataFrame], format: str = "csv") -> None:
        """
        Extract health datasets and store them in the dataframes dictionary.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary to store extracted DataFrames.
            format (str): File format to read from (default is 'csv').
        """
        if format == "csv":
            respiratory_df, life_expectancy_df = self._read_csv_files()
            dataframes["respiratory_diseases"] = respiratory_df
            dataframes["life_expectancy"] = life_expectancy_df

    def _read_csv_files(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Read raw health data from CSV files.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: DataFrames for respiratory diseases and life expectancy.

        Raises:
            FileNotFoundError: If any required file is missing.
            Exception: If any file fails to load.
        """
        self.logger.info(f"Loading raw health data from: {self.data_path}")

        respiratory_file = (
            self.data_path / "health_data" / "raw" / "enfermedades_respiratorias.csv"
        )
        life_expectancy_file = (
            self.data_path / "health_data" / "raw" / "esperanza_vida.csv"
        )

        if not respiratory_file.is_file():
            raise FileNotFoundError(f"Required file not found: {respiratory_file}")
        if not life_expectancy_file.is_file():
            raise FileNotFoundError(f"Required file not found: {life_expectancy_file}")

        try:
            respiratory_df = pd.read_csv(  # type: ignore
                respiratory_file,
                parse_dates=["Periodo"],
                decimal=",",
                sep=";",
            )

            life_expectancy_df = pd.read_csv(  # type: ignore
                life_expectancy_file,
                parse_dates=["Periodo"],
                decimal=",",
                sep=";",
                encoding="latin1",
            )

            self._log_dataframe_info(respiratory_df)
            self._log_dataframe_info(life_expectancy_df)

            return respiratory_df, life_expectancy_df

        except Exception as e:
            self.logger.error(f"Error loading CSV files: {str(e)}")
            raise
