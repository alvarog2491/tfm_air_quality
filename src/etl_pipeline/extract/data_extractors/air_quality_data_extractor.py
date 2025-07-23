from pathlib import Path
from typing import Dict

import pandas as pd

from .base_extractor import BaseExtractor


class AirQualityDataExtractor(BaseExtractor):
    """
    Extractor for air quality data from CSV files.

    Loads predefined columns from a raw air quality dataset.
    """

    _COLS_TO_USE: list[str] = [
        "Air Pollutant",
        "Air Pollutant Description",
        "Data Aggregation Process",
        "Year",
        "Air Pollution Level",
        "Unit Of Air Pollution Level",
        "Air Quality Station Type",
        "Air Quality Station Area",
        "Altitude",
        "Longitude",
        "Latitude",
        "Province",
    ]

    def __init__(self, data_path: Path):
        """
        Initialize the extractor with the path to the data directory.
        """
        super().__init__(__name__, data_path=data_path)

    def extract(self, dataframes: Dict[str, pd.DataFrame], format: str = "csv") -> None:
        """
        Extract air quality data and store it in the dataframes dictionary.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary to store the extracted DataFrame.
            format (str): File format to read from (default is 'csv').
        """
        if format == "csv":
            dataframes["air_quality"] = self._read_csv_files()

    def _read_csv_files(self) -> pd.DataFrame:
        """
        Read the air quality CSV file and return the loaded DataFrame.

        Returns:
            pd.DataFrame: Loaded air quality data.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
            ValueError: If the CSV file is empty or cannot be read.
        """
        self.logger.info(f"Loading raw air quality data from: {self.data_path}")
        file_path = (
            self.data_path
            / "air_quality_data"
            / "raw"
            / "air_quality_with_province.csv"
        )

        if not file_path.is_file():
            raise FileNotFoundError(f"Required file not found: {file_path}")

        try:
            df: pd.DataFrame = pd.read_csv(file_path, usecols=self._COLS_TO_USE, parse_dates=["Year"])  # type: ignore
            self._log_dataframe_info(df)
            return df
        except Exception as e:
            self.logger.error(f"Error loading CSV file: {str(e)}")
            raise
