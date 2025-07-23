from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from .base_extractor import BaseExtractor


class SocioeconomicDataExtractor(BaseExtractor):
    """
    Extractor for socioeconomic data: GDP per capita and provincial population.

    Loads and returns raw data from two CSV sources.
    """

    def __init__(self, data_path: Path):
        """
        Initialize the extractor with the path to the data directory.
        """
        super().__init__(__name__, data_path=data_path)

    def extract(self, dataframes: Dict[str, pd.DataFrame], format: str = "csv") -> None:
        """
        Extract socioeconomic datasets and store them in the dataframes dictionary.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary to store extracted DataFrames.
            format (str): File format to read from (default is 'csv').
        """
        if format == "csv":
            gdp_df, population_df = self._read_csv_files()
            dataframes["gdp"] = gdp_df
            dataframes["province_population"] = population_df

    def _read_csv_files(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Read raw GDP and population data from CSV files.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: DataFrames for GDP per capita and provincial population.

        Raises:
            FileNotFoundError: If any required file is missing.
            Exception: If any file fails to load.
        """
        self.logger.info(f"Loading raw socioeconomic data from: {self.data_path}")

        pib_file = (
            self.data_path
            / "socioeconomic_data"
            / "raw"
            / "PIB per cap provincias 2000-2021.csv"
        )
        population_file = (
            self.data_path / "socioeconomic_data" / "raw" / "poblacion_provincias.csv"
        )

        if not pib_file.is_file():
            raise FileNotFoundError(f"Required file not found: {pib_file}")
        if not population_file.is_file():
            raise FileNotFoundError(f"Required file not found: {population_file}")

        try:
            gdp_df = pd.read_csv(pib_file, sep=";", decimal=",", encoding="ISO-8859-1")  # type: ignore

            population_df = pd.read_csv(  # type: ignore
                population_file,
                parse_dates=["Periodo"],
                sep=";",
                decimal=",",
                encoding="latin1",
            )

            self._log_dataframe_info(gdp_df)
            self._log_dataframe_info(population_df)

            return gdp_df, population_df

        except Exception as e:
            self.logger.error(f"Error loading CSV file: {str(e)}")
            raise
