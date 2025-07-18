import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from common.utils import file_utils


class DatasetCleaner:
    """
    A class to clean datasets by removing nulls, island observations,
    undefined provinces, and filtering by year.
    """

    def __init__(self, data_folder: Optional[Path] = None):
        """
        Initializes the DatasetCleaner.

        :param data_folder: Optional path to the folder containing processed data files.
        :type data_folder: Optional[Path]
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        if data_folder is None:
            script_dir = Path(__file__).resolve().parent.parent
            self.data_folder = (script_dir / "data").resolve()
        else:
            self.data_folder = data_folder

        self._dataset: Optional[pd.DataFrame] = None

    @property
    def dataset(self) -> Optional[pd.DataFrame]:
        """
        Returns the cleaned dataset.

        :return: The cleaned dataset if loaded, otherwise None.
        :rtype: Optional[pd.DataFrame]
        """
        return self._dataset

    @property
    def is_dataset_loaded(self) -> bool:
        """
        Checks if a dataset has been loaded.

        :return: True if the dataset is loaded, False otherwise.
        :rtype: bool
        """
        return self._dataset is not None

    def _handle_null_values(self):
        """
        For each column in the dataset:
        - If null percentage == 0% → log info.
        - If null percentage < 5% → remove rows with nulls in that column.
        - If null percentage >= 5% → log warning and keep them.
        """
        self._convert_invalid_province_to_nan()

        for column in self._dataset.columns:
            null_percentage = self._dataset[column].isnull().mean() * 100

            if null_percentage == 0:
                self.logger.info(f"No null values found in '{column}'")
            elif null_percentage < 0.05:
                self.logger.info(
                    f"Removing rows with nulls in '{column}' ({null_percentage:.2f}% of dataset)"
                )
                self._dataset = self._dataset.dropna(subset=[column])
            else:
                self.logger.warning(
                    f"Nulls in '{column}' exceed 5% ({null_percentage:.2f}%), keeping them."
                )

    def _convert_invalid_province_to_nan(self):
        """
        Replace invalid values in the 'Province' column (e.g., 'nan', 'Desconocido', 'Error') with NaN.
        """
        self._dataset.loc[
            self._dataset["Province"].isin(["nan", "Desconocido", "Error"]), "Province"
        ] = np.nan

    def _remove_island_observations(self):
        """
        Removes all observations from island provinces.
        """
        island_provinces = [
            "Santa Cruz de Tenerife",
            "Las Palmas",
            "Illes Balears",
            "Ceuta",
            "Melilla",
        ]
        initial_count = len(self._dataset)
        self._dataset = self._dataset[~self._dataset["Province"].isin(island_provinces)]
        removed_count = initial_count - len(self._dataset)
        self.logger.info(f"Removed {removed_count} island observations")

    def _filter_timeframe(self):
        """
        Keeps only observations from the years 2000 to 2021 (inclusive).
        """
        initial_count = len(self._dataset)

        # Ensure comparison works for both datetime and integer types
        if pd.api.types.is_datetime64_any_dtype(self._dataset["Year"]):
            mask = self._dataset["Year"].dt.year.between(2000, 2021)
        else:
            mask = self._dataset["Year"].between(2000, 2021)

        self._dataset = self._dataset[mask]
        removed_count = initial_count - len(self._dataset)
        self.logger.info(
            f"Removed {removed_count} observations outside the 2000-2021 timeframe"
        )

    def _convert_to_appropriate_dtypes(self):
        """
        Converts columns to appropriate data types.
        """
        json_path = Path(__file__).parent.parent / "config" / "feature_types.json"
        dtypes = file_utils.load_json_file(json_path)

        for column, dtype in dtypes.items():
            if column in self._dataset.columns:
                self._dataset[column] = self._dataset[column].astype(dtype)

    def clean_dataset(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """
        Main method to clean the dataset by applying all cleaning steps.

        :param dataset: The dataset to clean.
        :type dataset: pd.DataFrame
        :return: The cleaned dataset.
        :rtype: pd.DataFrame
        """
        self.logger.info("Starting dataset cleaning process")
        self._dataset = dataset.copy()

        # Clean operations
        self._remove_island_observations()
        self._filter_timeframe()
        self._handle_null_values()
        self._convert_to_appropriate_dtypes()

        self.logger.info("Dataset cleaning process completed")
        return self._dataset
