import logging
from pathlib import Path
from typing import Optional
import pandas as pd


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

        self._remove_null_provinces()
        self._remove_island_observations()
        self._remove_undefined_provinces()
        self._filter_timeframe()

        self.logger.info("Dataset cleaning process completed")
        return self._dataset

    def _remove_null_provinces(self):
        """
        Removes rows with null values in the 'province' column if they represent less than 5% of the dataset.
        """
        null_province_percentage = self._dataset['Province'].isnull().mean() * 100
        print(null_province_percentage)
        if null_province_percentage == 0:
            self.logger.info("Not found any null value on Province")
        elif null_province_percentage < 5:
            self.logger.info(f"Removing null provinces (found {null_province_percentage:.2f}% of dataset)")
            self._dataset = self._dataset.dropna(subset=['Province'])
        else:
            self.logger.warning(f"Null provinces exceed 5% ({null_province_percentage:.2f}%), not removing them.")

    def _remove_island_observations(self):
        """
        Removes all observations from island provinces.
        """
        island_provinces = ['Santa Cruz de Tenerife', 'Las Palmas', 'Illes Balears', 'Ceuta', 'Melilla']
        initial_count = len(self._dataset)
        self._dataset = self._dataset[~self._dataset['Province'].isin(island_provinces)]
        removed_count = initial_count - len(self._dataset)
        self.logger.info(f"Removed {removed_count} island observations")

    def _remove_undefined_provinces(self):
        """
        Removes all observations with undefined or erroneous provinces.
        """
        undefined_provinces = ['Desconocido', 'Error']
        initial_count = len(self._dataset)
        self._dataset = self._dataset[~self._dataset['Province'].isin(undefined_provinces)]
        removed_count = initial_count - len(self._dataset)
        self.logger.info(f"Removed {removed_count} undefined province observations")

    def _filter_timeframe(self):
        """
        Keeps only observations from the years 2000 to 2022 (inclusive).
        """
        initial_count = len(self._dataset)

        # Ensure comparison works for both datetime and integer types
        if pd.api.types.is_datetime64_any_dtype(self._dataset['Year']):
            mask = self._dataset['Year'].dt.year.between(2000, 2022)
        else:
            mask = self._dataset['Year'].between(2000, 2022)

        self._dataset = self._dataset[mask]
        removed_count = initial_count - len(self._dataset)
        self.logger.info(f"Removed {removed_count} observations outside the 2000-2022 timeframe")

