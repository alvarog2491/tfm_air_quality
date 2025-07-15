"""
Health Data Processor - Respiratory Diseases and Life Expectancy by Spanish Provinces

Processes and merges Spanish provincial health data from two separate CSV sources.

INPUT: 
- "enfermedades_respiratorias.csv": 
| Causa de muerte                                 | Sexo  | Provincias  | Periodo    | Total |
|-------------------------------------------------|-------|-------------|------------|-------|
| 062-067 X.Enfermedades del sistema respiratorio | Total | 02 Albacete | 2023-01-01 | 397.0 |


- "esperanza_vida.csv": 
| Sexo       | Provincias  | Periodo    | Total |
|------------|-------------|------------|-------|
| Ambos sexos| 02 Albacete | 2023-01-01 | 83.61 |


OUTPUT: 
| Province | Periodo    | Respiratory_diseases_total | Life_expectancy_total |
|----------|------------|----------------------------|-----------------------|
| A Coruña | 1975-01-01 | 254                        | 72.69                 |


PROCESSING:
- Loads CSV files with Spanish locale settings (semicolon separator, latin1 encoding)
- Cleans province names by removing numeric codes
- Standardizes province names using ProvinceMapper
- Merges datasets on Province and Periodo with outer join
- Exports to "health.csv"

USAGE: processor = HealthProcessor(); processor.process()
"""

from processors.base_processor import BaseProcessor
from typing import Optional, Dict
from pathlib import Path

import pandas as pd
from utils.province_mapper import ProvinceMapper


class HealthProcessor(BaseProcessor):
    """
    Handles preprocessing of health-related CSV data, including loading, cleaning, and formatting steps.
    """

    # Define column data types as class constant for reusability
    _COLUMN_DTYPES: Dict[str, str] = {
        'Causa de muerte': 'category',
        'Sexo': 'category',
        'Provincias': 'category'
    }

    def __init__(self, data_folder: Optional[Path] = None):
        """
        Initialize the HealthProcessor.
        
        Args:
            data_folder: Optional custom path to data folder. If None, uses default relative path.
        """
        super().__init__(data_folder, "health_data")
        # Initialize DataFrames as private attributes
        self._health_df: Optional[pd.DataFrame] = None
        self._respiratory_diseases_df: Optional[pd.DataFrame] = None
        self._life_expectancy_df: Optional[pd.DataFrame] = None
    
    @property
    def health_df(self) -> Optional[pd.DataFrame]:
        """Get the merged health DataFrame."""
        return self._health_df
    
    @property
    def respiratory_diseases_df(self) -> Optional[pd.DataFrame]:
        """Get the respiratory diseases DataFrame."""
        return self._respiratory_diseases_df
    
    @property
    def life_expectancy_df(self) -> Optional[pd.DataFrame]:
        """Get the life expectancy DataFrame."""
        return self._life_expectancy_df
    
    @property
    def is_loaded(self) -> bool:
        """Check if data has been loaded."""
        return (self._respiratory_diseases_df is not None and 
                not self._respiratory_diseases_df.empty and
                self._life_expectancy_df is not None and 
                not self._life_expectancy_df.empty)
    
    def load_csv_files(self) -> None:
        """
        Load raw health data from CSV file
        
        Raises:
            FileNotFoundError: If any required CSV file is not found
            ValueError: If any loaded file is empty
        """
        self.logger.info(f"Loading raw health data from: {self.data_folder}")
        respiratory_file = self.data_folder / "raw" / "enfermedades_respiratorias.csv"
        life_expectancy_file = self.data_folder / "raw" / "esperanza_vida.csv"
        
        # Check if files exist before attempting to load
        if not respiratory_file.is_file():
            raise FileNotFoundError(f"Required file not found: {respiratory_file}")
        if not life_expectancy_file.is_file():
            raise FileNotFoundError(f"Required file not found: {life_expectancy_file}")
        
        try:
            # Load CSV files with appropriate settings for Spanish data
            self._respiratory_diseases_df = pd.read_csv(
                respiratory_file,
                dtype=self._COLUMN_DTYPES,
                parse_dates=['Periodo'],
                sep=';', 
                encoding='latin1'
            )
            self._life_expectancy_df = pd.read_csv(
                life_expectancy_file,
                dtype=self._COLUMN_DTYPES,
                parse_dates=['Periodo'],
                sep=';', 
                decimal=',', 
                encoding='latin1'
            )

            # Validate loaded data
            self._validate_dataframe_not_empty(self._respiratory_diseases_df, respiratory_file)
            self._validate_dataframe_not_empty(self._life_expectancy_df, life_expectancy_file)
            
            # Log data info
            self._log_dataframe_info(self._respiratory_diseases_df, "respiratory diseases")
            self._log_dataframe_info(self._life_expectancy_df, "life expectancy")
            
            # Log data quality info
            self._log_null_values(self._respiratory_diseases_df, "respiratory_diseases")
            self._log_null_values(self._life_expectancy_df, "life_expectancy")
            
        except Exception as e:
            self.logger.error(f"Error loading CSV files: {str(e)}")
            raise

    def clean_dataframes(self) -> None:
        """
        Clean and standardize the loaded DataFrames.
        
        Raises:
            ValueError: If data hasn't been loaded yet
        """
        if not self.is_loaded:
            raise ValueError("DataFrames must be loaded before cleaning")

        # Clean province names by removing numeric codes and extra spaces
        for df_name, df in [("respiratory_diseases", self._respiratory_diseases_df), 
                           ("life_expectancy", self._life_expectancy_df)]:
            if 'Provincias' in df.columns:
                df['Provincias'] = df['Provincias'].str.replace(r'[0-9\s]+', '', regex=True)
                df.rename(columns={'Provincias': 'Province'}, inplace=True)
                self.logger.info(f"Removed numeric codes on province names in {df_name} dataset")

    def merge_dataframes(self) -> None:
        """
        Merge respiratory diseases and life expectancy data into a single DataFrame.
        
        Raises:
            ValueError: If data hasn't been loaded and cleaned yet
        """
        if not self.is_loaded:
            raise ValueError("DataFrames must be loaded and cleaned before merging")
        
        # Rename 'Total' columns to be more descriptive
        respiratory_df = self._respiratory_diseases_df.rename(
            columns={'Total': 'Respiratory_diseases_total'}
        )
        life_expectancy_df = self._life_expectancy_df.rename(
            columns={'Total': 'Life_expectancy_total'}
        )
        
        # Merge the DataFrames on Province and Period
        try:
            self._health_df = pd.merge(
                respiratory_df,
                life_expectancy_df,
                on=['Province', 'Periodo'],
                how='outer',  # Use outer join to keep all data
                suffixes=('_respiratory', '_life_exp')
            )

            # Drop unnecessary columns
            self._health_df = self._health_df.drop(['Sexo_respiratory', 'Sexo_life_exp', 'Causa de muerte'], axis=1)
            
            # Log merge results
            self._log_dataframe_info(self._health_df, "merged health data")
            self._log_null_values(self._health_df, "merged health data")
                
        except Exception as e:
            self.logger.error(f"Error merging DataFrames: {str(e)}")
            raise

    def map_province_names(self) -> None:
        """
        Unify province name among all dataframes
        """
        self._respiratory_diseases_df = ProvinceMapper.map_province_name("respiratory_diseases", self._respiratory_diseases_df)
        self._life_expectancy_df = ProvinceMapper.map_province_name("life_expectancy", self._life_expectancy_df)       

    def save_processed_file(self) -> None:
        """
        Export the processed health DataFrame to a CSV file.
        
        Raises:
            ValueError: If no data is available to save
        """
        self._save_dataframe_to_csv(self._health_df, "health.csv")

    def process(self) -> None:
        """
        Execute the complete health data processing pipeline.
        """
        try:
            self.load_csv_files()
            self.clean_dataframes()
            self.map_province_names()
            self.merge_dataframes()
            self.save_processed_file()
            
        except Exception as e:
            self.logger.error(f"Error in processing pipeline: {str(e)}")
            raise