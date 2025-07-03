from pathlib import Path
from typing import Optional, Dict
import logging

import pandas as pd

class DataMerger:
    """Handles loading and merging of Dataframes."""

    def __init__(self, data_folder: Optional[Path] = None):
        """Initialize DataMerger with optional data folder path.
        
        Args:
            data_folder: Path to the folder containing processed data files
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        if data_folder is None:
            script_dir = Path(__file__).resolve().parent.parent
            self.data_folder = (script_dir / "data").resolve()
        else:
            self.data_folder = data_folder

        self._air_quality_df: Optional[pd.DataFrame] = None
        self._health_df: Optional[pd.DataFrame] = None
        self._socioeconomic_df: Optional[pd.DataFrame] = None
    
    @property
    def air_quality_df(self) -> Optional[pd.DataFrame]:
        """Get the air quality DataFrame."""
        return self._air_quality_df

    @property
    def health_df(self) -> Optional[pd.DataFrame]:
        """Get the health DataFrame."""
        return self._health_df
    
    @property
    def socioeconomic_df(self) -> Optional[pd.DataFrame]:
        """Get the socioeconomic DataFrame."""
        return self._socioeconomic_df
    
    @property
    def is_loaded(self) -> bool:
        """Check if all required data has been loaded successfully."""
        return (self._air_quality_df is not None and not self._air_quality_df.empty and
                self._health_df is not None and not self._health_df.empty and
                self._socioeconomic_df is not None and not self._socioeconomic_df.empty)
    
    def load_dataframes(self) -> None:
        """Load all CSV files from the processed data folder.
        
        Raises:
            ValueError: If data_folder is not set
            FileNotFoundError: If any required CSV file is missing
            pd.errors.EmptyDataError: If any CSV file is empty
            Exception: For other file reading errors
        """
        if self.data_folder is None:
            raise ValueError("Data folder path must be set before loading dataframes")
        
        self.logger.info(f"Loading processed data from: {self.data_folder}")
        
        # Define file paths
        air_quality_file_path = self.data_folder / "processed" / "air_quality.csv"
        health_file_path = self.data_folder / "processed" / "health.csv"
        socioeconomic_file_path = self.data_folder / "processed" / "socioeconomic.csv"

        # Check if all files exist
        for file_path in [air_quality_file_path, health_file_path, socioeconomic_file_path]:
            if not file_path.is_file():
                raise FileNotFoundError(f"Required file not found: {file_path}")
        
        try:
            # Load each CSV file with error handling
            self._air_quality_df = pd.read_csv(air_quality_file_path)
            if self._air_quality_df.empty:
                raise pd.errors.EmptyDataError(f"Air quality file is empty: {air_quality_file_path}")
            
            self._health_df = pd.read_csv(health_file_path)
            if self._health_df.empty:
                raise pd.errors.EmptyDataError(f"Health file is empty: {health_file_path}")
            
            self._socioeconomic_df = pd.read_csv(socioeconomic_file_path)
            if self._socioeconomic_df.empty:
                raise pd.errors.EmptyDataError(f"Socioeconomic file is empty: {socioeconomic_file_path}")
            
            self.logger.info("All dataframes loaded successfully")
            
        except pd.errors.EmptyDataError as e:
            self.logger.error(f"Empty data error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading dataframes: {e}")
            raise
    
    def merge_all_data(self, airquality_df: pd.DataFrame, health_df: pd.DataFrame, 
                      socioeconomic_df: pd.DataFrame) -> pd.DataFrame:
        """Merge air quality, health, and socioeconomic dataframes.
        
        Args:
            airquality_df: Air quality DataFrame with Province and Year columns
            health_df: Health DataFrame with Province and Periodo columns
            socioeconomic_df: Socioeconomic DataFrame with Province and anio columns
            
        Returns:
            Merged DataFrame with all data combined
            
        Raises:
            ValueError: If required columns are missing from input DataFrames
            Exception: For merge operation errors
        """
        try:
            # Validate required columns exist
            self._validate_merge_columns(airquality_df, health_df, socioeconomic_df)
            
            self.logger.info("Starting data merge process")
            
            # Merge air quality with health data
            merged_df = pd.merge(
                airquality_df,
                health_df,
                left_on=['Province', 'Year'],
                right_on=['Province', 'Periodo'],
                how='left'
            )
            
            # Merge with socioeconomic data
            merged_df = pd.merge(
                merged_df,
                socioeconomic_df,
                left_on=['Province', 'Year'],
                right_on=['Province', 'anio'],
                how='left'
            )
            
            # Clean up duplicate columns
            merged_df.drop(columns=['Periodo', 'anio'], inplace=True, errors='ignore')
            return merged_df
            
        except Exception as e:
            self.logger.error(f"Error during merge operation: {e}")
            raise
    
    def _validate_merge_columns(self, airquality_df: pd.DataFrame, health_df: pd.DataFrame, 
                               socioeconomic_df: pd.DataFrame) -> None:
        """Validate that all required columns exist for merging.
        
        Args:
            airquality_df: Air quality DataFrame
            health_df: Health DataFrame
            socioeconomic_df: Socioeconomic DataFrame
            
        Raises:
            ValueError: If any required column is missing
        """
        # Check air quality columns
        required_aq_cols = ['Province', 'Year']
        missing_aq_cols = [col for col in required_aq_cols if col not in airquality_df.columns]
        if missing_aq_cols:
            raise ValueError(f"Air quality DataFrame missing columns: {missing_aq_cols}")
        
        # Check health columns
        required_health_cols = ['Province', 'Periodo']
        missing_health_cols = [col for col in required_health_cols if col not in health_df.columns]
        if missing_health_cols:
            raise ValueError(f"Health DataFrame missing columns: {missing_health_cols}")
        
        # Check socioeconomic columns
        required_socio_cols = ['Province', 'anio']
        missing_socio_cols = [col for col in required_socio_cols if col not in socioeconomic_df.columns]
        if missing_socio_cols:
            raise ValueError(f"Socioeconomic DataFrame missing columns: {missing_socio_cols}")
    
    def load_and_merge(self) -> pd.DataFrame:
        """Convenience method to load all data and merge it in one step.
        
        Returns:
            Merged DataFrame with all data
            
        Raises:
            RuntimeError: If data loading fails or DataFrames are not loaded
        """
        try:
            if not self.is_loaded:
                self.load_dataframes()
            
            if not self.is_loaded:
                raise RuntimeError("Failed to load all required dataframes")
            
            return self.merge_all_data(self._air_quality_df, self._health_df, self._socioeconomic_df)
            
        except Exception as e:
            self.logger.error(f"Error in load_and_merge: {e}")
            raise