from pathlib import Path
from typing import Optional, Dict
import logging

import pandas as pd

class DataMerger:
    """Handles loading and merging of Dataframes."""

    def __init__(self):
        """
        Initialize the DataMerger.
        """
        self.logger = logging.getLogger(__name__)
    

    def merge_all_data(self, airquality_df: pd.DataFrame, 
                                health_df: pd.DataFrame, 
                                gdp_df: pd.DataFrame,
                                province_population_df: pd.DataFrame) -> pd.DataFrame:
        """Merge air quality, health, and socioeconomic dataframes.
        
        Args:
            airquality_df: Air quality DataFrame with Province and Year columns
            health_df: Health DataFrame with Province and Periodo columns
            gdp_df: GDP DataFrame with Province and anio columns
            province_population_df: Province pop. size DataFrame
            
        Returns:
            Merged DataFrame with all data combined
            
        Raises:
            ValueError: If required columns are missing from input DataFrames
            Exception: For merge operation errors
        """
        try:
            # Validate required columns exist
            self._validate_merge_columns(airquality_df, health_df, gdp_df, province_population_df)
            
            self.logger.info("Starting data merge process")
            
            # Merge air quality with health data
            merged_df = pd.merge(
                airquality_df,
                health_df,
                left_on=['Province', 'Year'],
                right_on=['Province', 'Periodo'],
                how='left'
            )
            
            # Merge with gdp data
            merged_df = pd.merge(
                merged_df,
                gdp_df,
                left_on=['Province', 'Year'],
                right_on=['Province', 'anio'],
                how='left'
            )
            
            # Merge with province population data
            merged_df = pd.merge(
                merged_df,
                province_population_df,
                left_on=['Province', 'Year'],
                right_on=['Province', 'Periodo'],
                how='left'
            )

            # Clean up duplicate columns
            merged_df.drop(columns=['Periodo', 'anio', 'Sexo', 'Periodo_x', 'Periodo_y'], inplace=True, errors='ignore')
            return merged_df
            
        except Exception as e:
            self.logger.error(f"Error during merge operation: {e}")
            raise
    
    def _validate_merge_columns(self, airquality_df: pd.DataFrame, 
                                        health_df: pd.DataFrame, 
                                        gdp_df: pd.DataFrame,
                                        province_population_df: pd.DataFrame) -> None:
        """Validate that all required columns exist for merging.
        
        Args:
            airquality_df: Air quality DataFrame
            health_df: Health DataFrame
            gdp_df: GDP DataFrame
            province_population_df: Province population DataFrame
            
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
        
        # Check gdp columns
        required_gdp_cols = ['Province', 'anio']
        missing_gdp_cols = [col for col in required_gdp_cols if col not in gdp_df.columns]
        if missing_gdp_cols:
            raise ValueError(f"GDP DataFrame missing columns: {missing_gdp_cols}")
        
        # Check province population columns
        required_province_population_cols = ['Province', 'Periodo']
        missing_province_pop_cols = [col for col in required_province_population_cols if col not in province_population_df.columns]
        if missing_province_pop_cols:
            raise ValueError(f"Province population size DataFrame missing columns: {missing_province_pop_cols}")