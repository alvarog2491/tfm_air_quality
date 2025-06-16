
"""
Socioeconomic Data Processor - GDP per Capita by Spanish Provinces

Transforms Spanish provincial GDP per capita data from wide format CSV to long format.

INPUT: "PIB per cap provincias 2000-2021.csv" (wide format with years as columns)
  Provincia | 2000  | 2001  | 2002  | ... | 2021
  Alava     | 22134 | 23917 | 25679 | ... | 35924

OUTPUT: Normalized long format
  Province  | anio          | pib
  Alava     | 2000-01-01    | 22134.0

PROCESSING:
- Loads CSV with Spanish locale settings (semicolon separator, comma decimal)
- Melts wide format to long format using pandas.melt()
- Standardizes province names and converts data types
- Exports to "socioeconomic.csv"

USAGE: processor = SocioeconomicProcessor(); processor.process()
"""


from processors.BaseProcessor import BaseProcessor
from typing import Optional
from pathlib import Path

import pandas as pd
from utils.province_mapper import ProvinceMapper


class SocioeconomicProcessor(BaseProcessor):
    """
    Handles preprocessing of socioeconomic-related CSV data, including loading, cleaning, and formatting steps.
    """

    def __init__(self, data_folder: Optional[Path] = None):
        """
        Initialize the SocioeconomicProcessor.
        
        Args:
            data_folder: Optional custom path to data folder. If None, uses default relative path.
        """
        super().__init__(data_folder, "socioeconomic_data")
        # Initialize DataFrame as private attribute
        self._socioeconomic_df: Optional[pd.DataFrame] = None
    
    @property
    def socioeconomic_df(self) -> Optional[pd.DataFrame]:
        """Get the socioeconomic DataFrame."""
        return self._socioeconomic_df
    
    @property
    def is_loaded(self) -> bool:
        """Check if data has been loaded."""
        return (self._socioeconomic_df is not None and 
                not self._socioeconomic_df.empty)
    
    def load_csv_files(self) -> None:
        """
        Load raw socioeconomic data from CSV file
        
        Raises:
            FileNotFoundError: If the required CSV file is not found
            ValueError: If the loaded file is empty
        """
        self.logger.info(f"Loading raw socioeconomic data from: {self.data_folder}")
        socioeconomic_file = self.data_folder / "raw" / "PIB per cap provincias 2000-2021.csv"
        
        # Check if file exists before attempting to load
        if not socioeconomic_file.is_file():
            raise FileNotFoundError(f"Required file not found: {socioeconomic_file}")
        
        try:
            # Load CSV file with appropriate settings for Spanish data
            self._socioeconomic_df = pd.read_csv(
                socioeconomic_file,
                sep=';', 
                decimal=',',
                encoding='ISO-8859-1'
            )
            
            # Validate loaded data
            self._validate_dataframe_not_empty(self._socioeconomic_df, socioeconomic_file)
            
            # Log data info
            self._log_dataframe_info(self._socioeconomic_df, "socioeconomic")
            
            # Log data quality info
            self._log_null_values(self._socioeconomic_df, "socioeconomic")
            
        except Exception as e:
            self.logger.error(f"Error loading CSV file: {str(e)}")
            raise

    def tranform_dataframe(self) -> None:
        """
        Clean and standardize the loaded DataFrame.
        
        Raises:
            ValueError: If data hasn't been loaded yet
        """
        if not self.is_loaded:
            raise ValueError("DataFrame must be loaded before cleaning")
        
        # Transform columns
        self._socioeconomic_df = self._socioeconomic_df.melt(id_vars='Provincia', var_name='anio')
        self._socioeconomic_df.rename(columns={"value":"pib"}, inplace=True)
        self._socioeconomic_df.rename(columns={'Provincia': 'Province'}, inplace=True)
        self._socioeconomic_df['anio'] = pd.to_datetime(self._socioeconomic_df['anio'], format='%Y')
        self._socioeconomic_df['pib'] = self._socioeconomic_df['pib'].astype(float)
        self.logger.info(f"Columns transformed {list(self._socioeconomic_df.columns)}")

    def map_province_names(self) -> None:
        """
        Unify province names in the dataframe
        """
        self._socioeconomic_df = ProvinceMapper.map_province_name("socioeconomic", self._socioeconomic_df)

    def save_processed_file(self) -> None:
        """
        Export the processed socioeconomic DataFrame to a CSV file.
        
        Raises:
            ValueError: If no data is available to save
        """
        self._save_dataframe_to_csv(self._socioeconomic_df, "socioeconomic.csv")

    def process(self) -> None:
        """
        Execute the complete socioeconomic data processing pipeline.
        """
        try:
            self.load_csv_files()
            self.tranform_dataframe()
            self.map_province_names()
            self.save_processed_file()
            
        except Exception as e:
            self.logger.error(f"Error in processing pipeline: {str(e)}")
            raise