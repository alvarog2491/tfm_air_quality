
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
- Melts wide format to long format using pandas.melt()Ø
- Standardizes province names and converts data types
- Exports to "socioeconomic.csv"

USAGE: processor = SocioeconomicProcessor(); processor.process()
"""


from source_processors.base_processor import BaseProcessor
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
        self._gdp_df: Optional[pd.DataFrame] = None
        self._province_population_df: Optional[pd.DataFrame] = None
    
    @property
    def gdp_df(self) -> Optional[pd.DataFrame]:
        """Get the pib DataFrame."""
        return self._gdp_df
    
    @property
    def province_population_df(self) -> Optional[pd.DataFrame]:
        """Get the province population size DataFrame."""
        return self._province_population_df
    
    @property
    def is_loaded(self) -> bool:
        """Check if data has been loaded."""
        return (self._gdp_df is not None and not self._gdp_df.empty and 
                self._province_population_df is not None and 
                not self._province_population_df.empty)
    
    def load_csv_files(self) -> None:
        """
        Load raw socioeconomic data from CSV files
        
        Raises:
            FileNotFoundError: If the required CSV file is not found
            ValueError: If the loaded file is empty
        """
        self.logger.info(f"Loading raw socioeconomic data from: {self.data_folder}")
        pib_file = self.data_folder / "raw" / "PIB per cap provincias 2000-2021.csv"
        population_file = self.data_folder / "raw" / "poblacion_provincias.csv"

        # Check if file exists before attempting to load
        if not pib_file.is_file():
            raise FileNotFoundError(f"Required file not found: {pib_file}")
        
        if not population_file.is_file():
            raise FileNotFoundError(f"Required file not found: {population_file}")

        try:
            # Load CSV file with appropriate settings for Spanish data
            self._gdp_df = pd.read_csv(
                pib_file,
                sep=';', 
                decimal=',',
                encoding='ISO-8859-1'
            )
            
            self._province_population_df = pd.read_csv(
                population_file,
                parse_dates=['Periodo'],
                sep=';', 
                decimal=',', 
                encoding='latin1'
             )
            # Validate loaded data
            self._validate_dataframe_not_empty(self._gdp_df, pib_file)
            self._validate_dataframe_not_empty(self._province_population_df, population_file)

            # Log data info
            self._log_dataframe_info(self._gdp_df, "GDP")
            self._log_dataframe_info(self._province_population_df, "province population")
            
            # Log data quality info
            self._log_null_values(self._gdp_df, "GDP")
            self._log_null_values(self._province_population_df, "province population")
            
        except Exception as e:
            self.logger.error(f"Error loading CSV file: {str(e)}")
            raise

    def tranform_dataframes(self) -> None:
        """
        Clean and standardize the loaded DataFrames.
        
        Raises:
            ValueError: If data hasn't been loaded yet
        """
        if not self.is_loaded:
            raise ValueError("DataFrame must be loaded before cleaning")

        self._transform_gdp_columns()
        self._transform_population_columns()
    
    def _transform_gdp_columns(self) -> None:
        """
        Transform GDP columns from wide to long format.
        """

        # Melt the DataFrame to long format
        self.logger.info("Transforming GDP DataFrame from wide to long format")
        self._gdp_df = self._gdp_df.melt(id_vars='Provincia', var_name='anio')
        self._gdp_df.rename(columns={"value": "pib"}, inplace=True)
        self._gdp_df.rename(columns={'Provincia': 'Province'}, inplace=True)
        self._gdp_df['anio'] = pd.to_datetime(self._gdp_df['anio'], format='%Y')
        self._gdp_df['pib'] = self._gdp_df['pib'].astype(float)

    def _transform_population_columns(self) -> None:
        """
            Clean population size dataframe
        """
        self.logger.info("Transforming province population DataFrame")

        # Rename columns for consistency and apply data type conversions
        self._province_population_df.rename(columns={'Total': 'Population', 'Periodo': 'Year'}, inplace=True)
        self._province_population_df['Population'] = self._province_population_df['Population'].str.replace('.', '', regex=False).astype(int)
        
        # Drop unnecessary columns
        self._province_population_df.drop(columns=['Sexo'], inplace=True)

        # Clean province names by removing numeric codes and extra spaces
        if 'Provincias' in self._province_population_df.columns:
            self._province_population_df['Provincias'] = self._province_population_df['Provincias'].str.replace(r'[0-9\s]+', '', regex=True)
            self._province_population_df.rename(columns={'Provincias': 'Province'}, inplace=True)
            self.logger.info(f"Removed numeric codes on province names")

    def map_province_names(self) -> None:
        """
        Unify province names in the dataframe
        """
        self._gdp_df = ProvinceMapper.map_province_name("gdp", self._gdp_df)
        self._province_population_df = ProvinceMapper.map_province_name("province_population", self._province_population_df)

    def save_processed_file(self) -> None:
        """
        Export the processed socioeconomic DataFrame to a CSV file.
        
        Raises:
            ValueError: If no data is available to save
        """
        self._save_dataframe_to_csv(self._gdp_df, "province_gdp.csv")
        self._save_dataframe_to_csv(self._province_population_df, "province_population_size.csv")

    def process(self) -> None:
        """
        Execute the complete socioeconomic data processing pipeline.
        """
        try:
            self.load_csv_files()
            self.tranform_dataframes()
            self.map_province_names()
            self.save_processed_file()
            
        except Exception as e:
            self.logger.error(f"Error in processing pipeline: {str(e)}")
            raise