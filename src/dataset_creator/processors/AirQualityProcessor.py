"""
Air Quality Data Processor - Spanish Air Quality Classification by Province

Processes Spanish air quality data and classifies pollution levels based on thresholds.

INPUT: "air_quality_with_province.csv"
| Air Pollutant | Air Pollutant Description     | Data Aggregation Process      | Year       | Air Pollution Level |
|---------------|-------------------------------|------------------------------ |------------|---------------------|
| no2           | Nitrogen dioxide (air)        | Annual mean / 1 calendar year | 1991-01-01 | 80.639              |

| Unit Of Air Pollution Level | Air Quality Station Type | Air Quality Station Area | Altitude | Province |
|-----------------------------|--------------------------|--------------------------|----------|----------|
| ug/m3                       | Background               | urban                    | 593.0    | Madrid   |


OUTPUT: Same columns + Quality classification
| Air Pollutant | Air Pollutant Description     | Data Aggregation Process      | Year       | Air Pollution Level /
|---------------|-------------------------------|-------------------------------|------------|---------------------/
| no2           | Nitrogen dioxide (air)        | Annual mean / 1 calendar year | 1991-01-01 | 80.639              /

| Unit Of Air Pollution Level | Air Quality Station Type | Air Quality Station Area | Altitude | Province | Quality             |
|-----------------------------|--------------------------|--------------------------|----------|----------|---------------------|
| ug/m3                       | Background               | urban                    | 593.0    | Madrid   | RAZONABLEMENTE BUENA |


PROCESSING:
- Loads CSV with optimized data types for air quality measurements
- Classifies pollution levels using pollutant-specific thresholds
- Normalizes pollutant names and applies quality labels
- Standardizes province names using ProvinceMapper
- Exports to "air_quality.csv"

USAGE: processor = AirQualityProcessor(); processor.process()
"""

from pathlib import Path
from typing import Optional, Dict



import pandas as pd
from utils.air_quality_rules import quality_thresholds, quality_labels
from utils.province_mapper import ProvinceMapper
from processors.BaseProcessor import BaseProcessor

class AirQualityProcessor(BaseProcessor):
    """
    Handles the processing of air quality data from CSV files, including loading, classification, and province normalization.
    """

    # Define column data types as class constant for reusability
    _COLUMN_DTYPES: Dict[str, str] = {
        'Air Pollutant': 'category',
        'Air Pollutant Description': 'category',
        'Data Aggregation Process': 'category',
        'Year': 'Int64',
        'Air Pollution Level': 'float64',
        'Unit Of Air Pollution Level': 'category',
        'Air Quality Station Type': 'category',
        'Air Quality Station Area': 'category',
        'Altitude': 'float64',
        'Longitude':'float64',
        'Latitude':'float64',
        'Province': 'category',
    }

    def __init__(self, data_folder: Optional[Path] = None):
        """
        Initialize the AirQualityProcessor.
        
        Args:
            data_folder: Optional custom path to data folder. If None, uses default relative path.
        """
        super().__init__(data_folder, "air_quality_data")
        # Initialize DataFrame as private attribute
        self._air_quality_df: Optional[pd.DataFrame] = None
    
    @property
    def air_quality_df(self) -> Optional[pd.DataFrame]:
        """Get the air quality DataFrame."""
        return self._air_quality_df
    
    @property
    def is_loaded(self) -> bool:
        """Check if data has been loaded."""
        return self._air_quality_df is not None and not self._air_quality_df.empty
    
    def load_csv_files(self) -> None:
        """
        Load raw air quality data from CSV file.
            
        Raises:
            FileNotFoundError: If the required CSV file is not found
            ValueError: If the loaded file is empty
        """
        self.logger.info(f"Loading raw air quality data from: {self.data_folder}")
        file_path = self.data_folder / "raw" / "air_quality_with_province.csv"
        
        if not file_path.is_file():
            raise FileNotFoundError(f"Required file not found: {file_path}")
        
        try:
            # Load with optimized data types and specific columns only
            self._air_quality_df = pd.read_csv(
                file_path, 
                usecols=self._COLUMN_DTYPES.keys(), 
                dtype=self._COLUMN_DTYPES,
                parse_dates=['Year']
            )
            self._validate_dataframe_not_empty(self._air_quality_df, file_path)
            self._log_dataframe_info(self._air_quality_df, "air quality data")
            self._log_null_values(self._air_quality_df, "air quality data")
            
        except Exception as e:
            self.logger.error(f"Error loading CSV file: {str(e)}")
            raise 
    
    def classify_quality(self) -> None:
        """
        Assign air quality labels based on pollutant-specific thresholds.
        
        Raises:
            ValueError: If data hasn't been loaded yet
        """
        if not self.is_loaded:
            raise ValueError("Data must be loaded before quality classification")
        
        # Skip if quality already classified
        if 'Quality' in self._air_quality_df.columns:
            self.logger.info("Quality classification already exists, skipping")
            return
        
        # Normalize pollutant names for consistent matching
        self._air_quality_df['Air Pollutant'] = self._air_quality_df['Air Pollutant'].str.lower()

        def get_quality(row):
            """Get quality classification for a single row based on pollutant type and level."""
            pollutant = row['Air Pollutant']
            value = row['Air Pollution Level']
            if pollutant in quality_thresholds:
                bins = quality_thresholds[pollutant]
                return pd.cut([value], bins=bins, labels=quality_labels)[0]
            else:
                return 'UNKNOWN'

        # Apply classification row by row
        self._air_quality_df['Quality'] = self._air_quality_df.apply(get_quality, axis=1)
        
        # Apply category type to cols
        self._air_quality_df['Air Pollutant'] = self._air_quality_df['Air Pollutant'].astype("category")
        self._air_quality_df['Quality'] = self._air_quality_df['Quality'].astype("category")

        # Log classification results
        quality_counts = self._air_quality_df['Quality'].value_counts()
        self.logger.info(f"Air quality classification completed: {quality_counts.to_dict()}")
        
        # Warn about unknown classifications
        unknown_count = quality_counts.get('UNKNOWN', 0)
        if unknown_count > 0:
            total_records = len(self._air_quality_df)
            unknown_percentage = (unknown_count / total_records) * 100
            self.logger.warning(
                f"{unknown_count:,} records ({unknown_percentage:.1f}%) "
                f"could not be classified (UNKNOWN)"
            )

    def map_province_names(self) -> None:
        """
        Unify province name among all dataframes
        """
        self._air_quality_df = ProvinceMapper.map_province_name("Air quality", self._air_quality_df)

    def save_processed_file(self) -> None:
        """     
        Saves the processed air quality data to the processed data folder.
        
        Raises:
            ValueError: If no data is available to save
        """
        self._save_dataframe_to_csv(self._air_quality_df, "air_quality.csv")

    def process(self) -> None:
        """
        Execute the complete air quality data processing pipeline.
        """
        try:
            self.load_csv_files()
            self.classify_quality()
            self.map_province_names()
            self.save_processed_file()

        except Exception as e:
            self.logger.error(f"Error in processing pipeline: {str(e)}")
            raise