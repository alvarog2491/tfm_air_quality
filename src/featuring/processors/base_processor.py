from pathlib import Path
import logging
from typing import Optional
import pandas as pd
from abc import ABC, abstractmethod

class BaseProcessor(ABC):
    """
    Base class for data processors that handles common functionality on the processing pipeline.
    """

    def __init__(self, data_folder: Optional[Path] = None, data_subfolder: str = ""):
        """
        Initialize the BaseProcessor.
        
        Args:
            data_folder: Optional custom path to data folder. If None, uses default relative path.
            data_subfolder: Subfolder name within the data directory (e.g., "air_quality_data", "health_data")
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if data_folder is None:
            script_dir = Path(__file__).resolve().parent.parent
            self.data_folder = (script_dir / "data" / data_subfolder).resolve()
        else:
            self.data_folder = data_folder

    @property
    @abstractmethod
    def is_loaded(self) -> bool:
        """Check if data has been loaded. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def load_csv_files(self) -> None:
        """Load raw data from CSV files. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def map_province_names(self) -> None:
        """Map province names using ProvinceMapper. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def process(self) -> None:
        """
        Execute the complete data processing pipeline. Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def save_processed_file(self) -> None:
        """Save processed data to file. Must be implemented by subclasses."""
        pass

    def _validate_dataframe_not_empty(self, df: pd.DataFrame, file_path: Path) -> None:
        """
        Validate that a DataFrame is not empty.
        
        Args:
            df: DataFrame to validate
            file_path: Path of the file that was loaded (for error messages)
            
        Raises:
            ValueError: If the DataFrame is empty
        """
        if df.empty:
            raise ValueError(f"Loaded file is empty: {file_path}")

    def _log_dataframe_info(self, df: pd.DataFrame, description: str) -> None:
        """
        Log information about a DataFrame (rows, columns, memory usage).
        
        Args:
            df: DataFrame to log information about
            description: Description of the DataFrame for logging
        """
        rows, cols = df.shape
        memory_usage = df.memory_usage(deep=True).sum() / 1024**2  # MB

        self.logger.info(
            f"Successfully loaded {description}: {rows:,} rows and {cols} columns "
            f"(~{memory_usage:.1f} MB memory usage)"
        )

    def _log_null_values(self, df: pd.DataFrame, description: str) -> None:
        """
        Log null values found in a DataFrame.
        
        Args:
            df: DataFrame to check for null values
            description: Description of the DataFrame for logging
        """
        null_counts = df.isnull().sum()
        if null_counts.any():
            self.logger.warning(f"Found null values in {description}: {null_counts[null_counts > 0].to_dict()}")

    def _save_dataframe_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save a DataFrame to CSV with common optimizations and logging.
        
        Args:
            df: DataFrame to save
            filename: Name of the output file
            
        Raises:
            ValueError: If DataFrame is empty
        """
        if df is None or df.empty:
            raise ValueError("No data available to save. Load and process data first.")
        
        # Ensure processed directory exists
        processed_dir = self.data_folder / "processed"
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        processed_file_path = processed_dir / filename
        
        try:
            self.logger.info(f"Saving processed file to: {processed_file_path}")
            
            # Save with optimizations
            df.to_csv(
                processed_file_path, 
                index=False,
                float_format='%.3f' 
            )
            
            # Log save success with file info
            file_size = processed_file_path.stat().st_size / 1024**2  # MB
            record_count = len(df)
            
            self.logger.info(
                f"Successfully saved {record_count:,} records "
                f"(~{file_size:.1f} MB)"
            )
            
        except Exception as e:
            self.logger.error(f"Error saving processed file: {str(e)}")
            raise