import json
from pathlib import Path
import logging
from typing import Dict, List, Set
import pandas as pd

class ProvinceMapper:
    """
    ProvinceMapper

    A utility class for normalizing province names in a pandas DataFrame using a JSON mapping file.
    The JSON must contain exactly 52 official Spanish provinces, each with a list of accepted name variants.
    """

    logger = logging.getLogger("ProvinceMapper")
    unified_province_dict: Dict[str, List[str]] = None

    @staticmethod
    def _load_json_file() -> None:
        """
        Load the JSON mapping file containing province name variants.

        This method loads and caches the mapping file 'unified_province_name.json' located in the same
        directory as this script. It validates that the mapping includes exactly 52 provinces.

        Raises:
            FileNotFoundError: If the mapping file is not found.
            ValueError: If the mapping does not contain exactly 52 provinces.
        """
        if ProvinceMapper.unified_province_dict is None:
            json_path = Path(__file__).parent / 'unified_province_name.json'
            if not json_path.is_file():
                raise FileNotFoundError(f"Expected file not found: {json_path}")
            
            with open(json_path, "r", encoding="utf-8") as f:
                ProvinceMapper.unified_province_dict = json.load(f)
            
            num_provinces = len(ProvinceMapper.unified_province_dict.keys())
            if num_provinces != 52:
                raise ValueError(f"Expected 52 provinces in the dictionary, but found {num_provinces}.")

    @staticmethod
    def map_province_name(df_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize province names in a pandas DataFrame using the province mapping file.

        Args:
            df_name: Name of the DataFrame (used for logging purposes).
            df: DataFrame that must contain a 'Province' column.

        Returns:
            pandas.DataFrame: DataFrame with the 'Province' column normalized and converted to a categorical type.

        Raises:
            KeyError: If the 'Province' column is missing from the DataFrame.
        """
        if "Province" not in df.columns:
            raise KeyError("Missing required column: 'Province'") 

        ProvinceMapper._load_json_file()
        ProvinceMapper.logger.info(f"Mapping province names in {df_name} dataset")

        # Create flat mapping from all aliases to official names
        province_mapping: Dict[str, str] = {
            alias: province
            for province, aliases in ProvinceMapper.unified_province_dict.items()
            for alias in aliases
        }

        # Apply mapping and convert to category
        df['Province'] = df['Province'].astype(str).replace(province_mapping)
        df['Province'] = df['Province'].astype('category')

        ProvinceMapper._check_provinces(df)
        return df

    @staticmethod
    def _check_provinces(df: pd.DataFrame) -> None:
        """
        Validate that all province names in the DataFrame are recognized.

        Logs a warning if there are any unrecognized province names after the normalization process.

        Args:
            df: DataFrame containing the 'Province' column to validate.
        """
        official_names: Set[str] = set(ProvinceMapper.unified_province_dict.keys())
        aliases: Set[str] = {alias for alias_list in ProvinceMapper.unified_province_dict.values() for alias in alias_list}
        all_known: Set[str] = official_names.union(aliases)

        provinces_in_df: Set[str] = set(df['Province'].unique())
        unknown_provinces: Set[str] = provinces_in_df - all_known

        if len(unknown_provinces) > 0:
            ProvinceMapper.logger.warning(f"Unrecognized provinces: {unknown_provinces}")
