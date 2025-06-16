import json
from pathlib import Path
import logging

class ProvinceMapper:
    """
    Handles normalization of province names in a DataFrame based on a mapping JSON file.
    """

    logger = logging.getLogger("ProvinceMapper")
    unified_province_dict = None

    @staticmethod
    def _load_json_file():
        """
        Load province mapping JSON file and cache it.
        Validates that exactly 52 (Spanish provinces) are present.
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
    def map_province_name(df_name: str, df):
        """
        Normalize province names in DataFrame using mapping file.
        Converts Province column to categorical type.
        
        Returns:
        - pandas.DataFrame: DataFrame with normalized Province column
        """
        if("Province" not in df.columns):
            raise KeyError("Missing required column: 'Province'") 

        ProvinceMapper._load_json_file()
        ProvinceMapper.logger.info(f"Mapping province names in {df_name} dataset")

        # Create flat mapping from all aliases to official names
        province_mapping = {
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
    def _check_provinces(df):
        """
        Log any unrecognized province names found in DataFrame.
        """
        official_names = set(ProvinceMapper.unified_province_dict.keys())
        aliases = {alias for alias_list in ProvinceMapper.unified_province_dict.values() for alias in alias_list}
        all_known = official_names.union(aliases)

        provinces_in_df = set(df['Province'].unique())
        unknown_provinces = provinces_in_df - all_known

        if len(unknown_provinces) > 0:
            ProvinceMapper.logger.warning(f"Unrecognized provinces: {unknown_provinces}")