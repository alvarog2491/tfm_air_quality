import logging
import pandas as pd


class FeatureEngineering:
    """
    Class for feature engineering steps in the pipeline.
    """

    def __init__(self):
        """
        Initialize the FeatureEngineering class.
        """
        self.logger = logging.getLogger(__name__)

    def apply(self, df):
        """
        Apply feature engineering transformations to the input dataset.
        
        Performs various data transformations and creates derived features to enhance
        the dataset for analysis.
        
        Args:
            df (pd.DataFrame): Input DataFrame to be transformed.
        
        Returns:
            pd.DataFrame: Enhanced DataFrame with additional engineered features
                        and transformations applied.
                        
        Note:
            Specific transformations are applied conditionally based on column availability.
        """
        # Create population-adjusted respiratory disease mortality rate
        if 'Respiratory_diseases_total' in df.columns and 'Population' in df.columns:
            df['respiratory_deaths_per_100k'] = round((df['Respiratory_diseases_total'] / df['Population']) * 100000, 2)
            self.logger.info("Created respiratory_deaths_per_100k feature")
        
        return df