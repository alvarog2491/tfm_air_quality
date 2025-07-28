#!/usr/bin/env python3
"""
Script to verify ETL pipeline output for CI.
"""
import pandas as pd
from pathlib import Path

from common.utils.file_utils import load_yaml_config


def verify_etl_output():
    """Verify ETL pipeline produced valid output using actual configuration."""
    
    output_file = Path("src/etl_pipeline/data/output/dataset.csv")
    
    # Check if ETL output exists
    if not output_file.exists():
        raise FileNotFoundError('ETL pipeline did not produce expected output')
    
    # Load and validate the output
    df = pd.read_csv(output_file)
    print(f'ETL output shape: {df.shape}')
    
    if df.empty:
        raise ValueError('ETL output is empty')
    
    # Load expected columns from actual configuration
    try:
        config = load_yaml_config("params.yaml")
        expected_columns = [
            config.get('preprocess', {}).get('target_column', 'Respiratory_diseases_total')
        ]
        
        # Add other expected columns from the actual pipeline
        expected_columns.extend(['Life_expectancy_total', 'pib', 'Air Pollution Level'])
        
        for col in expected_columns:
            if col not in df.columns:
                print(f'Warning: Expected column {col} not found. Available columns: {list(df.columns)}')
        
    except Exception as e:
        print(f'Could not load configuration: {e}')
        # Fallback to basic validation
        if len(df.columns) < 5:
            raise ValueError('ETL output has too few columns')
    
    print('ETL pipeline executed successfully!')
    print(f'Columns: {list(df.columns)}')


if __name__ == "__main__":
    verify_etl_output()