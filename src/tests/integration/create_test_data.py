#!/usr/bin/env python3
"""
Script to create minimal test data for CI pipeline.
"""
import pandas as pd
import os
from pathlib import Path


def create_test_data():
    """Create minimal test data for ETL pipeline."""
    
    # Create test data directories
    base_dir = Path("src/etl_pipeline/data/raw")
    air_quality_dir = base_dir / "air_quality"
    health_dir = base_dir / "health"
    socioeconomic_dir = base_dir / "socioeconomic"
    
    for directory in [air_quality_dir, health_dir, socioeconomic_dir]:
        directory.mkdir(parents=True, exist_ok=True)
    
    # Create minimal test air quality data
    air_data = pd.DataFrame({
        'Air Pollutant': ['NO2', 'PM10'],
        'Air Pollutant Description': ['Nitrogen Dioxide', 'Particulate Matter'],
        'Data Aggregation Process': ['Annual Mean', 'Annual Mean'],
        'Air Pollution Level': [25.5, 18.2],
        'Unit Of Air Pollution Level': ['µg/m³', 'µg/m³'],
        'Air Quality Station Type': ['Traffic', 'Urban Background'],
        'Air Quality Station Area': ['Urban', 'Urban'],
        'Altitude': [650, 700],
        'Province': ['Madrid', 'Barcelona'],
        'Year': [2020, 2020],
        'Quality': ['Valid', 'Valid']
    })
    air_data.to_csv(air_quality_dir / "test_air_quality.csv", index=False)
    
    # Create minimal test health data
    health_data = pd.DataFrame({
        'Province': ['Madrid', 'Barcelona'],
        'Year': [2020, 2020],
        'Respiratory_diseases_total': [150.2, 142.8],
        'Life_expectancy_total': [83.5, 83.2]
    })
    health_data.to_csv(health_dir / "test_health.csv", index=False)
    
    # Create minimal test socioeconomic data
    socio_data = pd.DataFrame({
        'Province': ['Madrid', 'Barcelona'],
        'Year': [2020, 2020],
        'pib': [35000, 32000]
    })
    socio_data.to_csv(socioeconomic_dir / "test_socio.csv", index=False)
    
    print("Test data created successfully!")


if __name__ == "__main__":
    create_test_data()