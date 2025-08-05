# ETL Pipeline - Getting Started

This guide will help you set up and run the ETL Pipeline for the first time. The ETL Pipeline is responsible for processing raw data into a clean, integrated dataset ready for analysis.

## Prerequisites

Before starting with the ETL Pipeline, ensure you have:

- **Python 3.8+** installed
- **Repository cloned** and dependencies installed (see [main installation guide](../getting-started/installation.md))
- **Raw data files** in the correct directory structure
- **Virtual environment** activated

## Quick Start

### 1. Verify Data Structure

First, ensure your data directory structure is correct:

```bash
# Check if the data structure utility works
python -c "from src.etl_pipeline.utils import CheckProjectStructure; CheckProjectStructure().execute()"
```

This should show the data path and create any missing directories:

```
src/etl_pipeline/data/
├── air_quality_data/raw/
├── health_data/raw/
├── socioeconomic_data/raw/
└── output/reports/
```

### 2. Place Raw Data Files

Ensure you have these files in their respective locations:

```bash
# Air quality data
src/etl_pipeline/data/air_quality_data/raw/air_quality_with_province.csv

# Health data  
src/etl_pipeline/data/health_data/raw/enfermedades_respiratorias.csv
src/etl_pipeline/data/health_data/raw/esperanza_vida.csv

# Socioeconomic data
src/etl_pipeline/data/socioeconomic_data/raw/PIB\ per\ cap\ provincias\ 2000-2021.csv
src/etl_pipeline/data/socioeconomic_data/raw/poblacion_provincias.csv
```

### 3. Run the ETL Pipeline

Execute the complete pipeline:

```bash
cd tfm_air_quality
python src/etl_pipeline/main_orchestrator.py
```

## Expected Output

### Console Output

During execution, you should see output similar to:

```
Starting automated data processing...
============================================================
2024-01-15 10:30:15 - ETLPipeline - INFO - Starting ETL Pipeline execution...
2024-01-15 10:30:15 - DataExtractionStep - INFO - ======================== Starting DataExtractionStep ========================
2024-01-15 10:30:16 - AirQualityDataExtractor - INFO - DataFrame shape: (15420, 12)
2024-01-15 10:30:16 - AirQualityDataExtractor - INFO - Memory usage: 1.45 MB
2024-01-15 10:30:16 - DataExtractionStep - INFO - ✅ Step DataExtractionStep completed successfully
...
============================================================
✅ Processing completed successfully!
Final dataset: 15420 rows, 18 columns
Total time: 0:02:34.567890
File saved as: /path/to/src/etl_pipeline/data/output/dataset.csv
Reports saved at: /path/to/src/etl_pipeline/data/output/reports
```

### Output Files

After successful execution, you'll find:

```bash
# Main output dataset
src/etl_pipeline/data/output/dataset.csv

# Data quality report
src/etl_pipeline/data/output/reports/data_quality_report.json

# Execution log
src/etl_pipeline/logs/featuring_YYYY-MM-DD_HH-MM-SS.log
```

## Pipeline Steps Explained

The ETL Pipeline executes 8 steps in sequence:

### Step 1: Data Extraction (30-60 seconds)
```
Executing step 1/8: DataExtractionStep
- Loading air quality data (12 columns, ~15K rows)
- Loading respiratory diseases data
- Loading life expectancy data  
- Loading GDP data
- Loading population data
```

**What happens**: Raw CSV files are loaded into memory with appropriate data types and encodings.

### Step 2: Data Transformation (15-30 seconds)
```
Executing step 2/8: DataTransformationStep
- Transforming air quality data (pollution classification)
- Transforming health data (column standardization)
- Transforming socioeconomic data (format conversion)
```

**What happens**: Domain-specific transformations apply business logic and standardize data formats.

### Step 3: Data Merging (5-10 seconds)
```
Executing step 3/8: DataMergingStep
- Merging on Province + Year keys
- Left joins preserve air quality as base dataset
```

**What happens**: All datasets are integrated using Province and Year as common keys.

### Step 4: Feature Engineering (5 seconds)
```
Executing step 4/8: FeatureEngineeringStep
- Creating respiratory_deaths_per_100k feature
```

**What happens**: Derived features are calculated for analysis (e.g., normalized mortality rates).

### Step 5: Data Cleaning (10-15 seconds)
```
Executing step 5/8: DataCleaningStep
- Removing excluded regions (islands, autonomous cities)
- Filtering time range (2000-2021)
- Handling null values and duplicates
```

**What happens**: Data quality issues are addressed and the dataset is prepared for analysis.

### Step 6: Data Validation (5-10 seconds)
```
Executing step 6/8: DataValidationStep
- Validating data types and required columns
- Checking business rules and statistical constraints
```

**What happens**: Comprehensive validation ensures data quality before export.

### Step 7: Data Export (5 seconds)
```
Executing step 7/8: DataExportStep
- Exporting to CSV format
```

**What happens**: The clean dataset is saved to the output directory.

### Step 8: Quality Report Generation (5 seconds)
```
Executing step 8/8: DataQualityReportStep
- Generating comprehensive quality metrics
```

**What happens**: A detailed quality report is generated in JSON format.

## Examining Results

### Dataset Overview

Quick examination of the output dataset:

```bash
# Check dataset dimensions
python -c "
import pandas as pd
df = pd.read_csv('src/etl_pipeline/data/output/dataset.csv')
print(f'Dataset shape: {df.shape}')
print(f'Columns: {list(df.columns)}')
print(f'Date range: {df.Year.min()} - {df.Year.max()}')
print(f'Provinces: {df.Province.nunique()}')
print(f'Unique provinces: {sorted(df.Province.unique())}')
"
```

Expected output:
```
Dataset shape: (15420, 18)
Columns: ['Province', 'Year', 'Air Pollution Level', 'Respiratory_diseases_total', ...]
Date range: 2000-2021
Provinces: 47
Unique provinces: ['A Coruña', 'Albacete', 'Alicante', ...]
```

### Quality Report

Examine the data quality report:

```bash
# View key quality metrics
python -c "
import json
with open('src/etl_pipeline/data/output/reports/data_quality_report.json') as f:
    report = json.load(f)
print(f'Total records: {report[\"total_records\"]:,}')
print(f'Total columns: {report[\"total_columns\"]}')
print(f'Memory usage: {report[\"memory_usage_mb\"]:.1f} MB')
print(f'Missing data: {report[\"missing_data\"][\"missing_percentage\"]:.2f}%')
print(f'Duplicate rows: {report[\"duplicate_rows\"]}')
"
```

### Sample Data Exploration

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load the dataset
df = pd.read_csv('src/etl_pipeline/data/output/dataset.csv')

# Basic statistics
print("Dataset Summary:")
print(df.describe())

# Check data completeness by year
print("\nData coverage by year:")
coverage = df.groupby('Year').agg({
    'Province': 'count',
    'Air Pollution Level': lambda x: x.notna().sum(),
    'Respiratory_diseases_total': lambda x: x.notna().sum()
})
print(coverage)

# Provincial data availability
print(f"\nProvinces with complete data: {df.groupby('Province').size().min()}-{df.groupby('Province').size().max()} records per province")
```

## Configuration Options

### Basic Configuration

The pipeline uses `src/etl_pipeline/config/pipeline_config.yaml` for configuration. Key settings include:

```yaml
# Time range for data processing
processing:
  time_range:
    start_year: 2000
    end_year: 2021

# Data quality thresholds  
  data_quality:
    null_threshold_percent: 5.0
    allow_duplicates: false

# Regions to exclude from analysis
  excluded_regions:
    - "Santa Cruz de Tenerife"
    - "Las Palmas" 
    - "Illes Balears"
```

### Environment-Specific Configuration

Create environment-specific configurations:

```bash
# Development configuration
cp src/etl_pipeline/config/pipeline_config.yaml src/etl_pipeline/config/pipeline_config_development.yaml

# Set environment
export ETL_ENV=development
python src/etl_pipeline/main_orchestrator.py
```

## Common Issues and Solutions

### Missing Data Files

**Error**: `FileNotFoundError: Air quality data file not found`

**Solution**: 
1. Verify all required CSV files are present
2. Check file names match exactly (case-sensitive)
3. Ensure file permissions allow reading

### Memory Issues

**Error**: `MemoryError: Unable to allocate memory`

**Solution**:
1. Ensure at least 4GB RAM available
2. Close other memory-intensive applications
3. Consider processing smaller date ranges

### Province Name Mismatches

**Warning**: `Province names not found in mapping`

**Solution**:
1. Check `src/etl_pipeline/utils/unified_province_name.json`
2. Add missing province name mappings
3. Verify source data uses expected province names

### Encoding Issues

**Error**: `UnicodeDecodeError`

**Solution**:
1. Verify data files use expected encoding (UTF-8, Latin1, ISO-8859-1)
2. Check configuration file encoding settings
3. Use text editor to verify file encoding

## Testing Your Setup

### Run Tests

Verify your installation with the test suite:

```bash
# Run all ETL tests
pytest src/etl_pipeline/tests/ -v

# Run specific component tests
pytest src/etl_pipeline/tests/extract_tests/ -v
pytest src/etl_pipeline/tests/transform_tests/ -v
pytest src/etl_pipeline/tests/load_tests/ -v
```

### Validate Configuration

```bash
# Test configuration loading
python -c "
from src.etl_pipeline.config.config_manager import get_config
config = get_config()
print('Configuration loaded successfully')
print(f'Pipeline steps: {len(config.get_pipeline_steps())}')
print(f'Data sources: {list(config.config[\"data_sources\"].keys())}')
"
```

## Next Steps

Now that you have successfully run the ETL Pipeline:

1. **Explore the Architecture**: Learn about the [ETL Pipeline Architecture](architecture.md)
2. **Understand Components**: Dive into [Extract Layer](components/extract.md), [Transform Layer](components/transform.md), and [Load Layer](components/load.md)
3. **Customize Configuration**: Learn about [Configuration Management](configuration.md)
4. **Use with ML Pipeline**: The output dataset is ready for the [ML Pipeline](../ml-pipeline/overview.md)
5. **Troubleshooting**: See [Common Issues](troubleshooting.md) for detailed problem-solving

The ETL Pipeline is now ready to process your data and provide clean, integrated datasets for analysis and machine learning!