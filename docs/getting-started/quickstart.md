# Quick Start Guide

This guide will help you run the ETL pipeline for the first time and understand its output.

## Prerequisites

Before starting, ensure you have:

- [x] Completed the [installation](installation.md)
- [x] Activated your virtual environment
- [x] Verified all data files are in place

## Running the Pipeline

### Basic Execution

The simplest way to run the ETL pipeline:

```bash
cd tfm_air_quality
python src/etl_pipeline/main_orchestrator.py
```

This will execute the complete pipeline with default settings.

### Expected Output

When running successfully, you should see output similar to:

```
Starting automated data processing...
============================================================
2024-01-15 10:30:15 - ETLPipeline - INFO - Starting ETL Pipeline execution...
2024-01-15 10:30:15 - DataExtractionStep - INFO - Executing step 1/8: DataExtractionStep
2024-01-15 10:30:16 - DataExtractionStep - INFO - ✅ Step DataExtractionStep completed successfully
2024-01-15 10:30:16 - DataTransformationStep - INFO - Executing step 2/8: DataTransformationStep
...
============================================================
✅ Processing completed successfully!
Final dataset: 15420 rows, 18 columns
Total time: 0:02:34.567890
File saved as: /path/to/src/etl_pipeline/data/output/dataset.csv
Reports saved at: /path/to/src/etl_pipeline/data/output/reports
```

## Understanding the Pipeline Flow

The pipeline executes the following steps in order:

### 1. Data Extraction (30-45 seconds)
```
Executing step 1/8: DataExtractionStep
- Loading air quality data: air_quality_with_province.csv
- Loading health data: enfermedades_respiratorias.csv, esperanza_vida.csv  
- Loading socioeconomic data: PIB per cap provincias 2000-2021.csv, poblacion_provincias.csv
```

### 2. Data Transformation (15-30 seconds)
```
Executing step 2/8: DataTransformationStep
- Transforming air quality data (classification, province standardization)
- Transforming health data (column renaming, data cleaning)
- Transforming socioeconomic data (format conversion, standardization)
```

### 3. Data Merging (5-10 seconds)
```
Executing step 3/8: DataMergingStep
- Merging datasets on Province + Year keys
- Handling missing values in joined data
```

### 4. Feature Engineering (5 seconds)
```
Executing step 4/8: FeatureEngineeringStep
- Creating respiratory_deaths_per_100k feature
```

### 5. Data Cleaning (10-15 seconds)
```
Executing step 5/8: DataCleaningStep
- Removing island provinces (if configured)
- Filtering time range (2000-2021)
- Handling null values and duplicates
```

### 6. Data Validation (5-10 seconds)
```
Executing step 6/8: DataValidationStep
- Validating data types and required columns
- Checking business rules and statistical anomalies
```

### 7. Data Export (5 seconds)
```
Executing step 7/8: DataExportStep
- Exporting to CSV format: dataset.csv
```

### 8. Quality Report Generation (5 seconds)
```
Executing step 8/8: DataQualityReportStep
- Generating comprehensive quality report
```

## Examining the Results

### Output Files

After successful execution, you'll find these files:

```bash
# Main dataset
ls -la src/etl_pipeline/data/output/dataset.csv

# Quality report
ls -la src/etl_pipeline/data/output/reports/data_quality_report.json

# Execution log
ls -la src/etl_pipeline/logs/featuring_*.log
```

### Dataset Preview

Quick look at the final dataset:

```bash
# View first few rows
head -5 src/etl_pipeline/data/output/dataset.csv

# Check dataset dimensions
python -c "
import pandas as pd
df = pd.read_csv('src/etl_pipeline/data/output/dataset.csv')
print(f'Dataset shape: {df.shape}')
print(f'Columns: {list(df.columns)}')
print(f'Date range: {df.Year.min()} - {df.Year.max()}')
print(f'Provinces: {df.Province.nunique()}')
"
```

### Quality Report

Examine the data quality report:

```bash
# View quality report
python -c "
import json
with open('src/etl_pipeline/data/output/reports/data_quality_report.json') as f:
    report = json.load(f)
print(f'Total records: {report[\"total_records\"]}')
print(f'Total columns: {report[\"total_columns\"]}')
print(f'Missing data percentage: {report[\"missing_data\"][\"missing_percentage\"]:.2f}%')
print(f'Duplicate rows: {report[\"duplicate_rows\"]}')
"
```

## Common First-Run Scenarios

### Successful Execution

Expected characteristics of a successful run:

- **Dataset Size**: ~15,000-20,000 rows (varies by available data)
- **Columns**: ~15-20 columns including derived features
- **Processing Time**: 2-5 minutes on typical hardware
- **No Errors**: All steps complete with ✅ status

### Common Warnings (Normal)

You might see warnings like these (which are normal):

```
WARNING - Validation completed with warnings, continuing pipeline
WARNING - Columns with >5% null values: ['Air Pollution Level'] (7.3%)
WARNING - Statistical outliers detected in 'respiratory_deaths_per_100k': 23 values
```

These warnings indicate data quality issues that don't prevent pipeline completion.

## Quick Analysis

### Basic Data Exploration

Try these quick analyses with your new dataset:

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load the dataset
df = pd.read_csv('src/etl_pipeline/data/output/dataset.csv')

# Basic statistics
print(df.describe())

# Check data coverage by year
print(df.groupby('Year').size())

# Provincial coverage
print(f"Provinces in dataset: {sorted(df.Province.unique())}")

# Air quality distribution
if 'Air Pollution Level' in df.columns:
    print("\nAir Pollution Level distribution:")
    print(df['Air Pollution Level'].describe())
```

### Visualize Results

Quick visualization of the data:

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Time series of average air pollution
yearly_pollution = df.groupby('Year')['Air Pollution Level'].mean()
plt.figure(figsize=(10, 6))
plt.plot(yearly_pollution.index, yearly_pollution.values)
plt.title('Average Air Pollution Level Over Time')
plt.xlabel('Year')
plt.ylabel('Air Pollution Level')
plt.show()

# Health outcomes by province (top 10)
if 'respiratory_deaths_per_100k' in df.columns:
    top_provinces = df.groupby('Province')['respiratory_deaths_per_100k'].mean().sort_values(ascending=False).head(10)
    plt.figure(figsize=(12, 6))
    top_provinces.plot(kind='bar')
    plt.title('Top 10 Provinces by Respiratory Deaths per 100k')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
```

## Next Steps

Now that you have successfully run the pipeline:

1. **[Explore the Architecture](../architecture/overview.md)**: Understand how the system works
2. **[Customize Configuration](../configuration/config-manager.md)**: Adjust settings for your needs
3. **[Data Source Details](../data-sources/overview.md)**: Learn about the input data
4. **[Component Documentation](../components/extract.md)**: Dive into specific components

## Troubleshooting First Run

### Pipeline Fails Early

If the pipeline fails in extraction:

1. **Check Data Files**: Ensure all CSV files are present and readable
2. **Verify File Formats**: Confirm CSV files have expected structure
3. **Check Permissions**: Ensure read access to data directories

### Pipeline Fails During Processing

If the pipeline fails in transformation/validation:

1. **Check Logs**: Review the detailed log file for specific errors
2. **Data Quality**: Some data quality issues might be severe enough to stop processing
3. **Configuration**: Verify your configuration matches your data format

### Unexpected Results

If the dataset looks wrong:

1. **Check Input Data**: Verify your source CSV files contain expected data
2. **Time Range**: Confirm the configured time range matches your data
3. **Province Mapping**: Check if province names are being mapped correctly

### Performance Issues

If the pipeline runs very slowly:

1. **System Resources**: Ensure adequate RAM (4GB+) is available
2. **Data Size**: Very large input files will naturally take longer
3. **Disk Space**: Ensure sufficient space for output files and logs

For more detailed troubleshooting, see the [Troubleshooting Guide](../troubleshooting/common-issues.md).

## Success!

Congratulations! You've successfully run the ETL pipeline and generated a unified dataset for air quality, health, and socioeconomic analysis. You're now ready to explore the system in more depth or begin your analysis work.