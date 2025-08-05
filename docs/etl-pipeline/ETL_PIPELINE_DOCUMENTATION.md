# ETL Pipeline Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Sources](#data-sources)
4. [Pipeline Components](#pipeline-components)
5. [Configuration](#configuration)
6. [Data Flow](#data-flow)
7. [Usage](#usage)
8. [Testing](#testing)
9. [Troubleshooting](#troubleshooting)

## Overview

The ETL (Extract, Transform, Load) pipeline is a comprehensive data processing system designed for analyzing air quality, health outcomes, and socioeconomic factors across Spanish provinces. This pipeline processes multiple heterogeneous data sources and creates a unified dataset optimized for machine learning analysis.

### Key Features
- **Modular Design**: Separate components for extraction, transformation, and loading
- **Data Quality Focus**: Comprehensive validation and quality reporting
- **Error Recovery**: Built-in recovery mechanisms for common failure scenarios
- **Configurable**: YAML-based configuration with environment-specific overrides
- **Extensive Logging**: Detailed logging throughout all pipeline stages
- **Testing**: Comprehensive test suite with pytest framework

## Architecture

The ETL pipeline follows a layered architecture with clear separation of concerns:

```
ETL Pipeline
├── Main Orchestrator (main_orchestrator.py)
├── Extract Layer
│   ├── Data Extraction Step
│   └── Data Extractors (Air Quality, Health, Socioeconomic)
├── Transform Layer
│   ├── Data Transformation Step
│   ├── Data Merging Step
│   ├── Feature Engineering Step
│   ├── Data Cleaning Step
│   └── Data Validation Step
└── Load Layer
    ├── Data Export Step
    └── Data Quality Report Step
```

### Base Classes and Patterns

**ETLStep Base Class** (`etl_step.py`):
- Abstract base class for all pipeline steps
- Provides common logging functionality
- Implements recovery mechanism interface
- Enforces consistent step execution pattern

**BaseExtractor Pattern** (`extract/data_extractors/base_extractor.py`):
- Abstract base for all data extractors
- Common DataFrame logging utilities
- Standardized error handling
- Consistent initialization pattern

**BaseTransformer Pattern** (`transform/data_transformers/base_transformer.py`):
- Abstract base for domain-specific transformers
- Province name standardization utilities
- Common data cleaning methods

## Data Sources

### 1. Air Quality Data
**Location**: `src/etl_pipeline/data/air_quality_data/raw/air_quality_with_province.csv`

**Description**: Spanish air quality monitoring station data with pollutant measurements.

**Key Columns**:
- Air Pollutant, Air Pollutant Description
- Data Aggregation Process
- Year (datetime)
- Air Pollution Level, Unit Of Air Pollution Level
- Air Quality Station Type, Air Quality Station Area
- Latitude, Longitude, Altitude
- Province

**Processing**:
- Invalid province values ("nan", "Desconocido", "Error") converted to NaN
- Air quality classification using WHO/EU thresholds
- Province name standardization

### 2. Health Data
**Location**: 
- `src/etl_pipeline/data/health_data/raw/enfermedades_respiratorias.csv`
- `src/etl_pipeline/data/health_data/raw/esperanza_vida.csv`

**Description**: Health outcome statistics including respiratory disease mortality and life expectancy data.

**Processing**:
- Spanish CSV format handling (semicolon separators, comma decimals)
- Column renaming to English standards
- Date parsing for temporal analysis
- Multiple encoding support (latin1)

### 3. Socioeconomic Data
**Location**:
- `src/etl_pipeline/data/socioeconomic_data/raw/PIB per cap provincias 2000-2021.csv`
- `src/etl_pipeline/data/socioeconomic_data/raw/poblacion_provincias.csv`

**Description**: Economic and demographic data at provincial level.

**Processing**:
- GDP data transformation from wide to long format
- Population data cleaning and type conversion
- Multiple encoding support (ISO-8859-1, latin1)
- Province name standardization

## Pipeline Components

### Extract Layer

#### DataExtractionStep
**File**: `extract/data_extraction_step.py`

**Purpose**: Orchestrates all data extraction operations.

**Process**:
1. Validates execution context
2. Sequentially executes specialized extractors
3. Aggregates results into shared dataframes dictionary
4. Reports extraction statistics

**Output**: 5 DataFrames (air_quality, respiratory_diseases, life_expectancy, gdp, province_population)

#### Specialized Extractors

**AirQualityDataExtractor**:
- Processes air quality monitoring data
- Selective column loading (12 predefined columns)
- Date parsing and data validation

**HealthDataExtractor**:
- Extracts respiratory disease and life expectancy data
- Handles Spanish CSV format (semicolon separators)
- Dual dataset processing in single operation

**SocioeconomicDataExtractor**:
- Processes GDP and population data
- Multiple encoding support
- Robust file validation

### Transform Layer

#### DataTransformationStep
**File**: `transform/data_transformation_step.py`

**Purpose**: Coordinates domain-specific transformations.

**Process**:
1. Air quality data transformation and classification
2. Health data cleaning and standardization
3. Socioeconomic data restructuring
4. Province name standardization across all datasets

#### DataMergingStep
**File**: `transform/data_merging_step.py`

**Purpose**: Combines all transformed datasets into unified structure.

**Process**:
1. Validates merge keys (Province, Year) across all datasets
2. Sequential left joins preserving air quality as base
3. Column cleanup and deduplication
4. Creates single integrated dataset

#### FeatureEngineeringStep
**File**: `transform/feature_engineering_step.py`

**Purpose**: Creates derived features for analysis.

**Current Features**:
- Respiratory deaths per 100k population
- Additional features can be easily added

#### DataCleaningStep
**File**: `transform/data_cleaning_step.py`

**Purpose**: Comprehensive data preprocessing and quality improvement.

**Process**:
1. Removes data from excluded regions (islands, autonomous cities)
2. Filters data to specified time range (2000-2021)
3. Handles null values (removes columns with <5% nulls, flags others)
4. Removes duplicate records
5. Applies data type conversions from configuration

#### DataValidationStep
**File**: `transform/data_validation_step.py`

**Purpose**: Comprehensive data quality validation with recovery capabilities.

**Validation Types**:
- **Basic**: Empty DataFrame, null percentages, data types, duplicates
- **Enhanced**: Required columns, business rules, statistical anomalies
- **Flexible**: Configuration-driven vs. strict legacy mode

**Recovery Features**:
- Distinguishes errors (pipeline stops) from warnings (logged, continues)
- Configurable validation thresholds
- Statistical anomaly detection using IQR method

### Load Layer

#### DataExportStep
**File**: `load/data_export_step.py`

**Purpose**: Exports processed dataset in multiple formats.

**Supported Formats**:
- CSV (primary format)
- Parquet (optional)

**Process**:
1. Validates output DataFrame presence and completeness
2. Creates output directory structure
3. Exports to configured formats
4. Updates context with file paths

#### DataQualityReportStep
**File**: `load/data_quality_report_step.py`

**Purpose**: Generates comprehensive data quality report.

**Report Contents**:
- Dataset statistics (rows, columns, memory usage)
- Missing data analysis
- Data type distribution
- Numeric column summaries
- Categorical column analysis
- Duplicate detection results

**Output**: JSON report saved to `output/reports/data_quality_report.json`

## Configuration

### Configuration Management
**File**: `config/config_manager.py`

**Features**:
- YAML-based configuration with environment-specific overrides
- Dot-notation access to nested configuration values
- Configuration validation with comprehensive error checking
- Global singleton pattern for consistent access

### Primary Configuration File
**File**: `config/pipeline_config.yaml`

**Sections**:
- **Pipeline**: Step definitions and execution order
- **Data Sources**: File paths and column specifications
- **Processing**: Time ranges, quality thresholds, excluded regions
- **Output**: Export formats and file locations
- **Validation**: Required columns, data types, business rules
- **Logging**: Log levels, formats, and file management

### Feature Types Configuration
**File**: `config/feature_types.json`

Defines data types for all columns in the final dataset, used by:
- Data cleaning step for type conversion
- Validation step for type checking
- ML pipeline for proper data handling

## Data Flow

### Complete Pipeline Flow

1. **Initialization**
   - Project structure validation
   - Configuration loading
   - Logger setup

2. **Extraction Phase** (5 DataFrames created)
   - Air quality data extraction
   - Health data extraction (respiratory diseases, life expectancy)
   - Socioeconomic data extraction (GDP, population)

3. **Transformation Phase**
   - Domain-specific transformations
   - Data merging on Province + Year
   - Feature engineering
   - Data cleaning and filtering
   - Comprehensive validation

4. **Loading Phase**
   - Dataset export (CSV/Parquet)
   - Quality report generation
   - Context updates for downstream processes

### Data Integration Strategy

**Province Name Standardization**:
- Central mapping file: `utils/unified_province_name.json`
- Applied consistently across all data sources
- Enables reliable cross-dataset merging

**Temporal Alignment**:
- All datasets aligned on annual basis (2000-2021)
- Date parsing and standardization
- Missing year handling through left joins

## Usage

### Running the Pipeline

```bash
# Install dependencies
pip install -r requirements.txt

# Run complete ETL pipeline
python src/etl_pipeline/main_orchestrator.py

# Run individual components (for debugging)
python -m pytest src/etl_pipeline/tests/test_main_orchestrator.py
```

### Output Files

After successful execution:
- **Dataset**: `src/etl_pipeline/data/output/dataset.csv`
- **Quality Report**: `src/etl_pipeline/data/output/reports/data_quality_report.json`
- **Logs**: `src/etl_pipeline/logs/featuring_YYYY-MM-DD_HH-MM-SS.log`

### Integration with ML Pipeline

The ETL pipeline produces `dataset.csv` which serves as input to the ML pipeline:

```bash
# ML pipeline uses ETL output
python src/modeling/preprocess.py params.yaml src/etl_pipeline/data/output/dataset.csv ...
```

## Testing

### Test Structure
**Location**: `src/etl_pipeline/tests/`

**Test Categories**:
- **Extract Tests**: Data extraction step and individual extractors
- **Transform Tests**: All transformation steps and transformers
- **Load Tests**: Export and reporting functionality
- **Utils Tests**: Province mapping and project structure validation
- **Integration Tests**: End-to-end pipeline execution

### Running Tests

```bash
# Run all ETL tests
pytest src/etl_pipeline/tests/

# Run specific test category
pytest src/etl_pipeline/tests/extract_tests/
pytest src/etl_pipeline/tests/transform_tests/
pytest src/etl_pipeline/tests/load_tests/

# Run with coverage
pytest src/etl_pipeline/tests/ --cov=src/etl_pipeline
```

### Test Configuration
**File**: `pytest.ini`
- Includes src paths for proper module resolution
- Configures test discovery patterns
- Sets up logging for test execution

## Troubleshooting

### Common Issues

**1. Missing Data Files**
```
FileNotFoundError: Raw data file not found
```
**Solution**: Ensure all raw data files are present in their respective directories.

**2. Province Name Mismatches**
```
ValueError: Province names not standardized
```
**Solution**: Update `unified_province_name.json` with new province name mappings.

**3. Configuration Errors**
```
ConfigurationError: Missing required configuration section
```
**Solution**: Verify `pipeline_config.yaml` contains all required sections.

**4. Memory Issues**
```
MemoryError: Unable to allocate memory for DataFrame
```
**Solution**: Process data in chunks or increase available memory.

### Error Recovery

The pipeline includes built-in recovery mechanisms:

**Validation Warnings**: Continue execution with logged warnings
**Data Type Mismatches**: Attempt automatic type coercion
**Missing Optional Data**: Continue with available data

### Logging and Monitoring

**Log Levels**:
- **INFO**: Normal pipeline progress
- **WARNING**: Non-fatal issues that don't stop execution
- **ERROR**: Fatal issues that require intervention

**Log Locations**:
- Console output for real-time monitoring
- File logs for persistent debugging: `logs/featuring_YYYY-MM-DD_HH-MM-SS.log`

**Performance Monitoring**:
- Execution time tracking
- Memory usage logging
- Data quality metrics
- Step-by-step progress reporting

### Performance Optimization

**Data Loading**: Only load required columns to reduce memory usage
**Type Conversion**: Apply data types early to optimize memory
**Chunked Processing**: Process large datasets in manageable chunks
**Parallel Processing**: Consider parallelizing independent transformation steps

---

This ETL pipeline provides a robust, scalable foundation for air quality and health data analysis, with comprehensive error handling, logging, and quality assurance throughout the entire data processing workflow.