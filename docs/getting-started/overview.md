# Overview

The Air Quality ETL Pipeline is a comprehensive data processing system designed to integrate and analyze environmental, health, and socioeconomic data across Spanish provinces. This guide will help you understand the system's purpose, capabilities, and how to get started.

## What is the ETL Pipeline?

The ETL (Extract, Transform, Load) pipeline is a data processing system that:

- **Extracts** data from multiple heterogeneous sources
- **Transforms** and standardizes the data for analysis
- **Loads** the processed data into a unified dataset

This pipeline specifically focuses on Spanish provincial data spanning from 2000 to 2021, integrating:

- Air quality measurements from monitoring stations
- Health outcome statistics (respiratory diseases, life expectancy)
- Socioeconomic indicators (GDP per capita, population)

## System Requirements

### Software Requirements

- **Python**: 3.8 or higher
- **Operating System**: Linux, macOS, or Windows
- **Memory**: Minimum 4GB RAM (8GB recommended)
- **Disk Space**: At least 2GB for data and logs

### Python Dependencies

The pipeline requires several Python packages that are automatically installed via `requirements.txt`:

- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **pyyaml**: YAML configuration file parsing
- **pathlib**: Path manipulation utilities
- **logging**: Logging framework

### Optional Dependencies

For enhanced functionality:

- **matplotlib/seaborn**: Data visualization (for reports)
- **pytest**: Testing framework
- **docker**: Containerization support

## Project Structure

Understanding the project structure is crucial for working with the pipeline:

```
tfm_air_quality/
├── src/
│   ├── etl_pipeline/           # Main ETL pipeline code
│   │   ├── main_orchestrator.py   # Pipeline entry point
│   │   ├── extract/               # Data extraction components
│   │   ├── transform/             # Data transformation components
│   │   ├── load/                  # Data loading components
│   │   ├── config/                # Configuration management
│   │   ├── utils/                 # Utility functions
│   │   ├── data/                  # Data directories
│   │   └── tests/                 # Test suite
│   ├── modeling/               # ML pipeline (separate)
│   └── app/                   # Web application
├── docs/                      # Documentation (MkDocs)
├── requirements.txt           # Python dependencies
├── mkdocs.yml                # Documentation configuration
└── CLAUDE.md                 # Development instructions
```

## Key Concepts

### Data Integration Strategy

The pipeline uses a **province-year** based integration strategy:

- All datasets are merged using `Province` and `Year` as common keys
- Province names are standardized using a mapping file
- Temporal alignment ensures consistent time series analysis

### Quality Assurance

The pipeline implements multiple quality assurance layers:

- **Input Validation**: Checks raw data integrity
- **Transformation Validation**: Ensures data consistency during processing
- **Output Validation**: Verifies final dataset quality
- **Comprehensive Reporting**: Generates detailed quality reports

### Error Recovery

The system includes built-in error recovery mechanisms:

- **Graceful Degradation**: Continues processing when possible
- **Detailed Logging**: Provides comprehensive error information
- **Recovery Strategies**: Implements specific recovery logic for common issues

## Data Flow Overview

The pipeline follows a sequential processing model:

1. **Project Structure Validation**: Ensures all required directories and files exist
2. **Data Extraction**: Loads raw data from CSV files
3. **Data Transformation**: Applies domain-specific transformations
4. **Data Integration**: Merges all datasets on common keys
5. **Feature Engineering**: Creates derived features
6. **Data Cleaning**: Removes outliers and handles missing values
7. **Data Validation**: Performs comprehensive quality checks
8. **Data Export**: Saves the final dataset and generates reports

## Configuration Management

The pipeline uses YAML-based configuration for flexibility:

- **Base Configuration**: `config/pipeline_config.yaml`
- **Environment Overrides**: `config/pipeline_config_{env}.yaml`
- **Feature Types**: `config/feature_types.json`

Configuration covers:

- Data source specifications
- Processing parameters
- Validation rules
- Output formats
- Logging settings

## Next Steps

Once you understand the overview, proceed to:

1. **[Installation](installation.md)**: Set up your development environment
2. **[Quick Start](quickstart.md)**: Run your first pipeline execution
3. **[Architecture](../architecture/overview.md)**: Dive deeper into system design

## Common Use Cases

The ETL pipeline supports various analytical scenarios:

### Environmental Health Research
Researchers can analyze correlations between air pollution levels and health outcomes across different provinces and time periods.

### Policy Impact Analysis
Policy makers can evaluate the effectiveness of environmental regulations by examining trends in air quality and health metrics.

### Predictive Modeling
Data scientists can use the integrated dataset to build machine learning models that predict health outcomes based on environmental and socioeconomic factors.

### Regional Comparisons
Analysts can compare provinces across multiple dimensions to identify patterns and outliers in environmental health relationships.

## Support and Resources

- **Documentation**: Comprehensive guides and API reference
- **Test Suite**: Extensive tests ensure system reliability
- **Logging**: Detailed logs help with troubleshooting
- **Configuration**: Flexible YAML-based configuration system

Ready to get started? Head to the [Installation Guide](installation.md) to set up your environment.