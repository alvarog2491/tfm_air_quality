# Installation Guide

This guide will walk you through setting up the ETL Pipeline on your local development environment.

## Prerequisites

Before installing the ETL Pipeline, ensure you have the following:

- **Python 3.8+** installed on your system
- **Git** for cloning the repository
- **pip** package manager
- At least **4GB of available RAM**
- **2GB of free disk space**

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/tfm_air_quality.git
cd tfm_air_quality
```

### 2. Create a Virtual Environment

It's highly recommended to use a virtual environment to isolate dependencies:

=== "Using venv"
    ```bash
    python -m venv etl_env
    source etl_env/bin/activate  # On Windows: etl_env\Scripts\activate
    ```

=== "Using conda"
    ```bash
    conda create -n etl_env python=3.8
    conda activate etl_env
    ```

### 3. Install Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 4. Verify Data Structure

Ensure the data directories exist and contain the required files:

```bash
python -c "from src.etl_pipeline.utils import CheckProjectStructure; CheckProjectStructure().execute()"
```

This should create the necessary directory structure if it doesn't exist:

```
src/etl_pipeline/data/
├── air_quality_data/
│   └── raw/
│       └── air_quality_with_province.csv
├── health_data/
│   └── raw/
│       ├── enfermedades_respiratorias.csv
│       └── esperanza_vida.csv
├── socioeconomic_data/
│   └── raw/
│       ├── PIB per cap provincias 2000-2021.csv
│       └── poblacion_provincias.csv
└── output/
    └── reports/
```

## Data Setup

### Required Data Files

You need to place the following CSV files in their respective directories:

1. **Air Quality Data**: `src/etl_pipeline/data/air_quality_data/raw/air_quality_with_province.csv`
2. **Respiratory Diseases**: `src/etl_pipeline/data/health_data/raw/enfermedades_respiratorias.csv`
3. **Life Expectancy**: `src/etl_pipeline/data/health_data/raw/esperanza_vida.csv`
4. **GDP Data**: `src/etl_pipeline/data/socioeconomic_data/raw/PIB per cap provincias 2000-2021.csv`
5. **Population Data**: `src/etl_pipeline/data/socioeconomic_data/raw/poblacion_provincias.csv`

### Data Sources

The data files should contain Spanish provincial data for the period 2000-2021:

- **Air Quality**: Monitoring station measurements for SO2, PM2.5, PM10, O3, NO2
- **Health**: Respiratory disease mortality and life expectancy statistics
- **Socioeconomic**: GDP per capita and population data by province

## Configuration Setup

### 1. Pipeline Configuration

The default configuration file `src/etl_pipeline/config/pipeline_config.yaml` should work out of the box. However, you can customize:

- **Time Range**: Adjust `processing.time_range.start_year` and `end_year`
- **Quality Thresholds**: Modify `processing.data_quality.null_threshold_percent`
- **Excluded Regions**: Update `processing.excluded_regions` list
- **Output Formats**: Configure `output.formats` for different export types

### 2. Feature Types Configuration

The file `src/etl_pipeline/config/feature_types.json` defines data types for the final dataset. This is automatically used by the pipeline but can be customized for specific analysis needs.

### 3. Environment Variables

Optional environment variables you can set:

```bash
export ETL_ENV=development  # Environment name (development, production, testing)
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"  # Add src to Python path
```

## Verification

### 1. Test Installation

Run the test suite to verify everything is working correctly:

```bash
# Run all tests
pytest src/etl_pipeline/tests/

# Run specific test categories
pytest src/etl_pipeline/tests/extract_tests/
pytest src/etl_pipeline/tests/transform_tests/
pytest src/etl_pipeline/tests/load_tests/
```

### 2. Configuration Test

Validate your configuration:

```bash
python -c "from src.etl_pipeline.config.config_manager import get_config; config = get_config(); print('Configuration loaded successfully')"
```

### 3. Quick Pipeline Test

Run a minimal pipeline test:

```bash
python -c "
from src.etl_pipeline.main_orchestrator import ETLPipeline
from src.etl_pipeline.utils import CheckProjectStructure
import logging
logging.basicConfig(level=logging.INFO)
try:
    data_path = CheckProjectStructure().execute()
    print(f'Project structure validated: {data_path}')
    print('Installation verified successfully!')
except Exception as e:
    print(f'Installation issue: {e}')
"
```

## Common Installation Issues

### Python Path Issues

If you encounter import errors:

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

Or add this to your shell profile (`.bashrc`, `.zshrc`, etc.).

### Missing Data Files

If you get `FileNotFoundError` for data files:

1. Ensure all required CSV files are in the correct directories
2. Check file names match exactly (case-sensitive)
3. Verify file permissions are readable

### Dependencies Issues

If package installation fails:

```bash
# Upgrade pip first
pip install --upgrade pip

# Install with specific Python version
python3.8 -m pip install -r requirements.txt

# For conda users
conda install --file requirements.txt
```

### Memory Issues

If you encounter memory issues during processing:

1. Ensure you have at least 4GB available RAM
2. Close other memory-intensive applications
3. Consider processing data in smaller chunks

## Docker Installation (Optional)

For containerized deployment:

### 1. Build Docker Image

```bash
docker build -t etl-pipeline .
```

### 2. Run Container

```bash
docker run -v $(pwd)/src/etl_pipeline/data:/app/data etl-pipeline
```

### 3. Development Mode

```bash
docker run -it -v $(pwd):/app etl-pipeline bash
```

## Development Setup

For development work, install additional tools:

```bash
# Install development dependencies
pip install pytest pytest-cov black flake8 mypy

# Install pre-commit hooks (optional)
pip install pre-commit
pre-commit install
```

## Next Steps

Once installation is complete:

1. **[Quick Start](quickstart.md)**: Run your first pipeline execution
2. **[Configuration](../configuration/config-manager.md)**: Customize the pipeline settings
3. **[Architecture](../architecture/overview.md)**: Understand the system design

## Getting Help

If you encounter issues during installation:

1. Check the [Troubleshooting Guide](../troubleshooting/common-issues.md)
2. Review the log files in `src/etl_pipeline/logs/`
3. Ensure all prerequisites are met
4. Verify data files are present and correctly formatted

The installation should now be complete and ready for your first pipeline run!