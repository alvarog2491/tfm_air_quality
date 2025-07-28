![Tests Status](https://github.com/alvarog2491/tfm_air_quality/actions/workflows/tests.yml/badge.svg)
# Table of contents

[dataset creation pipeline](src/database)

# Air Quality Analysis Project

This is a master's thesis project analyzing the relationship between air quality, health outcomes, and socioeconomic factors across Spanish provinces using machine learning.

## Project Structure
- `src/etl_pipeline/`: Data extraction, transformation, and loading
- `src/modeling/`: ML models (linear regression, random forest)
- `src/app/`: Flask web application for model serving
- `notebooks/`: Jupyter notebooks for data exploration
- `data/`: Processed training and test datasets
- `models/`: Trained model artifacts

## Key Commands
```bash
# Run ETL pipeline
python src/etl_pipeline/main_orchestrator.py

# Run ML pipeline with DVC
dvc repro

# Run tests
pytest

# Start web application
python src/app/main_app.py

# Install dependencies
pip install -r requirements.txt
```

## Data Sources
- Air quality data by province
- Health data (respiratory diseases, life expectancy)
- Socioeconomic data (GDP, population size)

## Development Notes
- Uses DVC for ML pipeline versioning
- Follows ETL pattern with extract/transform/load steps
- Implements data quality validation and reporting
- Province name standardization via `unified_province_name.json`

## Testing
- Test files located in respective module test directories
- Run with `pytest` from project root

## Key Features
- Multi-source data integration
- Data quality reporting
- Feature engineering for ML models
- Model evaluation metrics
- Web API for predictions