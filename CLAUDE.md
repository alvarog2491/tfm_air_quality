# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Key Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run ETL pipeline (data processing)
python src/etl_pipeline/main_orchestrator.py

# Run ML pipeline with DVC (preprocessing, training, evaluation)
dvc repro

# Run individual ML stages
python src/modeling/preprocess.py params.yaml src/etl_pipeline/data/output/dataset.csv data/processed_training_data.csv data/processed_test_data.csv
python src/modeling/train.py params.yaml data/processed_training_data.csv models/model.pkl
python src/modeling/evaluate.py params.yaml models/model.pkl data/processed_test_data.csv metrics/evaluation.json

# Run tests
pytest

# Start web application for model serving
python src/app/app.py
```

## Architecture Overview

This is a master's thesis project analyzing air quality, health outcomes, and socioeconomic factors across Spanish provinces using machine learning. The codebase follows a clear separation between data processing (ETL) and machine learning pipelines.

### Core Components

**ETL Pipeline (`src/etl_pipeline/`)**
- Modular pipeline with Extract/Transform/Load phases
- Main orchestrator: `src/etl_pipeline/main_orchestrator.py`
- Three data sources: air quality, health (respiratory diseases, life expectancy), socioeconomic (GDP, population)
- Data extractors, transformers, and reporters follow base class patterns
- Province name standardization via `unified_province_name.json` mapping file
- Comprehensive logging and data quality reporting

**ML Pipeline (`src/modeling/`)**
- DVC-managed pipeline defined in `dvc.yaml` with stages: preprocess → train → evaluate
- Configuration driven via `params.yaml` with YAML anchors for shared parameters
- Base model class pattern for different algorithms (linear regression, random forest)
- Model registry system for extensible model management
- Configurable metrics and hyperparameters via `params.yaml`
- Preprocessing handles feature selection, categorical encoding, train/test splits
- Model evaluation generates metrics to `metrics/evaluation.json`
- Scaler persistence for consistent data preprocessing

**Web Application (`src/app/`)**
- Flask application for model serving
- Model service layer for predictions
- Serves trained models from `models/` directory

### Data Flow

1. Raw data in `src/etl_pipeline/data/raw/` (air quality, health, socioeconomic)
2. ETL pipeline processes to `src/etl_pipeline/data/output/dataset.csv`
3. ML preprocessing creates train/test splits in `data/`
4. Models trained and saved to `models/`
5. Evaluation metrics saved to `metrics/`

### Testing

- Test configuration in `pytest.ini` with src paths included
- Test files organized alongside source code in respective modules
- Run all tests with `pytest` from project root

### Key Files

- `params.yaml`: ML pipeline configuration with feature definitions and model parameters
- `unified_province_name.json`: Province name standardization mapping
- `dvc.yaml`: ML pipeline stage definitions
- `pytest.ini`: Test runner configuration

### Development Notes

- Uses DVC for ML experiment tracking and pipeline versioning
- Extensive logging in both ETL and modeling components
- Data quality validation and reporting throughout pipeline
- Modular design with base classes for extensibility