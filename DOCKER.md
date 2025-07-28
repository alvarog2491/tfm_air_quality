# Docker Configuration

This document describes the containerized setup for the Air Quality Analysis Pipeline.

## 📁 Project Structure

```
tfm_air_quality/
├── docker-compose.yml          # Orchestration configuration
├── docker-run.sh               # Convenience runner script
├── .dockerignore               # Docker ignore file
├── src/
│   ├── etl_pipeline/
│   │   ├── Dockerfile          # ETL service container
│   │   └── requirements.txt    # ETL dependencies
│   ├── modeling/
│   │   ├── Dockerfile          # Modeling service container
│   │   └── requirements.txt    # Modeling dependencies
│   └── app/
│       ├── Dockerfile          # Flask app container
│       └── requirements.txt    # App dependencies
```

## 🚀 Usage

### Quick Start
```bash
# Run complete pipeline
./docker-run.sh full

# Run individual services
./docker-run.sh etl       # ETL only
./docker-run.sh modeling  # Modeling only
./docker-run.sh app       # App only
```

### Using Docker Compose Directly
```bash
# Build all services
docker-compose build

# Run complete pipeline
docker-compose --profile full up

# Run individual services
docker-compose --profile etl up
docker-compose --profile modeling up
docker-compose --profile app up
```

## 🔧 Service Details

### ETL Pipeline (`src/etl_pipeline/`)
- **Container**: ETL data processing pipeline
- **Dependencies**: pandas, numpy, PyYAML
- **Input**: Raw data files
- **Output**: Processed dataset for modeling

### Modeling Pipeline (`src/modeling/`)
- **Container**: ML model training and evaluation
- **Dependencies**: scikit-learn, DVC, joblib
- **Input**: Processed data from ETL
- **Output**: Trained models and metrics

### Flask Application (`src/app/`)
- **Container**: Web service for model inference
- **Dependencies**: Flask, scikit-learn, joblib
- **Input**: Trained models from modeling pipeline
- **Output**: HTTP API for predictions

## 📦 Shared Volumes

The services share data through Docker volumes:

- **ETL → Modeling**: Processed datasets
- **Modeling → App**: Trained model artifacts
- **All Services**: Logs directory

## 🔍 Monitoring

- **Logs**: `./docker-run.sh logs --follow`
- **Status**: `./docker-run.sh status`
- **Health**: Each container includes health checks