#!/usr/bin/env python3
"""
Script to verify modeling pipeline output for CI.
"""
import json
import joblib
from pathlib import Path

from common.utils.file_utils import load_yaml_config


def verify_modeling_output():
    """Verify modeling pipeline produced valid outputs using actual configuration."""
    
    # Check if model was created
    if not Path("models/model.pkl").exists():
        raise FileNotFoundError('Model file not found')
    
    # Check if scaler was created
    if not Path("models/scaler.pkl").exists():
        raise FileNotFoundError('Scaler file not found')
    
    # Check if metrics were generated
    if not Path("metrics/evaluation.json").exists():
        raise FileNotFoundError('Evaluation metrics not found')
    
    # Load and validate metrics using actual configuration
    with open("metrics/evaluation.json", 'r') as f:
        metrics = json.load(f)
    
    try:
        config = load_yaml_config("params.yaml")
        expected_metrics = config.get('train', {}).get('metrics', {}).get('enabled_metrics', [])
        
        if expected_metrics:
            for metric in expected_metrics:
                if metric not in metrics:
                    raise KeyError(f'Missing expected metric: {metric}')
        else:
            # Fallback to common metrics
            required_metrics = ['mse', 'rmse', 'mae', 'r2']
            for metric in required_metrics:
                if metric not in metrics:
                    raise KeyError(f'Missing metric: {metric}')
                    
    except Exception as e:
        print(f'Could not load configuration: {e}')
        # Basic validation
        if not isinstance(metrics, dict) or len(metrics) == 0:
            raise ValueError('Invalid metrics format')
    
    # Validate model can be loaded
    try:
        model = joblib.load("models/model.pkl")
        if not hasattr(model, 'predict'):
            raise ValueError('Model object missing predict method')
    except Exception as e:
        raise ValueError(f'Failed to load or validate model: {e}')
    
    # Validate scaler can be loaded
    try:
        scaler = joblib.load("models/scaler.pkl")
        if not hasattr(scaler, 'transform'):
            raise ValueError('Scaler object missing transform method')
    except Exception as e:
        raise ValueError(f'Failed to load or validate scaler: {e}')
    
    print('Modeling pipeline executed successfully!')
    print(f'Model metrics: {metrics}')


if __name__ == "__main__":
    verify_modeling_output()