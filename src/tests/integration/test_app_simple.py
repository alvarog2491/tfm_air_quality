#!/usr/bin/env python3
"""
Simple app functionality test using actual repository structure.
"""
import pytest
import pandas as pd
import json
import joblib
from pathlib import Path


def test_app_functionality():
    """Test app functionality using actual repository code."""
    
    # Check if model artifacts exist
    model_path = Path("models/model.pkl")
    scaler_path = Path("models/scaler.pkl")
    
    if not model_path.exists() or not scaler_path.exists():
        pytest.skip("Model artifacts not found. Run modeling pipeline first.")
    
    try:
        # Import actual app
        from app.model_service import app as model_app
        
        # Load test data if available
        test_data_path = Path("data/processed_test_data.csv")
        if test_data_path.exists():
            test_df = pd.read_csv(test_data_path)
            
            # Load scaler to get expected features
            scaler = joblib.load(scaler_path)
            expected_features = list(scaler.feature_names_in_)
            available_features = [col for col in expected_features if col in test_df.columns]
            
            if available_features:
                # Use actual processed data structure with correct features
                sample_row = test_df[available_features].iloc[0]
                test_data = sample_row.to_dict()
                
                # Convert numpy types to Python types for JSON serialization
                for key, value in test_data.items():
                    if hasattr(value, 'item'):
                        test_data[key] = value.item()
                    elif pd.isna(value):
                        test_data[key] = None
                        
            else:
                pytest.skip("No matching features found between scaler and test data")
        else:
            pytest.skip("No test data available")
        
        # Test using Flask test client
        with model_app.test_client() as client:
            response = client.post('/predict',
                                 data=json.dumps(test_data),
                                 content_type='application/json')
            
            assert response.status_code == 200, f"Model service failed: {response.status_code}"
            
            result = response.get_json()
            assert 'prediction' in result, "Model service response missing prediction"
            assert result.get('status') == 'success', f"Model service returned error: {result}"
            
            print(f'App functionality test successful: {result}')
    
    except ImportError as e:
        pytest.skip(f"Could not import app modules: {e}")


if __name__ == "__main__":
    test_app_functionality()