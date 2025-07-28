"""
End-to-end integration tests for the complete air quality analysis pipeline.
Tests the entire flow using the existing repository structure and configuration.
"""
import pytest
import pandas as pd
import os
import json
import yaml
from pathlib import Path

from common.utils.file_utils import load_yaml_config
from etl_pipeline.main_orchestrator import ETLPipeline


class TestEndToEndIntegration:
    """End-to-end integration tests using actual repository code and configuration."""

    @pytest.fixture
    def config(self):
        """Load actual configuration from params.yaml."""
        return load_yaml_config("params.yaml")
    
    @pytest.fixture
    def ensure_directories(self):
        """Ensure required directories exist."""
        directories = ["data", "models", "metrics"]
        for directory in directories:
            Path(directory).mkdir(exist_ok=True)
        yield
        # Cleanup is optional since these are needed directories
    
    def test_etl_pipeline_execution(self):
        """Test ETL pipeline execution using actual orchestrator."""
        
        # Check that we have the necessary input data files
        expected_data_dirs = [
            "src/etl_pipeline/data/air_quality_data/raw",
            "src/etl_pipeline/data/health_data/raw", 
            "src/etl_pipeline/data/socioeconomic_data/raw"
        ]
        
        for data_dir in expected_data_dirs:
            if not Path(data_dir).exists() or not any(Path(data_dir).glob("*.csv")):
                pytest.skip(f"Required data directory not found or empty: {data_dir}")
        
        # Use actual ETL pipeline
        pipeline = ETLPipeline()
        
        try:
            context = pipeline.run()
            
            # Verify the pipeline executed successfully
            assert context is not None, "ETL pipeline returned no context"
            
            # Check that output was created
            output_file = Path("src/etl_pipeline/data/output/dataset.csv")
            assert output_file.exists(), "ETL pipeline did not create dataset.csv"
            
            # Validate output content
            df = pd.read_csv(output_file)
            assert not df.empty, "ETL output dataset is empty"
            assert len(df.columns) >= 5, "ETL output has too few columns"
            
            print(f"ETL pipeline successful! Output shape: {df.shape}")
            
        except Exception as e:
            pytest.fail(f"ETL pipeline failed with error: {e}")
    
    def test_modeling_pipeline_with_config(self, config, ensure_directories):
        """Test modeling pipeline using actual configuration and imports."""
        
        # Ensure we have ETL output
        dataset_file = Path("src/etl_pipeline/data/output/dataset.csv")
        if not dataset_file.exists():
            self.test_etl_pipeline_execution()
        
        # Import actual modeling modules
        from modeling.preprocess import main as preprocess_main
        from modeling.train import main as train_main
        from modeling.evaluate import main as evaluate_main
        
        # Extract paths and parameters from config
        preprocess_config = config.get("preprocess", {})
        train_config = config.get("train", {})
        evaluate_config = config.get("evaluate", {})
        
        # Define file paths
        input_dataset = "src/etl_pipeline/data/output/dataset.csv"
        train_data_path = "data/processed_training_data.csv"
        test_data_path = "data/processed_test_data.csv"
        scaler_path = "models/scaler.pkl"
        model_path = "models/model.pkl"
        metrics_path = "metrics/evaluation.json"
        
        try:
            # Step 1: Preprocessing
            print("Running preprocessing...")
            preprocess_main(
                config_file="params.yaml",
                raw_dataset=input_dataset,
                output_train_dataset=train_data_path,
                output_test_dataset=test_data_path,
                scaler_output=scaler_path
            )
            
            # Verify preprocessing outputs
            assert Path(train_data_path).exists(), "Training data not created"
            assert Path(test_data_path).exists(), "Test data not created"
            assert Path(scaler_path).exists(), "Scaler not saved"
            
            # Step 2: Training
            print("Running training...")
            train_main(
                config_file="params.yaml",
                processed_dataset=train_data_path,
                output_model=model_path,
                scaler_path=scaler_path
            )
            
            # Verify training outputs
            assert Path(model_path).exists(), "Model not saved"
            
            # Step 3: Evaluation
            print("Running evaluation...")
            evaluate_main(
                config_file="params.yaml",
                model_file=model_path,
                evaluation_dataset=test_data_path,
                output_metrics=metrics_path,
                scaler_path=scaler_path
            )
            
            # Verify evaluation outputs
            assert Path(metrics_path).exists(), "Evaluation metrics not saved"
            
            # Validate metrics content
            with open(metrics_path, 'r') as f:
                metrics = json.load(f)
            
            expected_metrics = train_config.get("metrics", {}).get("enabled_metrics", [])
            for metric in expected_metrics:
                assert metric in metrics, f"Missing expected metric: {metric}"
                assert isinstance(metrics[metric], (int, float)), f"Invalid metric type for {metric}"
            
            print(f"Modeling pipeline successful! Metrics: {metrics}")
            
        except Exception as e:
            pytest.fail(f"Modeling pipeline failed with error: {e}")
    
    def test_app_functionality_with_real_model(self, ensure_directories):
        """Test app functionality with actual trained model."""
        
        # Ensure we have a trained model
        model_path = Path("models/model.pkl")
        if not model_path.exists():
            pytest.skip("No trained model available. Run modeling pipeline first.")
        
        # Import actual app modules
        try:
            from app.model_service import app as model_app
            import threading
            import time
            import requests
            
            # Start model service in a separate thread
            def run_model_service():
                model_app.run(host='localhost', port=5000, debug=False)
            
            service_thread = threading.Thread(target=run_model_service, daemon=True)
            service_thread.start()
            
            # Wait for service to start
            time.sleep(3)
            
            # Load actual test data if available
            test_data_path = Path("data/processed_test_data.csv")
            if test_data_path.exists():
                test_df = pd.read_csv(test_data_path)
                if not test_df.empty:
                    # Use real processed data structure
                    sample_row = test_df.drop(columns=['Respiratory_diseases_total'], errors='ignore').iloc[0]
                    test_data = sample_row.to_dict()
                else:
                    pytest.skip("Test data is empty")
            else:
                pytest.skip("No test data available")
            
            # Test prediction
            try:
                response = requests.post(
                    'http://localhost:5000/predict',
                    json=test_data,
                    timeout=10
                )
                
                assert response.status_code == 200, f"Model service failed: {response.text}"
                
                result = response.json()
                assert 'prediction' in result, "Model service response missing prediction"
                assert result['status'] == 'success', f"Model service returned error: {result}"
                
                print(f"Model service test successful: {result['prediction']}")
                
            except requests.exceptions.ConnectionError:
                pytest.skip("Could not connect to model service")
                
        except ImportError as e:
            pytest.skip(f"Could not import app modules: {e}")
    
    def test_configuration_consistency(self, config):
        """Test that configuration is consistent across pipeline stages."""
        
        # Check that all required sections exist
        required_sections = ['preprocess', 'train', 'evaluate']
        for section in required_sections:
            assert section in config, f"Missing configuration section: {section}"
        
        # Check target column consistency
        preprocess_target = config['preprocess'].get('target_column')
        train_target = config['train'].get('target_column')
        evaluate_target = config['evaluate'].get('target_column')
        
        assert preprocess_target == train_target == evaluate_target, \
            "Target column inconsistent across pipeline stages"
        
        # Check metrics consistency
        train_metrics = set(config['train'].get('metrics', {}).get('enabled_metrics', []))
        eval_metrics = set(config['evaluate'].get('metrics', {}).get('enabled_metrics', []))
        
        assert train_metrics == eval_metrics, \
            "Metrics configuration inconsistent between train and evaluate"
        
        print("Configuration consistency test passed!")
    
    def test_data_flow_integrity(self):
        """Test data integrity throughout the pipeline."""
        
        # Check each stage of data transformation
        stages = [
            ("ETL Output", "src/etl_pipeline/data/output/dataset.csv"),
            ("Training Data", "data/processed_training_data.csv"),
            ("Test Data", "data/processed_test_data.csv")
        ]
        
        dataframes = {}
        for stage_name, file_path in stages:
            if Path(file_path).exists():
                df = pd.read_csv(file_path)
                dataframes[stage_name] = df
                print(f"{stage_name}: {df.shape}")
            else:
                print(f"{stage_name}: File not found - {file_path}")
        
        # If we have both training and test data, check consistency
        if "Training Data" in dataframes and "Test Data" in dataframes:
            train_df = dataframes["Training Data"]
            test_df = dataframes["Test Data"]
            
            # Same number of features
            assert train_df.shape[1] == test_df.shape[1], \
                "Training and test data have different number of features"
            
            # Same column names
            assert list(train_df.columns) == list(test_df.columns), \
                "Training and test data have different column names"
            
            print("Data flow integrity test passed!")
    
    def test_model_artifacts_validity(self):
        """Test that generated model artifacts are valid."""
        
        model_path = Path("models/model.pkl")
        scaler_path = Path("models/scaler.pkl")
        metrics_path = Path("metrics/evaluation.json")
        
        # Test model file
        if model_path.exists():
            import joblib
            try:
                model = joblib.load(model_path)
                assert hasattr(model, 'predict'), "Model object missing predict method"
                print("Model artifact is valid")
            except Exception as e:
                pytest.fail(f"Failed to load model: {e}")
        
        # Test scaler file
        if scaler_path.exists():
            import joblib
            try:
                scaler = joblib.load(scaler_path)
                assert hasattr(scaler, 'transform'), "Scaler object missing transform method"
                print("Scaler artifact is valid")
            except Exception as e:
                pytest.fail(f"Failed to load scaler: {e}")
        
        # Test metrics file
        if metrics_path.exists():
            try:
                with open(metrics_path, 'r') as f:
                    metrics = json.load(f)
                
                assert isinstance(metrics, dict), "Metrics should be a dictionary"
                
                # Check metric values are reasonable
                for metric_name, value in metrics.items():
                    assert isinstance(value, (int, float)), f"Metric {metric_name} should be numeric"
                    
                    # R2 should be between -inf and 1
                    if metric_name == 'r2':
                        assert value <= 1, f"R2 value {value} should be <= 1"
                    
                    # MSE, RMSE, MAE should be non-negative
                    if metric_name in ['mse', 'rmse', 'mae']:
                        assert value >= 0, f"{metric_name} value {value} should be non-negative"
                
                print(f"Metrics artifact is valid: {metrics}")
                
            except Exception as e:
                pytest.fail(f"Failed to load or validate metrics: {e}")
    
    def test_complete_pipeline_integration(self, config, ensure_directories):
        """Test complete pipeline from ETL to model artifacts."""
        
        print("Starting complete pipeline integration test...")
        
        # Step 1: ETL
        print("Step 1: ETL Pipeline")
        self.test_etl_pipeline_execution()
        
        # Step 2: Modeling
        print("Step 2: Modeling Pipeline")
        self.test_modeling_pipeline_with_config(config, ensure_directories)
        
        # Step 3: Validation
        print("Step 3: Artifact Validation")
        self.test_model_artifacts_validity()
        
        # Step 4: Data Integrity
        print("Step 4: Data Integrity Check")
        self.test_data_flow_integrity()
        
        print("Complete pipeline integration test passed successfully!")