"""
Integration tests for modeling pipeline.
Tests the complete modeling pipeline flow using actual repository structure.
"""
import pytest
import pandas as pd
import os
import json
import joblib
from pathlib import Path

from common.utils.file_utils import load_yaml_config


class TestModelingIntegration:
    """Integration tests for the complete modeling pipeline using actual repository code."""

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
    
    @pytest.fixture
    def ensure_etl_data(self):
        """Ensure ETL output exists for modeling tests."""
        etl_output = Path("src/etl_pipeline/data/output/dataset.csv")
        if not etl_output.exists():
            pytest.skip("ETL output dataset.csv not found. Run ETL pipeline first.")
        yield

    def test_modeling_pipeline_preprocess(self, config, ensure_directories, ensure_etl_data):
        """Test the preprocessing stage of the modeling pipeline."""
        
        # Import actual preprocessing module
        from modeling.preprocess import main as preprocess_main
        
        # Define file paths
        input_dataset = "src/etl_pipeline/data/output/dataset.csv"
        train_data_path = "data/processed_training_data.csv"
        test_data_path = "data/processed_test_data.csv"
        scaler_path = "models/scaler.pkl"
        
        # Clean up any existing files
        for path in [train_data_path, test_data_path, scaler_path]:
            if Path(path).exists():
                Path(path).unlink()
        
        # Run preprocessing
        preprocess_main(
            config_file="params.yaml",
            raw_dataset=input_dataset,
            output_train_dataset=train_data_path,
            output_test_dataset=test_data_path,
            scaler_output=scaler_path
        )
        
        # Verify outputs
        assert Path(train_data_path).exists(), "Training data not created"
        assert Path(test_data_path).exists(), "Test data not created"
        assert Path(scaler_path).exists(), "Scaler not saved"
        
        # Verify data structure
        train_df = pd.read_csv(train_data_path)
        test_df = pd.read_csv(test_data_path)
        
        assert not train_df.empty, "Training data is empty"
        assert not test_df.empty, "Test data is empty"
        assert train_df.shape[1] == test_df.shape[1], "Train and test have different number of features"
        
        # Verify scaler
        scaler = joblib.load(scaler_path)
        assert hasattr(scaler, 'transform'), "Scaler missing transform method"
        
        print(f"Preprocessing successful! Train shape: {train_df.shape}, Test shape: {test_df.shape}")

    def test_modeling_pipeline_train(self, config, ensure_directories):
        """Test the training stage of the modeling pipeline."""
        
        # Ensure preprocessing has been done
        train_data_path = "data/processed_training_data.csv"
        scaler_path = "models/scaler.pkl"
        
        if not Path(train_data_path).exists() or not Path(scaler_path).exists():
            self.test_modeling_pipeline_preprocess(config, ensure_directories, None)
        
        # Import actual training module
        from modeling.train import main as train_main
        
        # Define file paths
        model_path = "models/model.pkl"
        
        # Clean up existing model
        if Path(model_path).exists():
            Path(model_path).unlink()
        
        # Run training
        train_main(
            config_file="params.yaml",
            processed_dataset=train_data_path,
            output_model=model_path,
            scaler_path=scaler_path
        )
        
        # Verify model was created
        assert Path(model_path).exists(), "Model not saved"
        
        # Verify model can be loaded and has predict method
        model = joblib.load(model_path)
        assert hasattr(model, 'predict'), "Model missing predict method"
        
        print("Training successful! Model saved and validated.")

    def test_modeling_pipeline_evaluate(self, config, ensure_directories):
        """Test the evaluation stage of the modeling pipeline."""
        
        # Ensure training has been done
        model_path = "models/model.pkl"
        test_data_path = "data/processed_test_data.csv"
        scaler_path = "models/scaler.pkl"
        
        if not all(Path(p).exists() for p in [model_path, test_data_path, scaler_path]):
            self.test_modeling_pipeline_train(config, ensure_directories)
        
        # Import actual evaluation module
        from modeling.evaluate import main as evaluate_main
        
        # Define file paths
        metrics_path = "metrics/evaluation.json"
        
        # Clean up existing metrics
        if Path(metrics_path).exists():
            Path(metrics_path).unlink()
        
        # Run evaluation
        evaluate_main(
            config_file="params.yaml",
            model_file=model_path,
            evaluation_dataset=test_data_path,
            output_metrics=metrics_path,
            scaler_path=scaler_path
        )
        
        # Verify metrics were created
        assert Path(metrics_path).exists(), "Evaluation metrics not saved"
        
        # Verify metrics content
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)
        
        # Check expected metrics exist
        expected_metrics = config.get('train', {}).get('metrics', {}).get('enabled_metrics', [])
        for metric in expected_metrics:
            assert metric in metrics, f"Missing expected metric: {metric}"
            assert isinstance(metrics[metric], (int, float)), f"Invalid metric type for {metric}"
        
        print(f"Evaluation successful! Metrics: {metrics}")

    def test_model_type_switching(self, config, ensure_directories):
        """Test switching between different model types."""
        
        # Ensure we have preprocessed data
        train_data_path = "data/processed_training_data.csv"
        if not Path(train_data_path).exists():
            self.test_modeling_pipeline_preprocess(config, ensure_directories, None)
        
        from modeling.train import main as train_main
        
        # Test both model types
        model_types = ['linear_regression', 'random_forest']
        
        for model_type in model_types:
            print(f"Testing model type: {model_type}")
            
            # Create a temporary config with this model type
            test_config = config.copy()
            test_config['train']['model_type'] = model_type
            
            # Save temporary config
            temp_config_path = "temp_params.yaml"
            with open(temp_config_path, 'w') as f:
                import yaml
                yaml.dump(test_config, f)
            
            try:
                model_path = f"models/model_{model_type}.pkl"
                scaler_path = "models/scaler.pkl"
                
                # Clean up existing model
                if Path(model_path).exists():
                    Path(model_path).unlink()
                
                # Run training with the specific model type
                train_main(
                    config_file=temp_config_path,
                    processed_dataset=train_data_path,
                    output_model=model_path,
                    scaler_path=scaler_path
                )
                
                # Verify model was created
                assert Path(model_path).exists(), f"Model not saved for {model_type}"
                
                # Verify model can be loaded
                model = joblib.load(model_path)
                assert hasattr(model, 'predict'), f"Model missing predict method for {model_type}"
                
                print(f"Successfully created {model_type} model")
                
            finally:
                # Clean up temporary config
                if Path(temp_config_path).exists():
                    Path(temp_config_path).unlink()

    def test_dvc_pipeline_integration(self, config, ensure_directories):
        """Test that DVC pipeline components work correctly."""
        
        # Check if dvc.yaml exists
        dvc_config_path = Path("dvc.yaml")
        if not dvc_config_path.exists():
            pytest.skip("dvc.yaml not found")
        
        # Read DVC configuration
        import yaml
        with open(dvc_config_path, 'r') as f:
            dvc_config = yaml.safe_load(f)
        
        # Verify expected stages exist
        expected_stages = ['preprocess', 'train', 'evaluate']
        assert 'stages' in dvc_config, "DVC config missing stages"
        
        for stage in expected_stages:
            assert stage in dvc_config['stages'], f"Missing DVC stage: {stage}"
        
        # Verify params.yaml is listed as dependency
        for stage_name, stage_config in dvc_config['stages'].items():
            if 'deps' in stage_config:
                deps = stage_config['deps']
                params_dep_found = any('params.yaml' in str(dep) for dep in deps)
                assert params_dep_found, f"Stage {stage_name} missing params.yaml dependency"
        
        print("DVC pipeline configuration validated successfully")

    def test_complete_modeling_pipeline(self, config, ensure_directories, ensure_etl_data):
        """Test complete modeling pipeline from preprocessing to evaluation."""
        
        print("Running complete modeling pipeline integration test...")
        
        # Step 1: Preprocessing
        print("Step 1: Preprocessing")
        self.test_modeling_pipeline_preprocess(config, ensure_directories, ensure_etl_data)
        
        # Step 2: Training
        print("Step 2: Training")
        self.test_modeling_pipeline_train(config, ensure_directories)
        
        # Step 3: Evaluation
        print("Step 3: Evaluation")
        self.test_modeling_pipeline_evaluate(config, ensure_directories)
        
        print("Complete modeling pipeline integration test passed!")

    def test_model_artifacts_persistence(self, ensure_directories):
        """Test that model artifacts are properly saved and can be reloaded."""
        
        model_path = Path("models/model.pkl")
        scaler_path = Path("models/scaler.pkl")
        metrics_path = Path("metrics/evaluation.json")
        
        # Check if artifacts exist
        if not all(p.exists() for p in [model_path, scaler_path, metrics_path]):
            pytest.skip("Model artifacts not found. Run modeling pipeline first.")
        
        # Test model persistence
        model = joblib.load(model_path)
        assert hasattr(model, 'predict'), "Loaded model missing predict method"
        
        # Test scaler persistence
        scaler = joblib.load(scaler_path)
        assert hasattr(scaler, 'transform'), "Loaded scaler missing transform method"
        
        # Test metrics persistence
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)
        assert isinstance(metrics, dict), "Metrics should be a dictionary"
        assert len(metrics) > 0, "Metrics dictionary is empty"
        
        for metric_name, value in metrics.items():
            assert isinstance(value, (int, float)), f"Metric {metric_name} should be numeric"
        
        print("All model artifacts validated successfully")