"""
Integration tests for train.py module.
"""
import tempfile
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
import joblib

from modeling.train import main


@pytest.fixture
def sample_config():
    """Sample configuration for training."""
    return {
        "train": {
            "model_type": "linear_regression",
            "target_column": "target",
            "metrics": {"enabled_metrics": ["mse", "r2"]},
            "train_test_split": {
                "test_size": 0.2,
                "random_state": 42
            },
            "shuffle": False,
            "shuffle_random_state": 42
        }
    }


@pytest.fixture
def sample_processed_dataset():
    """Sample processed dataset for training."""
    return pd.DataFrame({
        "feature1": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "feature2": [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0],
        "target": [2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
    })


class TestTrainIntegration:
    """Integration tests for the training pipeline."""

    @patch("modeling.train.load_yaml_config")
    @patch("modeling.train.prepare_features_and_target")
    def test_train_pipeline_end_to_end(
        self, mock_prepare_features, mock_load_config, 
        sample_config, sample_processed_dataset, tmp_path
    ):
        """Test complete training pipeline from config to model output."""
        
        # Setup mocks
        mock_load_config.return_value = sample_config
        
        # Prepare features and target
        X = sample_processed_dataset[["feature1", "feature2"]]
        y = sample_processed_dataset["target"]
        mock_prepare_features.return_value = (X, y)
        
        # Setup file paths
        config_file = tmp_path / "config.yaml"
        config_file.write_text("dummy config")
        
        dataset_file = tmp_path / "dataset.csv"
        sample_processed_dataset.to_csv(dataset_file, index=False)
        
        model_output = tmp_path / "models" / "model.pkl"
        
        # Execute training
        main(
            config_file=str(config_file),
            processed_dataset=str(dataset_file),
            output_model=str(model_output)
        )
        
        # Verify model was saved
        assert model_output.exists()
        
        # Verify model can be loaded and used
        model = joblib.load(model_output)
        predictions = model.predict(X)
        assert len(predictions) == len(X)
        assert predictions.dtype in ['float64', 'float32']

    @patch("modeling.train.load_yaml_config")
    @patch("modeling.train.prepare_features_and_target")
    def test_train_with_scaler_persistence(
        self, mock_prepare_features, mock_load_config,
        sample_config, sample_processed_dataset, tmp_path
    ):
        """Test training pipeline with scaler persistence."""
        
        # Setup mocks
        mock_load_config.return_value = sample_config
        
        X = sample_processed_dataset[["feature1", "feature2"]]
        y = sample_processed_dataset["target"]
        mock_prepare_features.return_value = (X, y)
        
        # Create a dummy scaler file
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        scaler.fit(X)
        
        scaler_path = tmp_path / "scaler.pkl"
        joblib.dump(scaler, scaler_path)
        
        # Setup other paths
        config_file = tmp_path / "config.yaml"
        config_file.write_text("dummy")
        
        dataset_file = tmp_path / "dataset.csv"
        sample_processed_dataset.to_csv(dataset_file, index=False)
        
        model_output = tmp_path / "models" / "model.pkl"
        
        # Execute training with scaler
        main(
            config_file=str(config_file),
            processed_dataset=str(dataset_file),
            output_model=str(model_output),
            scaler_path=str(scaler_path)
        )
        
        # Verify both model and scaler were saved
        assert model_output.exists()
        assert (tmp_path / "models" / "scaler.pkl").exists()

    @patch("modeling.train.load_yaml_config")
    def test_train_validation_errors(self, mock_load_config, tmp_path):
        """Test that validation errors are properly handled."""
        
        # Test missing config file
        with pytest.raises(Exception):  # ValidationError or FileNotFoundError
            main(
                config_file="nonexistent.yaml",
                processed_dataset="dummy.csv",
                output_model="model.pkl"
            )
        
        # Test invalid config
        mock_load_config.return_value = {"train": {"invalid": "config"}}
        config_file = tmp_path / "config.yaml"
        config_file.write_text("dummy")
        
        with pytest.raises(Exception):  # ValidationError
            main(
                config_file=str(config_file),
                processed_dataset="nonexistent.csv",
                output_model="model.pkl"
            )

    @patch("modeling.train.load_yaml_config")
    @patch("modeling.train.prepare_features_and_target")
    def test_train_metrics_configuration(
        self, mock_prepare_features, mock_load_config,
        sample_processed_dataset, tmp_path
    ):
        """Test that metrics configuration is properly used."""
        
        # Config with custom metrics
        custom_config = {
            "train": {
                "model_type": "linear_regression",
                "target_column": "target",
                "metrics": {"enabled_metrics": ["mse", "rmse", "mae", "r2"]},
                "train_test_split": {"test_size": 0.2, "random_state": 42},
                "shuffle": False,
                "shuffle_random_state": 42
            }
        }
        
        mock_load_config.return_value = custom_config
        
        X = sample_processed_dataset[["feature1", "feature2"]]
        y = sample_processed_dataset["target"]
        mock_prepare_features.return_value = (X, y)
        
        # Setup paths
        config_file = tmp_path / "config.yaml"
        config_file.write_text("dummy")
        
        dataset_file = tmp_path / "dataset.csv"
        sample_processed_dataset.to_csv(dataset_file, index=False)
        
        model_output = tmp_path / "models" / "model.pkl"
        
        # Execute training
        main(
            config_file=str(config_file),
            processed_dataset=str(dataset_file),
            output_model=str(model_output)
        )
        
        # Verify model was created with metrics config
        assert model_output.exists()
        model = joblib.load(model_output)
        
        # Test that model can evaluate with configured metrics
        metrics = model.evaluate(X.iloc[:5], y.iloc[:5])
        expected_metrics = {"mse", "rmse", "mae", "r2"}
        assert set(metrics.keys()) == expected_metrics