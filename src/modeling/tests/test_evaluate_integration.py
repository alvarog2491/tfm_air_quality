"""
Integration tests for evaluate.py module.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LinearRegression

from modeling.evaluate import main


@pytest.fixture
def sample_config():
    """Sample configuration for evaluation."""
    return {
        "evaluate": {
            "target_column": "target",
            "metrics": {"enabled_metrics": ["mse", "r2", "mae"]},
            "var_dtypes": {},
            "shuffle": False,
            "shuffle_random_state": 42,
        }
    }


@pytest.fixture
def sample_evaluation_dataset():
    """Sample evaluation dataset."""
    return pd.DataFrame(
        {
            "feature1": [1.0, 2.0, 3.0, 4.0, 5.0],
            "feature2": [0.5, 1.0, 1.5, 2.0, 2.5],
            "target": [2.0, 4.0, 6.0, 8.0, 10.0],
        }
    )


@pytest.fixture
def trained_model():
    """Sample trained model for testing."""
    from src.modeling.linear_regression.linear_regression import LinearRegressionModel

    # Create and train a simple model
    model = LinearRegressionModel({"enabled_metrics": ["mse", "r2", "mae"]})

    # Simple training data
    X_train = pd.DataFrame(
        {"feature1": [1.0, 2.0, 3.0, 4.0], "feature2": [0.5, 1.0, 1.5, 2.0]}
    )
    y_train = pd.Series([2.0, 4.0, 6.0, 8.0])

    model.train(X_train, y_train)
    return model


class TestEvaluateIntegration:
    """Integration tests for the evaluation pipeline."""

    @patch("modeling.evaluate.load_yaml_config")
    @patch("modeling.evaluate.prepare_features_and_target")
    def test_evaluate_pipeline_end_to_end(
        self,
        mock_prepare_features,
        mock_load_config,
        sample_config,
        sample_evaluation_dataset,
        trained_model,
        tmp_path,
    ):
        """Test complete evaluation pipeline from config to metrics output."""

        # Setup mocks
        mock_load_config.return_value = sample_config

        X = sample_evaluation_dataset[["feature1", "feature2"]]
        y = sample_evaluation_dataset["target"]
        mock_prepare_features.return_value = (X, y)

        # Save model to file
        model_file = tmp_path / "model.pkl"
        joblib.dump(trained_model, model_file)

        # Setup other files
        config_file = tmp_path / "config.yaml"
        config_file.write_text("dummy config")

        dataset_file = tmp_path / "dataset.csv"
        sample_evaluation_dataset.to_csv(dataset_file, index=False)

        metrics_output = tmp_path / "metrics.json"

        # Execute evaluation
        main(
            config_file=str(config_file),
            model_file=str(model_file),
            evaluation_dataset=str(dataset_file),
            output_metrics=str(metrics_output),
        )

        # Verify metrics were saved
        assert metrics_output.exists()

        # Verify metrics content
        with open(metrics_output) as f:
            metrics = json.load(f)

        expected_metrics = {"mse", "r2", "mae"}
        assert set(metrics.keys()) == expected_metrics

        # Verify metric values are reasonable
        assert isinstance(metrics["mse"], (int, float))
        assert isinstance(metrics["r2"], (int, float))
        assert isinstance(metrics["mae"], (int, float))

    @patch("modeling.evaluate.load_yaml_config")
    @patch("modeling.evaluate.prepare_features_and_target")
    def test_evaluate_with_scaler(
        self,
        mock_prepare_features,
        mock_load_config,
        sample_config,
        sample_evaluation_dataset,
        trained_model,
        tmp_path,
    ):
        """Test evaluation pipeline with scaler loading."""

        # Setup mocks
        mock_load_config.return_value = sample_config

        X = sample_evaluation_dataset[["feature1", "feature2"]]
        y = sample_evaluation_dataset["target"]
        mock_prepare_features.return_value = (X, y)

        # Create and save scaler
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        scaler.fit(X)

        scaler_file = tmp_path / "scaler.pkl"
        joblib.dump(scaler, scaler_file)

        # Save model
        model_file = tmp_path / "model.pkl"
        joblib.dump(trained_model, model_file)

        # Setup other files
        config_file = tmp_path / "config.yaml"
        config_file.write_text("dummy")

        dataset_file = tmp_path / "dataset.csv"
        sample_evaluation_dataset.to_csv(dataset_file, index=False)

        metrics_output = tmp_path / "metrics.json"

        # Execute evaluation with scaler
        main(
            config_file=str(config_file),
            model_file=str(model_file),
            evaluation_dataset=str(dataset_file),
            output_metrics=str(metrics_output),
            scaler_path=str(scaler_file),
        )

        # Verify metrics were generated
        assert metrics_output.exists()

        with open(metrics_output) as f:
            metrics = json.load(f)

        assert len(metrics) > 0

    @patch("modeling.evaluate.load_yaml_config")
    def test_evaluate_validation_errors(self, mock_load_config, tmp_path):
        """Test that validation errors are properly handled."""

        # Test missing config file
        with pytest.raises(Exception):  # ValidationError or FileNotFoundError
            main(
                config_file="nonexistent.yaml",
                model_file="model.pkl",
                evaluation_dataset="dataset.csv",
                output_metrics="metrics.json",
            )

        # Test missing model file
        config_file = tmp_path / "config.yaml"
        config_file.write_text("dummy")
        mock_load_config.return_value = {"evaluate": {"target_column": "target"}}

        with pytest.raises(Exception):  # ValidationError
            main(
                config_file=str(config_file),
                model_file="nonexistent_model.pkl",
                evaluation_dataset="dataset.csv",
                output_metrics="metrics.json",
            )

    @patch("modeling.evaluate.load_yaml_config")
    @patch("modeling.evaluate.prepare_features_and_target")
    def test_evaluate_metrics_output_format(
        self,
        mock_prepare_features,
        mock_load_config,
        sample_config,
        sample_evaluation_dataset,
        trained_model,
        tmp_path,
    ):
        """Test that metrics are output in correct JSON format."""

        # Setup mocks
        mock_load_config.return_value = sample_config

        X = sample_evaluation_dataset[["feature1", "feature2"]]
        y = sample_evaluation_dataset["target"]
        mock_prepare_features.return_value = (X, y)

        # Save model
        model_file = tmp_path / "model.pkl"
        joblib.dump(trained_model, model_file)

        # Setup files
        config_file = tmp_path / "config.yaml"
        config_file.write_text("dummy")

        dataset_file = tmp_path / "dataset.csv"
        sample_evaluation_dataset.to_csv(dataset_file, index=False)

        metrics_output = tmp_path / "metrics.json"

        # Execute evaluation
        main(
            config_file=str(config_file),
            model_file=str(model_file),
            evaluation_dataset=str(dataset_file),
            output_metrics=str(metrics_output),
        )

        # Verify JSON format
        assert metrics_output.exists()

        with open(metrics_output) as f:
            metrics = json.load(f)

        # Verify it's a valid JSON object with numeric values
        assert isinstance(metrics, dict)
        for key, value in metrics.items():
            assert isinstance(key, str)
            assert isinstance(value, (int, float))
            # Verify no NaN or infinite values
            assert not (pd.isna(value) or np.isinf(value))
