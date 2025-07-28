"""
Tests for RandomForest model implementation.
"""

import pytest
import pandas as pd
import numpy as np

from src.modeling.random_forest.random_forest import RandomForestModel


@pytest.fixture
def sample_data():
    """Sample training and test data."""
    np.random.seed(42)
    X = pd.DataFrame(
        {
            "feature1": np.random.randn(100),
            "feature2": np.random.randn(100),
            "feature3": np.random.randn(100),
        }
    )
    # Create target with some relationship to features
    y = pd.Series(
        2 * X["feature1"]
        + X["feature2"]
        + 0.5 * X["feature3"]
        + np.random.randn(100) * 0.1
    )

    return X, y


@pytest.fixture
def sample_hyperparameters():
    """Sample hyperparameters for RandomForest."""
    return {
        "n_estimators": 10,  # Small for fast testing
        "max_depth": 3,
        "min_samples_split": 5,
        "min_samples_leaf": 2,
        "random_state": 42,
    }


@pytest.fixture
def sample_metrics_config():
    """Sample metrics configuration."""
    return {"enabled_metrics": ["mse", "rmse", "mae", "r2"]}


class TestRandomForestModel:
    """Test cases for RandomForest model."""

    def test_model_initialization_default(self):
        """Test model initialization with default parameters."""
        model = RandomForestModel()

        assert model.metrics_config == {}
        assert model.hyperparameters == {}
        assert hasattr(model, "pipeline")

    def test_model_initialization_with_config(
        self, sample_hyperparameters, sample_metrics_config
    ):
        """Test model initialization with custom configuration."""
        model = RandomForestModel(
            metrics_config=sample_metrics_config, hyperparameters=sample_hyperparameters
        )

        assert model.metrics_config == sample_metrics_config
        assert model.hyperparameters == sample_hyperparameters

        # Check that hyperparameters are applied to the regressor
        regressor = model.pipeline.named_steps["regressor"]
        assert regressor.n_estimators == 10
        assert regressor.max_depth == 3
        assert regressor.random_state == 42

    def test_model_training(self, sample_data, sample_hyperparameters):
        """Test model training."""
        X, y = sample_data
        model = RandomForestModel(hyperparameters=sample_hyperparameters)

        # Should not raise any exceptions
        model.train(X, y)

        # Check that the model is fitted
        assert hasattr(model.pipeline.named_steps["regressor"], "estimators_")

    def test_model_prediction(self, sample_data, sample_hyperparameters):
        """Test model prediction."""
        X, y = sample_data
        model = RandomForestModel(hyperparameters=sample_hyperparameters)

        model.train(X, y)
        predictions = model.predict(X)

        assert len(predictions) == len(y)
        assert predictions.dtype in ["float64", "float32"]

    def test_model_evaluation_default_metrics(
        self, sample_data, sample_hyperparameters
    ):
        """Test model evaluation with default metrics."""
        X, y = sample_data
        model = RandomForestModel(hyperparameters=sample_hyperparameters)

        X_train, X_test = X[:80], X[80:]
        y_train, y_test = y[:80], y[80:]

        model.train(X_train, y_train)
        metrics = model.evaluate(X_test, y_test)

        # Default metrics should be mse and r2
        expected_metrics = {"mse", "r2"}
        assert set(metrics.keys()) == expected_metrics

        # Check that metrics are reasonable values
        assert isinstance(metrics["mse"], (int, float))
        assert isinstance(metrics["r2"], (int, float))
        assert metrics["mse"] >= 0
        assert -1 <= metrics["r2"] <= 1

    def test_model_evaluation_custom_metrics(
        self, sample_data, sample_hyperparameters, sample_metrics_config
    ):
        """Test model evaluation with custom metrics."""
        X, y = sample_data
        model = RandomForestModel(
            metrics_config=sample_metrics_config, hyperparameters=sample_hyperparameters
        )

        X_train, X_test = X[:80], X[80:]
        y_train, y_test = y[:80], y[80:]

        model.train(X_train, y_train)
        metrics = model.evaluate(X_test, y_test)

        # Should have all configured metrics
        expected_metrics = {"mse", "rmse", "mae", "r2"}
        assert set(metrics.keys()) == expected_metrics

        # Check metric relationships
        assert metrics["rmse"] == pytest.approx(np.sqrt(metrics["mse"]), rel=1e-10)
        assert all(isinstance(v, (int, float)) for v in metrics.values())

    def test_model_evaluation_subset_metrics(self, sample_data, sample_hyperparameters):
        """Test model evaluation with subset of metrics."""
        X, y = sample_data
        metrics_config = {"enabled_metrics": ["mae", "r2"]}
        model = RandomForestModel(
            metrics_config=metrics_config, hyperparameters=sample_hyperparameters
        )

        X_train, X_test = X[:80], X[80:]
        y_train, y_test = y[:80], y[80:]

        model.train(X_train, y_train)
        metrics = model.evaluate(X_test, y_test)

        # Should only have requested metrics
        expected_metrics = {"mae", "r2"}
        assert set(metrics.keys()) == expected_metrics

    def test_model_hyperparameter_validation(self):
        """Test that invalid hyperparameters are handled."""
        # This should work - valid hyperparameters
        valid_params = {"n_estimators": 50, "random_state": 42}
        model = RandomForestModel(hyperparameters=valid_params)
        assert model.hyperparameters == valid_params

    def test_model_performance_consistency(self, sample_data):
        """Test that model with same random state produces consistent results."""
        X, y = sample_data
        hyperparams = {"n_estimators": 10, "random_state": 42}

        # Train two models with same parameters
        model1 = RandomForestModel(hyperparameters=hyperparams)
        model2 = RandomForestModel(hyperparameters=hyperparams)

        X_train, X_test = X[:80], X[80:]
        y_train, y_test = y[:80], y[80:]

        model1.train(X_train, y_train)
        model2.train(X_train, y_train)

        pred1 = model1.predict(X_test)
        pred2 = model2.predict(X_test)

        # Predictions should be identical due to fixed random state
        np.testing.assert_array_almost_equal(pred1, pred2, decimal=10)
