from base_model import BaseModel
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from typing import Dict, Any


class RandomForestModel(BaseModel):
    def __init__(self, metrics_config: Dict[str, Any] = None, hyperparameters: Dict[str, Any] = None):
        super().__init__(metrics_config, hyperparameters)
        
        # Apply hyperparameters to RandomForestRegressor
        rf_params = self.hyperparameters.copy()
        
        self.pipeline = Pipeline([("regressor", RandomForestRegressor(**rf_params))])

    def train(self, X_train, y_train):
        self.pipeline.fit(X_train, y_train)

    def predict(self, X_test):
        return self.pipeline.predict(X_test)

    def evaluate(self, X_test, y_test):
        from sklearn.metrics import (
            mean_squared_error, r2_score, mean_absolute_error, 
            mean_absolute_percentage_error
        )
        import numpy as np

        predictions = self.predict(X_test)
        
        # Get configured metrics or use defaults
        enabled_metrics = self.metrics_config.get("enabled_metrics", ["mse", "r2"])
        
        metrics = {}
        
        if "mse" in enabled_metrics:
            metrics["mse"] = mean_squared_error(y_test, predictions)
        
        if "rmse" in enabled_metrics:
            metrics["rmse"] = np.sqrt(mean_squared_error(y_test, predictions))
        
        if "mae" in enabled_metrics:
            metrics["mae"] = mean_absolute_error(y_test, predictions)
        
        if "mape" in enabled_metrics:
            metrics["mape"] = mean_absolute_percentage_error(y_test, predictions)
        
        if "r2" in enabled_metrics:
            metrics["r2"] = r2_score(y_test, predictions)
        
        return metrics