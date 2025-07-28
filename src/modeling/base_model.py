from abc import ABC, abstractmethod
from typing import Dict, List, Any


class BaseModel(ABC):
    def __init__(self, metrics_config: Dict[str, Any] = None, hyperparameters: Dict[str, Any] = None):
        """
        Initialize the base model.
        
        Args:
            metrics_config: Configuration for evaluation metrics
            hyperparameters: Model-specific hyperparameters
        """
        self.metrics_config = metrics_config or {}
        self.hyperparameters = hyperparameters or {}
    
    @abstractmethod
    def train(self, X_train, y_train):
        pass

    @abstractmethod
    def predict(self, X_test):
        pass

    @abstractmethod
    def evaluate(self, X_test, y_test):
        pass
