"""
Model Registry for managing different model types in the ML pipeline.
"""
from typing import Dict, Type
import logging
from modeling.base_model import BaseModel

# Use a simple logger without stage formatting to avoid conflicts
logger = logging.getLogger("ModelRegistry")
logger.setLevel(logging.INFO)

# Only add handler if not already present
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


class ModelRegistry:
    """Registry for managing model implementations."""
    
    _models: Dict[str, Type[BaseModel]] = {}
    
    @classmethod
    def register(cls, name: str, model_class: Type[BaseModel]) -> None:
        """
        Register a model class with a given name.
        
        Args:
            name: String identifier for the model
            model_class: Model class that inherits from BaseModel
        """
        if not issubclass(model_class, BaseModel):
            raise ValueError(f"Model class {model_class.__name__} must inherit from BaseModel")
        cls._models[name] = model_class
    
    @classmethod
    def create_model(cls, name: str, metrics_config: dict = None, hyperparameters: dict = None) -> BaseModel:
        """
        Create an instance of the registered model.
        
        Args:
            name: String identifier for the model
            metrics_config: Configuration for model metrics
            hyperparameters: Model-specific hyperparameters
            
        Returns:
            Instance of the requested model
            
        Raises:
            ValueError: If model name is not registered
        """
        if name not in cls._models:
            available_models = list(cls._models.keys())
            raise ValueError(
                f"Unknown model type: '{name}'. "
                f"Available models: {available_models}"
            )
        return cls._models[name](metrics_config, hyperparameters)
    
    @classmethod
    def list_models(cls) -> list:
        """Return list of available model names."""
        return list(cls._models.keys())


def get_model(model_type: str, metrics_config: dict = None, hyperparameters: dict = None) -> BaseModel:
    """
    Convenience function to get a model instance.
    
    Args:
        model_type: String identifier for the model
        metrics_config: Configuration for model metrics
        hyperparameters: Model-specific hyperparameters
        
    Returns:
        Instance of the requested model
    """
    return ModelRegistry.create_model(model_type, metrics_config, hyperparameters)


# Auto-register available models
def _register_available_models():
    """Auto-register all available model implementations."""
    try:
        from modeling.linear_regression.linear_regression import LinearRegressionModel
        ModelRegistry.register("linear_regression", LinearRegressionModel)
        logger.info("Successfully registered linear_regression model")
    except ImportError as e:
        logger.error(f"Failed to import LinearRegressionModel: {e}")
        raise
    except Exception as e:
        logger.error(f"Error registering linear_regression model: {e}")
        raise
    
    try:
        from modeling.random_forest.random_forest import RandomForestModel
        ModelRegistry.register("random_forest", RandomForestModel)
        logger.info("Successfully registered random_forest model")
    except ImportError as e:
        logger.error(f"Failed to import RandomForestModel: {e}")
        raise
    except Exception as e:
        logger.error(f"Error registering random_forest model: {e}")
        raise


# Initialize registry with available models
_register_available_models()