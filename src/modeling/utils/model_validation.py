"""
Model-specific validation utilities.
"""

from typing import Any, Dict

from common.utils.file_utils import ValidationError


def validate_model_config(config: Dict[str, Any]) -> None:
    """
    Validate model configuration parameters.

    Args:
        config: Configuration dictionary

    Raises:
        ValidationError: If configuration is invalid
    """
    required_keys = ["model_type", "target_column"]
    missing_keys = set(required_keys) - set(config.keys())
    if missing_keys:
        raise ValidationError(
            f"Model config missing required keys: {sorted(missing_keys)}"
        )

    if not isinstance(config["model_type"], str):
        raise ValidationError("model_type must be a string")

    if not isinstance(config["target_column"], str):
        raise ValidationError("target_column must be a string")


def validate_preprocessing_config(config: Dict[str, Any]) -> None:
    """
    Validate preprocessing configuration parameters.

    Args:
        config: Configuration dictionary

    Raises:
        ValidationError: If configuration is invalid
    """
    required_keys = [
        "categorical_features",
        "numerical_features",
        "target_column",
        "validation_size",
    ]
    missing_keys = set(required_keys) - set(config.keys())
    if missing_keys:
        raise ValidationError(
            f"Preprocessing config missing required keys: {sorted(missing_keys)}"
        )

    if not isinstance(config["categorical_features"], list):
        raise ValidationError("categorical_features must be a list")

    if not isinstance(config["numerical_features"], list):
        raise ValidationError("numerical_features must be a list")

    validation_size = config["validation_size"]
    if not isinstance(validation_size, (int, float)) or not (0 < validation_size < 1):
        raise ValidationError("validation_size must be a number between 0 and 1")
