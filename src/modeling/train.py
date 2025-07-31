import argparse
import json
from pathlib import Path
from typing import Any, Dict

import joblib
from sklearn.model_selection import train_test_split

from common.utils.dataframe_utils import (
    validate_data_shapes_match,
    validate_dataframe_not_empty,
)
from common.utils.file_utils import (
    ValidationError,
    load_yaml_config,
    validate_file_exists,
)
from modeling.config.logger import setup_logger
from modeling.utils.dataset_modeling_utils import prepare_features_and_target
from modeling.utils.model_validation import validate_model_config

logger = setup_logger(stage="TRAIN")


def main(
    config_file: str, processed_dataset: str, output_model: str, scaler_path: str = None
):
    try:
        # Validate inputs
        logger.info("Validating inputs...")
        validate_file_exists(config_file, "Configuration file")
        validate_file_exists(processed_dataset, "Processed dataset file")

        # Load configuration
        config: Dict[str, Any] = load_yaml_config(config_file)["train"]
        validate_model_config(config)

        model_type: str = config["model_type"]
        target_column = config["target_column"]
        metrics_config = config.get("metrics", {})
        hyperparameters_config = config.get("hyperparameters", {})
        model_hyperparameters = hyperparameters_config.get(model_type, {})
        shuffle = config["shuffle"]
        shuffle_random_state = config["shuffle_random_state"]
        random_state = config["train_test_split"]["random_state"]
        test_size = config["train_test_split"]["test_size"]

        # Load and split the dataset
        logger.info("Loading and splitting the dataset...")
        X, y = prepare_features_and_target(
            processed_dataset, target_column, shuffle, shuffle_random_state
        )

        # Validate data
        validate_data_shapes_match(X, y)

    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during training initialization: {e}")
        raise

    # Split the dataset
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    logger.info(f"Dataset shape: {X.shape}")
    logger.info(f"Train set shape: {X_train.shape}")
    logger.info(f"Test set shape: {X_test.shape}")

    # Train and evaluate the model
    logger.info("Training and evaluating the model...")
    logger.info(f"Using hyperparameters: {model_hyperparameters}")
    from modeling.model_factory import create_model

    model = create_model(model_type, metrics_config, model_hyperparameters)
    model.train(X_train, y_train)

    metrics = model.evaluate(X_test, y_test)

    # Save model and scaler
    model_dir = Path(output_model).parent
    model_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, output_model)
    logger.info(f"Model saved to: {output_model}")

    # Save scaler if provided (from preprocessing step)
    if scaler_path and Path(scaler_path).exists():
        scaler_output = model_dir / "scaler.pkl"
        scaler = joblib.load(scaler_path)
        joblib.dump(scaler, scaler_output)
        logger.info(f"Scaler saved to: {scaler_output}")

    logger.info("====================Test Set Metrics==================")
    logger.info(json.dumps(metrics, indent=2))
    logger.info("======================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train the air quality model")
    parser.add_argument(
        "config_file",
        type=str,
        help="Configuration file with parameters",
        default="params.yaml",
    )
    parser.add_argument(
        "input_dataset",
        type=str,
        help="Processed Input CSV file path",
        default="data/processed_training_data.csv",
    )
    parser.add_argument(
        "output_model",
        type=str,
        help="Output model file path",
        default="models/linear_regression_model.pkl",
    )
    parser.add_argument(
        "--scaler_path",
        type=str,
        help="Path to saved scaler from preprocessing",
        default="models/scaler.pkl",
    )
    args = parser.parse_args()
    main(
        config_file=args.config_file,
        processed_dataset=args.input_dataset,
        output_model=args.output_model,
        scaler_path=args.scaler_path,
    )
