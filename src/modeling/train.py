import argparse
import json

import joblib
from config.logger import setup_logger
from sklearn.model_selection import train_test_split

from common.utils.data_utils import prepare_features_and_target
from common.utils.file_utils import load_yaml_config

logger = setup_logger(stage="TRAIN")


def main(config_file: str, processed_dataset: str, output_model: str):
    # Load configuration
    config = load_yaml_config(config_file)["train"]
    model_type = config["model_type"]
    target_column = config["target_column"]
    shuffle = config["shuffle"]
    shuffle_random_state = config["shuffle_random_state"]
    random_state = config["train_test_split"]["random_state"]
    test_size = config["train_test_split"]["test_size"]

    # Load and split the dataset
    logger.info("Loading and splitting the dataset...")
    X, y = prepare_features_and_target(
        processed_dataset, target_column, shuffle, shuffle_random_state
    )

    # Split the dataset
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    logger.info(f"Dataset shape: {X.shape}")
    logger.info(f"Train set shape: {X_train.shape}")
    logger.info(f"Test set shape: {X_test.shape}")

    # Train and evaluate the model
    logger.info("Training and evaluating the model...")
    if model_type == "linear_regression":
        from linear_regression.linear_regression import LinearRegressionModel

        model = LinearRegressionModel()
        model.train(X_train, y_train)

    metrics = model.evaluate(X_test, y_test)
    joblib.dump(model, output_model)
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
    args = parser.parse_args()
    main(
        config_file=args.config_file,
        processed_dataset=args.input_dataset,
        output_model=args.output_model,
    )
