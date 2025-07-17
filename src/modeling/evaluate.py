import json
from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split
from utils.data_utils import prepare_features_and_target, one_hot_encode_categorical_features, scale_numerical_features
from utils.file_system_utils import load_yaml_config, create_directory, load_pickle_file

import argparse
from config.logger import setup_logger

logger = setup_logger(stage="EVALUATE")

def evaluate_model(model, X_test, y_test):
    """
    Evaluate the model on the test set and return metrics.
    """
    metrics = model.evaluate(X_test, y_test)
    return metrics

def save_metrics(metrics, output_file):
    """
    Save the evaluation metrics to a JSON file.
    """
    create_directory(Path(output_file).parent)
    with open(output_file, 'w') as f:
        json.dump(metrics, f, indent=2)


def main(
    config_file: str,
    model_file: str,
    evaluation_dataset: str,
    output_metrics: str,
):
    # Load configuration
    logger.info("Loading configuration...")
    config = load_yaml_config(config_file)["evaluate"]
    target_column = config["target_column"]
    shuffle = config["shuffle"]
    shuffle_random_state = config["shuffle_random_state"]
    var_dtypes = config["var_dtypes"]

    # Loading dataset
    logger.info("Loading and splitting the dataset...")
    X, y = prepare_features_and_target(evaluation_dataset, target_column, shuffle, shuffle_random_state, var_dtypes=var_dtypes)
    logger.info(f"Dataset X shape: {X.shape}")
    logger.info(f"Dataset y shape: {y.shape}")

    # Load the trained model
    logger.info("Loading the trained model...")
    model = load_pickle_file(model_file) 

    # Evaluate the model
    logger.info("Evaluating the model...")
    metrics = evaluate_model(model, X, y)

    logger.info("====================Test Set Metrics==================")
    logger.info(json.dumps(metrics, indent=2))
    logger.info("======================================================")
    save_metrics(metrics, output_metrics)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluate Air Quality prediction model"
    )
    parser.add_argument(
        "config_file",
        type=str,
        help="Configuration file with parameters",
    )
    parser.add_argument(
        "model_file",
        type=str,
        help="Trained model file path",
    )
    parser.add_argument(
        "evaluation_dataset",
        type=str,
        help="Processed evaluation CSV file path",
    )
    parser.add_argument(
        "output_metrics",
        type=str,
        help="Output JSON file for evaluation metrics",
    )

    args = parser.parse_args()

    main(
        config_file=args.config_file,
        model_file=args.model_file,
        evaluation_dataset=args.evaluation_dataset,
        output_metrics=args.output_metrics,
    )
