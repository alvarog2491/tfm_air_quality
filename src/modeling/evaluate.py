import json
from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split
from data_utils import prepare_features_and_target, one_hot_encode_categorical_features, scale_numerical_features
from utils import load_yaml_config, create_directory

import argparse

def evaluate_model(model, X_test, y_test):
    """
    Evaluate the model on the test set and return metrics.
    """
    metrics = model.evaluate(X_test, y_test)
    return metrics

def load_pickle_file(file_path):
    """
    Load a pickle file from the specified path.
    """
    import joblib
    return joblib.load(file_path)

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
    print("Loading configuration...")
    config = load_yaml_config(config_file)["evaluate"]
    target_column = config["target_column"]
    shuffle = config["shuffle"]
    shuffle_random_state = config["shuffle_random_state"]
    var_dtypes = config["var_dtypes"]

    # Loading dataset
    print("Loading and splitting the dataset...")
    X, y = prepare_features_and_target(evaluation_dataset, target_column, shuffle, shuffle_random_state, var_dtypes=var_dtypes)
    print(f"Dataset X shape: {X.shape}")
    print(f"Dataset y shape: {y.shape}")

    # Load the trained model
    print("Loading the trained model...")
    model = load_pickle_file(model_file) 

    # Evaluate the model
    print("Evaluating the model...")
    metrics = evaluate_model(model, X, y)

    print("====================Test Set Metrics==================")
    print(json.dumps(metrics, indent=2))
    print("======================================================")
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
