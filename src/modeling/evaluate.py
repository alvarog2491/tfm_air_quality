import json

import pandas as pd
from sklearn.model_selection import train_test_split

from metrics_and_plots import save_confusion_matrix, save_metrics, save_predictions
from model import evaluate_model, train_model
from utils_and_constants import load_config, load_data

import argparse


def main(config_file: str, processed_dataset: str):
    config = load_config(config_file)

    # Load and split the dataset
    print("Loading and splitting the dataset...")

    target_column = config["train_and_evaluate"]["target_column"]
    shuffle = config["train_and_evaluate"]["shuffle"]
    shuffle_random_state = config["train_and_evaluate"]["shuffle_random_state"]
    X, y = load_data(processed_dataset, target_column, shuffle, shuffle_random_state)

    # Split the dataset
    random_state = config["train_and_evaluate"]["train_test_split"]["random_state"]
    test_size = config["train_and_evaluate"]["train_test_split"]["test_size"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    print(f"Dataset shape: {X.shape}")
    print(f"Train set shape: {X_train.shape}")
    print(f"Test set shape: {X_test.shape}")

    # Train and evaluate the model
    print("Training and evaluating the model...")
    rfc_params = config["train_and_evaluate"]["rfc_params"]
    model = train_model(X_train, y_train, **rfc_params)
    metrics = evaluate_model(model, X_test, y_test)

    print("====================Test Set Metrics==================")
    print(json.dumps(metrics, indent=2))
    print("======================================================")

    save_metrics(metrics)
    save_confusion_matrix(model, X_test, y_test)
    save_predictions(y_test, model.predict(X_test))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train and evaluate the weather prediction model"
    )
    parser.add_argument(
        "config_file",
        type=str,
        help="Configuration file with parameters",
        default="config.yaml",
    )
    parser.add_argument(
        "input_dataset",
        type=str,
        help="Processed Input CSV file path",
        default="processed_dataset/weather.csv",
    )
    args = parser.parse_args()
    main(
        config_file=args.config_file,
        processed_dataset=args.input_dataset,
    )
