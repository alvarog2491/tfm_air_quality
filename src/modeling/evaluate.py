import argparse
import json
from pathlib import Path

from modeling.config.logger import setup_logger

from modeling.utils.dataset_modeling_utils import prepare_features_and_target
from common.utils.file_utils import create_directory, load_pickle_file, load_yaml_config, validate_file_exists, ValidationError
from common.utils.dataframe_utils import validate_data_shapes_match

logger = setup_logger(stage="EVALUATE")


def evaluate_model(model, X_test, y_test) -> None:
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
    with open(output_file, "w") as f:
        json.dump(metrics, f, indent=2)


def main(
    config_file: str,
    model_file: str,
    evaluation_dataset: str,
    output_metrics: str,
    scaler_path: str = None,
):
    try:
        # Validate inputs
        logger.info("Validating inputs...")
        validate_file_exists(config_file, "Configuration file")
        validate_file_exists(evaluation_dataset, "Evaluation dataset file")
        validate_file_exists(model_file, "Model file")
        
        # Load configuration
        logger.info("Loading configuration...")
        config = load_yaml_config(config_file)["evaluate"]
        target_column = config["target_column"]
        shuffle = config["shuffle"]
        shuffle_random_state = config["shuffle_random_state"]
        var_dtypes = config["var_dtypes"]

        # Loading dataset
        logger.info("Loading and splitting the dataset...")
        X, y = prepare_features_and_target(
            evaluation_dataset,
            target_column,
            shuffle,
            shuffle_random_state,
            var_dtypes=var_dtypes,
        )
        
        # Validate data
        validate_data_shapes_match(X, y)
        logger.info(f"Dataset X shape: {X.shape}")
        logger.info(f"Dataset y shape: {y.shape}")

        # Load the trained model
        logger.info("Loading the trained model...")
        model = load_pickle_file(model_file)
        
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during evaluation initialization: {e}")
        raise
    
    # Load scaler if provided
    scaler = None
    if scaler_path and Path(scaler_path).exists():
        logger.info("Loading scaler...")
        scaler = load_pickle_file(scaler_path)
    else:
        logger.warning("No scaler provided or scaler file not found. Using unscaled data.")

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
    parser.add_argument(
        "--scaler_path",
        type=str,
        help="Path to the fitted scaler",
        default="models/scaler.pkl",
    )

    args = parser.parse_args()

    main(
        config_file=args.config_file,
        model_file=args.model_file,
        evaluation_dataset=args.evaluation_dataset,
        output_metrics=args.output_metrics,
        scaler_path=args.scaler_path,
    )
