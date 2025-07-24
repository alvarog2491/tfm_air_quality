import argparse

from common.utils.dataframe_utils import (
    load_raw_dataset,
    validate_no_missing_values,
)
from modeling.utils.dataset_modeling_utils import (
    one_hot_encode_categorical_features,
    scale_numerical_features,
    separate_train_evaluate_dataset,
)
from common.utils.file_utils import load_yaml_config

from modeling.config.logger import setup_logger

logger = setup_logger(stage="PREPROCESS")


def main(
    config_file: str,
    raw_dataset: str,
    output_train_dataset: str,
    output_test_dataset: str,
) -> None:

    # Load configuration
    config = load_yaml_config(config_file)["preprocess"]
    drop_colnames = config["drop_colnames"]
    categorical_columns = config["categorical_features"]
    numerical_columns = config["numerical_features"]
    validation_size: float = config["validation_size"]
    var_dtypes = config["var_dtypes"]

    # Read dataset
    logger.info("Reading raw data...")
    dataset = load_raw_dataset(
        filepath=raw_dataset, drop_columns=drop_colnames, var_dtypes=var_dtypes
    )

    logger.info("Splitting dataset into training and evaluation sets...")
    training_dataset, evaluation_dataset = separate_train_evaluate_dataset(
        df=dataset, size=validation_size
    )

    # Check for missing values
    logger.info("Checking for missing values...")
    validate_no_missing_values(training_dataset)
    validate_no_missing_values(evaluation_dataset)

    # One-hot encode categorical columns
    logger.info("One-hot encoding categorical columns...")
    training_dataset = one_hot_encode_categorical_features(
        df=training_dataset, categorical_columns=categorical_columns
    )
    evaluation_dataset = one_hot_encode_categorical_features(
        df=evaluation_dataset, categorical_columns=categorical_columns
    )

    # Scale features
    logger.info("Scaling features...")
    training_dataset, scaler = scale_numerical_features(
        df=training_dataset, numerical_features=numerical_columns, scaler=None
    )
    evaluation_dataset, _ = scale_numerical_features(
        df=evaluation_dataset, numerical_features=numerical_columns, scaler=None
    )

    # Write processed dataset
    logger.info("Writing processed data...")
    training_dataset.to_csv(output_train_dataset, index=None)
    evaluation_dataset.to_csv(output_test_dataset, index=None)
    logger.info("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Preprocess data for the air quality dataset"
    )
    parser.add_argument(
        "config_file",
        type=str,
        help="Configuration file with parameters",
        default="params.yaml",
    )
    parser.add_argument(
        "input_dataset",
        type=str,
        help="CSV file with the raw dataset",
        default="src/dataset_creator/data/output/dataset.csv",
    )
    parser.add_argument(
        "output_train_dataset",
        type=str,
        help="Processed train CSV file path",
        default="data/processed_training_data.csv",
    )
    parser.add_argument(
        "output_test_dataset",
        type=str,
        help="Processed test CSV file path",
        default="data/processed_test_data.csv",
    )
    args = parser.parse_args()
    main(
        config_file=args.config_file,
        raw_dataset=args.input_dataset,
        output_train_dataset=args.output_train_dataset,
        output_test_dataset=args.output_test_dataset,
    )
