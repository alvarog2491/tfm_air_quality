from typing import List

import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
import joblib

from utils import load_config
import argparse


def read_dataset(
    filename: str, drop_columns: List[str]) -> pd.DataFrame:
    """
    Reads the raw data file and returns pandas dataframe

    Parameters:
    filename (str): raw data filename
    drop_columns (List[str]): column names that will be dropped

    Returns:
    pd.Dataframe: Target encoded dataframe
    """
    df = pd.read_csv(filename).drop(columns=drop_columns)
    return df

def check_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks for missing values in the dataframe and stops execution if any are found

    Parameters:
    df (pd.Dataframe): Pandas dataframe containing features and targets

    Returns:
    pd.Dataframe: Dataframe 
    """
    if df.isnull().values.any():
        raise ValueError("Data contains missing values. Please handle them before proceeding.")

    return df

def one_hot_encode_categorical_features(
    df: pd.DataFrame, categorical_columns: List[str]) -> pd.DataFrame:
    """
    One hot encodes categorical columns in the dataframe


    Parameters:
    df (pd.Dataframe): Pandas dataframe containing features and targets
    categorical_columns (List[str]): categorical column names that will be one-hot encoded

    Returns:
    pd.Dataframe: One-hot encoded dataframe
    """
    df = pd.get_dummies(df, columns=categorical_columns, drop_first=True)
    return df


def scale_numerical_features(df: pd.DataFrame, numerical_features: list[str]) -> pd.DataFrame:
    """
    Scales the data to a normal distribution

    Parameters:
    df (pd.Dataframe): Pandas dataframe containing features and targets
    numerical_features (List[str]): numerical_features column names that will be scaled

    Returns:
    pd.Dataframe: Scaled dataframe
    """

    # Scale and fit with zero mean and unit variance
    scaler = StandardScaler()
    df[numerical_features] = scaler.fit_transform(df[numerical_features])
    return df, scaler

def separate_train_evaluate_dataset(df: pd.DataFrame, size: float) -> pd.DataFrame:
    """
    Separates the evaluation dataset from the training dataset

    Parameters:
    df (pd.Dataframe): Pandas dataframe containing features and targets

    Returns:
    pd.Dataframe: Evaluation dataset
    """
    return df.iloc[int(len(df) * size):], df.iloc[:int(len(df) * size)]


def main(config_file: str, raw_dataset: str, output_dataset: str):
    # Load configuration
    config = load_config(config_file)["preprocess"]
    drop_colnames = config["drop_colnames"]
    categorical_columns = config["categorical_features"]
    numerical_columns = config["numerical_features"]
    validation_size = config["validation_size"]
    target_column = config["target_column"]

    # Read dataset
    print("Reading raw data and processing it...")
    dataset = read_dataset(filename=raw_dataset, drop_columns=drop_colnames)
    training_dataset, evaluation_dataset = separate_train_evaluate_dataset(
        df=dataset, size=validation_size)

    # Check for missing values
    print("Checking for missing values...")
    training_dataset = check_missing_values(training_dataset)
    evaluation_dataset = check_missing_values(evaluation_dataset)
    
    # One-hot encode categorical columns
    print("One-hot encoding categorical columns...")
    training_dataset = one_hot_encode_categorical_features(
        df=training_dataset, categorical_columns=categorical_columns)

    # Scale features
    print("Scaling features...")
    training_dataset, scaler = scale_numerical_features(
        df=training_dataset, numerical_features=numerical_columns)

    # Write processed dataset
    print(f"Writing processed data...")
    training_dataset.to_csv(output_dataset, index=None)
    evaluation_dataset.to_csv("data/processed_evaluation_data.csv", index=None)
    joblib.dump(scaler, "models/scaler.pkl")
    print("Done!")


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
        "output_dataset",
        type=str,
        help="Processed CSV file path",
        default="data/processed_training_data.csv",
    )
    args = parser.parse_args()
    main(
        config_file=args.config_file,
        raw_dataset=args.input_dataset,
        output_dataset=args.output_dataset,
    )
