from typing import Dict, List, Optional, Tuple

import pandas as pd
from sklearn.preprocessing import StandardScaler
import logging
from common.utils.dataframe_utils import load_raw_dataset

# Use a simple logger without stage formatting to avoid conflicts
logger = logging.getLogger("ModelingUtils")
logger.setLevel(logging.INFO)

# Only add handler if not already present
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


def prepare_features_and_target(
    filepath: str,
    target_column: str,
    shuffle: bool,
    shuffle_random_state: int,
    var_dtypes: Dict[str, str] = {},
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Load dataset from CSV, optionally shuffle it, and split into features and target.

    Parameters:
    filepath (str): Path to the data CSV file.
    target_column (str): Name of the target column.
    shuffle (bool): Whether to shuffle the data.
    shuffle_random_state (int): Random seed for reproducible shuffling.

    Returns:
    Tuple[pd.DataFrame, pd.Series]: Features dataframe (X) and target series (y).
    """
    data = load_raw_dataset(filepath, var_dtypes=var_dtypes)

    if shuffle:
        data = data.sample(frac=1, random_state=shuffle_random_state).reset_index(
            drop=True
        )

    X = data.drop(columns=target_column)
    y = data[target_column]
    logger.info(f"X Shape: {X.shape}")
    logger.info(f"y Shape: {y.shape}")
    return X, y


def one_hot_encode_categorical_features(
    df: pd.DataFrame, categorical_columns: List[str]
) -> pd.DataFrame:
    """
    One hot encodes categorical columns in the dataframe

    Parameters:
    df (pd.DataFrame): Pandas dataframe containing features and targets
    categorical_columns (List[str]): categorical column names that will be one-hot encoded

    Returns:
    pd.DataFrame: One-hot encoded dataframe
    """
    df = pd.get_dummies(df, columns=categorical_columns, drop_first=True)
    return df


def scale_numerical_features(
    df: pd.DataFrame,
    numerical_features: List[str],
    scaler: Optional[StandardScaler] = None,
) -> Tuple[pd.DataFrame, StandardScaler]:
    """
    Scales the data to a normal distribution

    Parameters:
    df (pd.DataFrame): Pandas dataframe containing features and targets
    numerical_features (List[str]): numerical_features column names that will be scaled

    Returns:
    Tuple[pd.DataFrame, StandardScaler]: Scaled dataframe and the fitted scaler
    """

    # Scale and fit with zero mean and unit variance
    if scaler is None:
        scaler = StandardScaler()
        df[numerical_features] = scaler.fit_transform(df[numerical_features])  # type: ignore
    else:
        df[numerical_features] = scaler.transform(df[numerical_features])

    return (df, scaler)


def separate_train_evaluate_dataset(
    df: pd.DataFrame, size: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separates the evaluation dataset from the training dataset

    Parameters:
    df (pd.DataFrame): Pandas dataframe containing features and targets
    size (float): Fraction of data to use for evaluation

    Returns:
    Tuple[pd.DataFrame, pd.DataFrame]: Evaluation dataset, Training dataset
    """
    eval_df = df.iloc[int(len(df) * size) :]
    train_df = df.iloc[: int(len(df) * size)]
    return eval_df, train_df
