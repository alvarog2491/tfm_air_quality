from typing import List, Tuple
import pandas as pd
from sklearn.preprocessing import StandardScaler

def load_raw_dataset(
    filepath: str, drop_columns: List[str] = None, var_dtypes: dict = None
) -> pd.DataFrame:
    """
    Load the raw dataset from a CSV file and drop specified columns.

    Parameters:
    filepath (str): Path to the raw data CSV file.
    drop_columns (List[str]): List of column names to be removed from the dataframe.

    Returns:
    pd.DataFrame: DataFrame after dropping specified columns.
    """
    df = pd.read_csv(filepath, dtype=var_dtypes)
    if drop_columns:
        df = df.drop(columns=drop_columns, errors='ignore')
    # Ensure the index is reset after dropping columns
    df.reset_index(drop=True, inplace=True)
    # Validate that the dataframe is not empty
    if df.empty:
        raise ValueError("The loaded dataset is empty. Please check the file path and content.")

    return df

def prepare_features_and_target(
    filepath: str,
    target_column: str,
    shuffle: bool,
    shuffle_random_state: int,
    var_dtypes: dict = None,
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
        data = data.sample(frac=1, random_state=shuffle_random_state).reset_index(drop=True)

    X = data.drop(columns=target_column)
    y = data[target_column]
    return X, y

def validate_no_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates that the dataframe contains no missing values; raises an error if any are found.

    Parameters:
    df (pd.DataFrame): Pandas dataframe containing features and targets

    Returns:
    pd.DataFrame: The original dataframe if validation passes.
    """
    if df.isnull().values.any():
        raise ValueError("Data contains missing values. Please handle them before proceeding.")

    return df

def one_hot_encode_categorical_features(
    df: pd.DataFrame, categorical_columns: List[str]) -> pd.DataFrame:
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


def scale_numerical_features(df: pd.DataFrame, numerical_features: List[str], scaler = None) -> Tuple[pd.DataFrame, StandardScaler]:
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
        df[numerical_features] = scaler.fit_transform(df[numerical_features])
    else:
        df[numerical_features] = scaler.transform(df[numerical_features])
    
    return df, scaler

def separate_train_evaluate_dataset(df: pd.DataFrame, size: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separates the evaluation dataset from the training dataset

    Parameters:
    df (pd.DataFrame): Pandas dataframe containing features and targets
    size (float): Fraction of data to use for evaluation

    Returns:
    Tuple[pd.DataFrame, pd.DataFrame]: Evaluation dataset, Training dataset
    """
    eval_df = df.iloc[int(len(df) * size):]
    train_df = df.iloc[:int(len(df) * size)]
    return eval_df, train_df
