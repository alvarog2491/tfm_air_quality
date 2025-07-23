import logging
from typing import List, Optional, Dict

import pandas as pd


def load_raw_dataset(
    filepath: str,
    drop_columns: Optional[List[str]] = None,
    use_cols: Optional[List[str]] = None,
    var_dtypes: Optional[Dict[str, str]] = None,
    parse_dates: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Load the raw dataset from a CSV file, optionally selecting and dropping specified columns.

    Parameters:
    - filepath (str): Path to the raw data CSV file.
    - drop_columns (List[str], optional): Columns to drop after loading. Ignored if None.
    - use_cols (List[str], optional): Columns to load from the CSV. If None, load all.
    - var_dtypes (dict, optional): Dictionary of column dtypes.
    - parse_dates (List[str], optional): Columns to parse as dates.

    Returns:
    - pd.DataFrame: Loaded DataFrame after processing.

    Raises:
    - ValueError: If resulting DataFrame is empty.
    """

    # If both use_cols and drop_columns are set, check for overlaps
    if use_cols and drop_columns:
        overlap = set(use_cols) & set(drop_columns)
        if overlap:
            raise ValueError(
                f"Conflict detected: Columns {overlap} are present in both 'use_cols' and 'drop_columns'."
            )

    df = pd.read_csv(filepath, usecols=use_cols, dtype=var_dtypes, parse_dates=parse_dates)  # type: ignore

    if drop_columns:
        df = df.drop(columns=drop_columns, errors="ignore")

    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError(
            "The loaded dataset is empty after applying filters. Check file, columns, or filters."
        )

    return df


def validate_no_missing_values(df: pd.DataFrame) -> None:
    """
    Validates that the dataframe contains no missing values; raises an error if any are found.

    Parameters:
    df (pd.DataFrame): Pandas dataframe containing features and targets

    Returns:
    pd.DataFrame: The original dataframe if validation passes.
    """
    if df.isnull().values.any():
        raise ValueError(
            "Data contains missing values. Please handle them before proceeding."
        )


def remove_commas_and_dots(
    df: pd.DataFrame, columns: List[str], convert_to: type
) -> None:
    """
    Removes all commas (',') and dots ('.') from the specified columns and converts the result to the given type.

    Args:
        df: The DataFrame containing the target columns.
        columns: List of column names to clean and convert.
        convert_to: Data type to convert the cleaned values to (e.g., int, float).

    Returns:
        None. The DataFrame is modified in place.
    """
    for column in columns:
        df[column] = (
            df[column]
            .astype(str)
            .str.replace(",", ".")
            .str.replace(".", "")
            .astype(convert_to)
        )
        logging.info(f"Removed ',' and '.' on '{column}' column")


def remove_dots(df: pd.DataFrame, columns: List[str], convert_to: type) -> None:
    """
    Removes all dots ('.') from string values in the specified columns and converts them to a given type.

    Args:
        df: The DataFrame containing the target columns.
        columns: List of column names to clean and convert.
        convert_to: Target data type to cast the cleaned values to (e.g., int or float).

    Returns:
        None. The input DataFrame is modified in place.
    """
    for column in columns:
        df[column] = df[column].astype(str).str.replace(".", "").astype(convert_to)
        logging.info(f"Removed ',' and '.' on '{column}' column")
