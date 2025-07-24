import logging
from typing import Dict, List, Optional

import pandas as pd


def load_raw_dataset(
    filepath: str,
    drop_columns: Optional[List[str]] = None,
    use_cols: Optional[List[str]] = None,
    var_dtypes: Optional[Dict[str, str]] = None,
    parse_dates: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Load a raw dataset from a CSV file, with optional column selection, type casting, date parsing, and column dropping.

    Args:
        filepath (str): Path to the input CSV file.
        drop_columns (List[str], optional): Columns to drop after loading. Ignored if None.
        use_cols (List[str], optional): Subset of columns to load. If None, all columns are loaded.
        var_dtypes (dict, optional): Data types to apply to specific columns.
        parse_dates (List[str], optional): Columns to parse as datetime.

    Returns:
        pd.DataFrame: Loaded and preprocessed DataFrame.

    Raises:
        ValueError: If any column appears in both use_cols and drop_columns.
        ValueError: If the resulting DataFrame is empty.
    """
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
        raise ValueError("The loaded dataset is empty after applying filters.")

    return df


def validate_no_missing_values(df: pd.DataFrame) -> None:
    """
    Raise an error if the DataFrame contains any missing values.

    Args:
        df (pd.DataFrame): DataFrame to validate.

    Raises:
        ValueError: If missing values are found.
    """
    if df.isnull().values.any():
        raise ValueError(
            "Data contains missing values. Please handle them before proceeding."
        )


def remove_commas_and_dots(
    df: pd.DataFrame, columns: List[str], convert_to: type
) -> None:
    """
    Remove commas and dots from string values in the given columns, and convert the result to a specified type.

    Args:
        df (pd.DataFrame): DataFrame containing the target columns.
        columns (List[str]): Columns to clean and convert.
        convert_to (type): Target data type (e.g., int, float).

    Returns:
        None. The DataFrame is modified in place.
    """
    for column in columns:
        df[column] = (
            df[column]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(".", "", regex=False)
            .astype(convert_to)
        )
        logging.info(
            f"Removed ',' and '.' from '{column}' column and converted to {convert_to.__name__}"
        )


def remove_dots(df: pd.DataFrame, columns: List[str], convert_to: type) -> None:
    """
    Remove dots from string values in the specified columns and convert to a given type.

    Args:
        df (pd.DataFrame): DataFrame containing the target columns.
        columns (List[str]): Columns to clean and convert.
        convert_to (type): Target data type (e.g., int, float).

    Returns:
        None. The DataFrame is modified in place.
    """
    for column in columns:
        df[column] = (
            df[column].astype(str).str.replace(".", "", regex=False).astype(convert_to)
        )
        logging.info(
            f"Removed '.' from '{column}' column and converted to {convert_to.__name__}"
        )


def log_null_values(df: pd.DataFrame) -> None:
    """
    Log the count of null values per column, if any.

    Args:
        df (pd.DataFrame): DataFrame to check.
    """
    null_counts = df.isnull().sum()
    if null_counts.any():
        logging.warning(
            f"Null values detected in columns: {null_counts[null_counts > 0].to_dict()}"  # type: ignore
        )


def log_duplicated_rows(df: pd.DataFrame) -> None:
    """
    Log the number of duplicated rows in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to check.
    """
    duplicated_count = df.duplicated().sum()
    if duplicated_count:
        logging.warning(f"Duplicated rows found: {duplicated_count}")


def log_info(df: pd.DataFrame) -> None:
    """
    Log the structure and metadata of the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to describe.
    """
    logging.info("DataFrame info:")
    logging.info(df.info())


def log_empty_rows(df: pd.DataFrame) -> None:
    """
    Log the number of rows with more than 70% missing values.

    Args:
        df (pd.DataFrame): DataFrame to evaluate.
    """
    empty_rows = df.isnull().mean(axis=1) > 0.7
    if empty_rows.any():
        logging.warning(
            "%d rows contain more than 70%% missing values.", empty_rows.sum()
        )


def log_memory_usage(df: pd.DataFrame) -> None:
    """
    Log the approximate memory usage of the DataFrame in MB.

    Args:
        df (pd.DataFrame): DataFrame to analyze.
    """
    memory_usage = df.memory_usage(deep=True).sum() / 1024**2
    logging.info(f"(~{memory_usage:.1f} MB memory usage)")
