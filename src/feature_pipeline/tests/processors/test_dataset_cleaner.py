from datetime import datetime

import pandas as pd
import pytest
from processors.dataset_cleaner import DatasetCleaner


@pytest.fixture
def raw_dataset():
    return pd.DataFrame(
        {
            "Province": [
                "Madrid",
                "Santa Cruz de Tenerife",
                "Las Palmas",
                "Illes Balears",
                "Ceuta",
                "Melilla",
                "Barcelona",
                "Desconocido",
                "Error",
                "Madrid",
                "Santa Cruz de Tenerife",
                "Las Palmas",
                "Illes Balears",
                "Ceuta",
                "Melilla",
                "Barcelona",
                "Desconocido",
                "Error",
                "Madrid",
                "Barcelona",
                "Madrid",
                "Barcelona",
                "Madrid",
                "Barcelona",
                None,  # 1 null (<5%)
            ],
            "Year": [
                datetime(2005, 1, 1),
                datetime(2010, 1, 1),
                datetime(2015, 1, 1),
                datetime(2018, 1, 1),
                datetime(2020, 1, 1),
                datetime(2021, 1, 1),
                datetime(2023, 1, 1),
                datetime(2012, 1, 1),
                datetime(2008, 1, 1),
                datetime(2004, 1, 1),
                datetime(2005, 1, 1),
                datetime(2010, 1, 1),
                datetime(2015, 1, 1),
                datetime(2018, 1, 1),
                datetime(2020, 1, 1),
                datetime(2021, 1, 1),
                datetime(2012, 1, 1),
                datetime(2008, 1, 1),
                datetime(2004, 1, 1),
                datetime(2019, 1, 1),
                datetime(2020, 1, 1),
                datetime(2021, 1, 1),
                datetime(2022, 1, 1),
                datetime(2022, 1, 1),
                datetime(2007, 1, 1),
            ],
            "Value": list(range(25)),
        }
    )


@pytest.fixture
def dataset_with_many_nulls():
    """
    Provides a dataset where 'Value' column has 20% nulls.
    Used to test that rows are not dropped when nulls exceed the threshold.
    """
    return pd.DataFrame(
        {
            "Province": ["Madrid"] * 20,
            "Year": [datetime(2020, 1, 1)] * 20,
            "Value": list(range(16)) + [None] * 4,  # 4 nulls de 20 → 20%
        }
    )


def test_dataset_loading(raw_dataset):
    """
    Tests that the dataset is correctly loaded and the dataset property is properly set.
    """
    cleaner = DatasetCleaner()
    assert cleaner.is_dataset_loaded is False

    cleaner.clean_dataset(raw_dataset)
    assert cleaner.is_dataset_loaded is True
    assert isinstance(cleaner.dataset, pd.DataFrame)


def test_remove_null_provinces(raw_dataset):
    """
    Tests that all rows with null provinces are removed after cleaning.
    """
    cleaner = DatasetCleaner()
    cleaner.clean_dataset(raw_dataset)

    assert cleaner.dataset["Province"].isnull().sum() == 0


def test_not_remove_rows_when_nulls_exceed_threshold(dataset_with_many_nulls):
    """
    Tests that rows are not removed when a column has >= 5% null values.
    """
    cleaner = DatasetCleaner()

    original_length = len(dataset_with_many_nulls)

    cleaner.clean_dataset(dataset_with_many_nulls)
    cleaned_df = cleaner.dataset

    assert (
        len(cleaned_df) == original_length
    ), f"Expected {original_length} rows, but got {len(cleaned_df)}"

    assert (
        cleaned_df["Value"].isnull().sum() == 4
    ), f"Expected 4 nulls in 'Value', but got {cleaned_df['Value'].isnull().sum()}"


def test_remove_island_observations(raw_dataset):
    """
    Tests that island provinces are completely removed from the dataset.
    """
    cleaner = DatasetCleaner()
    cleaned_dataset = cleaner.clean_dataset(raw_dataset)

    island_provinces = [
        "Santa Cruz de Tenerife",
        "Las Palmas",
        "Illes Balears",
        "Ceuta",
        "Melilla",
    ]
    assert not cleaned_dataset["Province"].isin(island_provinces).any()


def test_remove_undefined_provinces(raw_dataset):
    """
    Tests that undefined provinces like 'Desconocido' and 'Error' are removed from the dataset.
    """
    cleaner = DatasetCleaner()
    cleaned_dataset = cleaner.clean_dataset(raw_dataset)

    undefined_provinces = ["Desconocido", "Error"]
    assert not cleaned_dataset["Province"].isin(undefined_provinces).any()


def test_filter_timeframe_with_datetime(raw_dataset):
    """
    Tests that only observations between 2000 and 2022 (inclusive) remain when 'Year' is a datetime object.
    """
    cleaner = DatasetCleaner()
    cleaned_dataset = cleaner.clean_dataset(raw_dataset)

    assert cleaned_dataset["Year"].dt.year.min() >= 2000
    assert cleaned_dataset["Year"].dt.year.max() <= 2022
    assert not cleaned_dataset["Year"].dt.year.gt(2022).any()


def test_dataset_return_type(raw_dataset):
    """
    Tests that the clean_dataset method always returns a pandas DataFrame.
    """
    cleaner = DatasetCleaner()
    cleaned_dataset = cleaner.clean_dataset(raw_dataset)

    assert isinstance(cleaned_dataset, pd.DataFrame)
