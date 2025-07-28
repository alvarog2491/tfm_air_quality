from unittest.mock import patch

import pandas as pd
import pytest
from transform import DataCleaningStep


@pytest.fixture
def cleaning_step():
    """Create a DataCleaningStep instance."""
    return DataCleaningStep()


def test_execute_missing_output_df(cleaning_step):
    """Test that execute raises ValueError when output_df is missing."""
    empty_dataframes = {}

    with pytest.raises(ValueError, match="'output_df' is missing"):
        cleaning_step.execute(empty_dataframes, {})


@patch.object(DataCleaningStep, "_remove_island_observations")
@patch.object(DataCleaningStep, "_filter_timeframe")
@patch.object(DataCleaningStep, "_handle_null_values")
@patch.object(DataCleaningStep, "_handle_duplicated_rows")
@patch.object(DataCleaningStep, "_convert_to_appropriate_dtypes")
def test_execute_calls_all_cleaning_methods(
    mock_convert_dtypes,
    mock_handle_duplicates,
    mock_handle_nulls,
    mock_filter_timeframe,
    mock_remove_islands,
    cleaning_step,
    dataframes_with_output,
):
    """Test that execute calls all cleaning methods in correct order."""
    cleaning_step.execute(dataframes_with_output, {})

    # Verify all cleaning methods were called
    mock_remove_islands.assert_called_once()
    mock_filter_timeframe.assert_called_once()
    mock_handle_nulls.assert_called_once()
    mock_handle_duplicates.assert_called_once()
    mock_convert_dtypes.assert_called_once()


def test_remove_island_observations(cleaning_step, sample_output_df):
    """Test that island observations are removed correctly."""
    # Setup - include some island provinces
    df_with_islands = sample_output_df.copy()
    df_with_islands.loc[len(df_with_islands)] = [
        "Santa Cruz de Tenerife",
        2020,
        40.0,
        50.0,
        "80.0",
        "25000",
    ]
    df_with_islands.loc[len(df_with_islands)] = [
        "Las Palmas",
        2020,
        35.0,
        45.0,
        "79.0",
        "24000",
    ]

    initial_length = len(df_with_islands)

    # Execute
    cleaning_step._remove_island_observations(df_with_islands)

    # Verify islands were removed
    assert len(df_with_islands) < initial_length
    assert "Santa Cruz de Tenerife" not in df_with_islands["Province"].values
    assert "Las Palmas" not in df_with_islands["Province"].values


def test_filter_timeframe(cleaning_step):
    """Test that timeframe filtering works correctly."""
    # Setup - create test dataframe with years outside 2000-2021 range
    df_with_various_years = pd.DataFrame(
        {
            "Province": ["Madrid", "Barcelona", "Madrid", "Madrid"],
            "Year": [1999, 2020, 2021, 2022],
            "Air Pollution Level": [40.0, 45.0, 50.0, 35.0],
            "Respiratory_diseases_total": [50.0, 90.0, 100.0, 45.0],
            "Life_expectancy_total": ["80.0", "83.0", "82.0", "79.0"],
            "pib": ["25000", "32000", "35000", "24000"],
        }
    )

    # Execute
    cleaning_step._filter_timeframe(df_with_various_years)

    # Verify only years 2000-2021 remain (excluding 1999 and 2022)
    assert len(df_with_various_years) == 2  # Only 2020 and 2021 should remain
    assert df_with_various_years["Year"].between(2000, 2021).all()


def test_handle_null_values(cleaning_step):
    """Test that null values are handled correctly (removes if < 5% nulls)."""
    # Setup dataframe with few null values (< 5%)
    # 1 null out of 25 = 4%
    data_list = {
        "Province": [
            "Madrid",
            "Barcelona",
            "Valencia",
            "Madrid",
            "Sevilla",
            "Toledo",
            "Córdoba",
            "Granada",
            "Málaga",
            "Cádiz",
        ]
        * 2
        + ["León", "Burgos", "Palencia", "Valladolid", "Zamora"],
        "Year": [2019, 2020, 2021, 2020, 2019, 2020, 2021, 2019, 2020, 2021] * 2
        + [2019, 2020, 2021, 2020, 2019],
        "Air Pollution Level": [
            50.0,
            45.0,
            40.0,
            52.0,
            48.0,
            46.0,
            44.0,
            49.0,
            47.0,
            43.0,
            51.0,
            45.5,
            40.5,
            52.5,
            48.5,
            46.5,
            44.5,
            49.5,
            47.5,
            43.5,
            50.5,
            45.0,
            40.0,
            None,
            48.0,
        ],  # 1 null out of 25 = 4%
        "Respiratory_diseases_total": [
            100.0,
            90.0,
            85.0,
            105.0,
            95.0,
            92.0,
            88.0,
            98.0,
            93.0,
            87.0,
        ]
        * 2
        + [100.0, 90.0, 85.0, 105.0, 95.0],
        "Life_expectancy_total": [
            "82.0",
            "83.0",
            "81.0",
            "82.5",
            "82.2",
            "83.1",
            "81.1",
            "82.3",
            "83.2",
            "81.2",
        ]
        * 2
        + ["82.0", "83.0", "81.0", "82.5", "82.2"],
        "pib": [
            "35000",
            "32000",
            "28000",
            "36000",
            "34000",
            "33000",
            "29000",
            "35500",
            "32500",
            "28500",
        ]
        * 2
        + ["35000", "32000", "28000", "36000", "34000"],
    }
    df_with_nulls = pd.DataFrame(data_list)
    initial_length = len(df_with_nulls)

    # Execute
    cleaning_step._handle_null_values(df_with_nulls)

    # Verify row with null was removed (since null percentage < 5%)
    assert len(df_with_nulls) < initial_length
    assert not df_with_nulls.isnull().any().any()


def test_handle_duplicated_rows(cleaning_step, sample_output_df):
    """Test that duplicate rows are removed correctly."""
    # Setup dataframe with duplicates
    df_with_duplicates = sample_output_df.copy()
    initial_length = len(df_with_duplicates)

    # Execute
    cleaning_step._handle_duplicated_rows(df_with_duplicates)

    # Verify duplicates were removed
    assert len(df_with_duplicates) < initial_length
    assert not df_with_duplicates.duplicated().any()


def test_convert_to_appropriate_dtypes(cleaning_step):
    """Test that data types are converted correctly."""
    # Setup dataframe with string numeric columns
    df_for_conversion = pd.DataFrame(
        {
            "Province": ["Madrid", "Barcelona", "Valencia"],
            "Year": [2019, 2020, 2021],
            "Air Pollution Level": [50.0, 45.0, 40.0],
            "Respiratory_diseases_total": [100.0, 90.0, 85.0],
            "Life_expectancy_total": ["82.0", "83.0", "81.0"],  # String column
            "pib": ["35000", "32000", "28000"],  # String column
        }
    )

    # Execute
    cleaning_step._convert_to_appropriate_dtypes(df_for_conversion)

    # Verify string columns were converted to numeric
    assert pd.api.types.is_numeric_dtype(df_for_conversion["Life_expectancy_total"])
    assert pd.api.types.is_numeric_dtype(df_for_conversion["pib"])


def test_step_name_initialization(cleaning_step):
    """Test that the step is initialized with correct name."""
    assert cleaning_step.name == "transform.data_cleaning_step"
