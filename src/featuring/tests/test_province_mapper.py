import pytest
import pandas as pd
import json
from pathlib import Path
from pandas.api.types import is_categorical_dtype
from utils.province_mapper import ProvinceMapper

@pytest.fixture()
def df_input():
    return pd.DataFrame({
        "Province": ["CiudadReal", "Palmas,Las"]
    })

@pytest.fixture()
def df_expected():
    return pd.DataFrame({
        "Province": pd.Categorical(["Ciudad Real", "Las Palmas"])
    })

def test_map_province_name_success(df_input, df_expected):
    """Test basic functionality of mapping province names correctly."""
    
    result = ProvinceMapper.map_province_name("Test DF", df_input)
    pd.testing.assert_series_equal(result["Province"], df_expected["Province"])

def test_correct_type_province_type(df_input):
    """Verify that the 'Province' column is converted to categorical dtype after mapping."""

    result = ProvinceMapper.map_province_name("Test DF", df_input)
    assert isinstance(result["Province"].dtype, pd.CategoricalDtype)

def test_map_non_existing_province(df_input):
    """Ensure that provinces not recognized in the mapping remain unchanged."""

    unknown_province = pd.DataFrame({"Province": ["something"]})
    df_input = pd.concat([df_input, unknown_province])
    
    result = ProvinceMapper.map_province_name("Test DF", df_input)
    
    assert "something" in result["Province"].values

def test_no_name_parameter():
    """Verify that a TypeError is raised when the required 'name' parameter is missing."""

    with pytest.raises(TypeError):
        ProvinceMapper.map_province_name(df=df_input)

def test_no_dataframe_parameter():
    """Verify that a TypeError is raised when the required 'df' parameter is missing."""

    with pytest.raises(TypeError):
        ProvinceMapper.map_province_name("Something")

def test_empty_dataframe():
    """Verify that a KeyError is raised if the dataframe is empty or lacks the 'Province' column."""

    with pytest.raises(KeyError):
        ProvinceMapper.map_province_name("Test DF", pd.DataFrame())
