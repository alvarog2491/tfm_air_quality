import pytest
import pandas as pd
import json
from pathlib import Path
from pandas.api.types import is_categorical_dtype
from utils.province_mapper import ProvinceMapper

# Mock logger to suppress real logging
class MockLogger:
    def info(self, msg):
        pass

    def warning(self, msg):
        pass


@pytest.fixture(autouse=True)
def setup_mocks(monkeypatch):
    ProvinceMapper.logger = MockLogger()

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
    """Test basic functionallity maping provinces"""
    
    result = ProvinceMapper.map_province_name("Test DF", df_input)
    pd.testing.assert_series_equal(result["Province"], df_expected["Province"])

def test_correct_type_province_type(df_input):
    """After mapping the province, the df province variable should be transformed to category"""

    result = ProvinceMapper.map_province_name("Test DF", df_input)
    assert isinstance(result["Province"].dtype, pd.CategoricalDtype)

def test_map_non_existing_province(df_input):
    """If a province is unrecognized, it should not be mapped by anything"""
    unknown_province = pd.DataFrame({"Province": ["something"]})
    df_input = pd.concat([df_input, unknown_province])
    
    result = ProvinceMapper.map_province_name("Test DF", df_input)
    
    assert "something" in result["Province"].values



