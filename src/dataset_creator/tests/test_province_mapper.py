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


@pytest.fixture(autouse=True)
def setup_mocks(monkeypatch):
    ProvinceMapper.logger = MockLogger()
    
    # Mock _load_json_file and _check_provinces as no-ops
    monkeypatch.setattr(ProvinceMapper, "_load_json_file", lambda: None)
    monkeypatch.setattr(ProvinceMapper, "_check_provinces", lambda df: None)


@pytest.fixture(autouse=True)
def real_province_mapping():
    json_path = Path(__file__).parent.parent / 'utils' / 'unified_province_name.json'
    if not json_path.is_file():
        raise FileNotFoundError(f"Expected file not found: {json_path}")
    
    with open(json_path, "r", encoding="utf-8") as f:
        ProvinceMapper.unified_province_dict = json.load(f)


def test_map_province_name_success():
    df_input = pd.DataFrame({
        "Province": ["CiudadReal", "Palmas,Las"]
    })

    expected = pd.DataFrame({
        "Province": pd.Categorical(["Ciudad Real", "Las Palmas"])
    })

    result = ProvinceMapper.map_province_name("Test DF", df_input)

    pd.testing.assert_series_equal(result["Province"], expected["Province"])
    assert is_categorical_dtype(result["Province"])
