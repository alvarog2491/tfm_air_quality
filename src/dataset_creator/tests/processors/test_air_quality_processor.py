import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path
from processors.AirQualityProcessor import AirQualityProcessor

@pytest.fixture
def sample_df():
    """Sample DataFrame similar to expected air quality data."""

    return pd.DataFrame({
        'Air Pollutant': ['no2', 'pm10'],
        'Air Pollutant Description': ['Nitrogen dioxide (air)', 'Particulate matter'],
        'Data Aggregation Process': ['Annual mean / 1 calendar year', 'Annual mean / 1 calendar year'],
        'Year': pd.to_datetime(['1991-01-01', '1991-01-01']),
        'Air Pollution Level': [80.6, 50.0],
        'Unit Of Air Pollution Level': ['ug/m3', 'ug/m3'],
        'Air Quality Station Type': ['Background', 'Background'],
        'Air Quality Station Area': ['urban', 'urban'],
        'Altitude': [593.0, 600.0],
        'Longitude': [0.0, 0.0],
        'Latitude': [0.0, 0.0],
        'Province': ['Madrid', 'Madrid'],
    })
import pytest

@pytest.fixture
def air_quality_raw_csv_data():
    """
    Fixture that returns a sample CSV content string mimicking the real raw air quality data file.
    """
    return """Country,Air Quality Network,Air Quality Network Name,Air Quality Station EoI Code,Air Quality Station Name,Sampling Point Id,Air Pollutant,Air Pollutant Description,Data Aggregation Process Id,Data Aggregation Process,Year,Air Pollution Level,Unit Of Air Pollution Level,Data Coverage,Verification,Air Quality Station Type,Air Quality Station Area,Longitude,Latitude,Altitude,City,City Code,City Population,Source Of Data Flow,Calculation Time,Link to raw data (only E1a/validated data from AQ e-Reporting),Observation Frequency,Province
Spain,NET_ES131A,Ayto Madrid,ES0125A,VILLAVERDE,SP_28079017_8_8,NO2,Nitrogen dioxide (air),P1Y,Annual mean / 1 calendar year,1991,80.639,ug/m3,94.77,,Background,urban,-3.705,40.3469,593.0,Madrid,ES001K1,5098717.0,Reporting within EoI/AirBase 8,31/12/2012 00:00:00,,,Madrid
"""


@pytest.fixture
def processor(tmp_path):
    """Create an AirQualityProcessor instance with a temporary data folder."""

    return AirQualityProcessor(data_folder=tmp_path)

def test_load_csv_files_success(processor, tmp_path, air_quality_csv_data):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    csv_path = raw_dir / "air_quality_with_province.csv"
    csv_path.write_text(air_quality_csv_data)
    
    processor.load_csv_files()
    
    assert processor.is_loaded
    df = processor.air_quality_df
    assert set(df.columns) == set(processor._COLUMN_DTYPES.keys())
    assert pd.api.types.is_categorical_dtype(df['Air Pollutant'])


def test_load_csv_files_file_not_found(processor):
    """Test load_csv_files raises FileNotFoundError if CSV missing."""

    with pytest.raises(FileNotFoundError):
        processor.load_csv_files()

def test_classify_quality_assigns_labels(processor, sample_df):
    """Test classify_quality adds 'Quality' column with correct categories."""

    # Inject sample data directly
    processor._air_quality_df = sample_df.copy()

    # Patch quality_thresholds and quality_labels for test
    with patch("utils.air_quality_rules.quality_thresholds", {'no2': [0, 40, 80, 120]}), \
         patch("utils.air_quality_rules.quality_labels", ['LOW', 'MEDIUM', 'HIGH']):

        processor.classify_quality()

        # Check 'Quality' column added
        assert 'Quality' in processor.air_quality_df.columns
        # Check type is categorical
        assert pd.api.types.is_categorical_dtype(processor.air_quality_df['Quality'])
        # Check known classification label
        assert processor.air_quality_df.loc[0, 'Quality'] in ['LOW', 'MEDIUM', 'HIGH']

def test_classify_quality_without_data_raises(processor):
    """Test classify_quality raises ValueError if data not loaded."""

    processor._air_quality_df = None
    with pytest.raises(ValueError):
        processor.classify_quality()

def test_map_province_names_calls_mapper(processor, sample_df):
    """Test map_province_names calls ProvinceMapper and replaces DataFrame."""

    processor._air_quality_df = sample_df.copy()

    # Patch ProvinceMapper.map_province_name to a mock function
    with patch("utils.province_mapper.ProvinceMapper.map_province_name", return_value=sample_df) as mock_mapper:
        processor.map_province_names()
        mock_mapper.assert_called_once()
        assert processor.air_quality_df.equals(sample_df)

def test_save_processed_file_saves_csv(processor, sample_df, tmp_path):
    """Test save_processed_file saves CSV file to 'processed' folder."""

    processor._air_quality_df = sample_df.copy()

    # Run save processed file
    processor.save_processed_file()

    processed_file = tmp_path / "air_quality_data" / "processed" / "air_quality.csv"
    # The processed folder is relative to data_folder + "air_quality_data"
    expected_path = processor.data_folder / "processed" / "air_quality.csv"
    assert expected_path.exists()
    # Read back the file and check content
    saved_df = pd.read_csv(expected_path)
    assert not saved_df.empty
    assert 'Air Pollutant' in saved_df.columns

def test_save_processed_file_raises_if_no_data(processor):
    """Test save_processed_file raises ValueError if no data to save."""

    processor._air_quality_df = None
    with pytest.raises(ValueError):
        processor.save_processed_file()

def test_process_runs_full_pipeline(processor, sample_df, tmp_path):
    """
    Test full process() method runs all steps without error.
    Mocks load_csv_files, classify_quality, map_province_names and save_processed_file.
    """

    # Prepare CSV for load_csv_files to read
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    csv_path = raw_dir / "air_quality_with_province.csv"
    sample_df.to_csv(csv_path, index=False)

    # Patch classify_quality, map_province_names and save_processed_file to mocks to track calls
    with patch.object(processor, 'classify_quality') as mock_classify, \
         patch.object(processor, 'map_province_names') as mock_map_province, \
         patch.object(processor, 'save_processed_file') as mock_save:

        processor.process()

        mock_classify.assert_called_once()
        mock_map_province.assert_called_once()
        mock_save.assert_called_once()
