from pathlib import Path
from typing import Dict, Any

import pandas as pd
import pytest

from extract import DataExtractionStep


@pytest.fixture
def mock_context(tmp_path: Path) -> Dict[str, Any]:
    return {"data_path": str(tmp_path)}


def initialize_test_data(tmp_path: Path) -> None:
    """
    Initializes the full directory structure and creates realistic CSV files
    required by air quality, health, and socioeconomic extractors.
    """

    # Air quality data
    air_path = tmp_path / "air_quality_data" / "raw"
    air_path.mkdir(parents=True)
    air_file = air_path / "air_quality_with_province.csv"
    pd.DataFrame(
        {
            "Country": ["Spain"],
            "Air Quality Network": ["NET_ES131A"],
            "Air Quality Network Name": ["Ayto Madrid"],
            "Air Quality Station EoI Code": ["ES0125A"],
            "Air Quality Station Name": ["VILLAVERDE"],
            "Sampling Point Id": ["SP_28079017_8_8"],
            "Air Pollutant": ["NO2"],
            "Air Pollutant Description": ["Nitrogen dioxide (air)"],
            "Data Aggregation Process Id": ["P1Y"],
            "Data Aggregation Process": ["Annual mean / 1 calendar year"],
            "Year": ["1991"],
            "Air Pollution Level": [80.639],
            "Unit Of Air Pollution Level": ["ug/m3"],
            "Data Coverage": [94.77],
            "Verification": [None],
            "Air Quality Station Type": ["Background"],
            "Air Quality Station Area": ["urban"],
            "Longitude": [-3.705],
            "Latitude": [40.3469],
            "Altitude": [593.0],
            "City": ["Madrid"],
            "City Code": ["ES001K1"],
            "City Population": [5098717.0],
            "Source Of Data Flow": ["Reporting within EoI/AirBase 8"],
            "Calculation Time": ["31/12/2012 00:00:00"],
            "Link to raw data (only E1a/validated data from AQ e-Reporting)": [None],
            "Observation Frequency": [None],
            "Province": ["Madrid"],
        }
    ).to_csv(air_file, index=False)

    # Health data
    health_path = tmp_path / "health_data" / "raw"
    health_path.mkdir(parents=True)

    # Enfermedades respiratorias
    (health_path / "enfermedades_respiratorias.csv").write_text(
        "Causa de muerte;Sexo;Provincias;Periodo;Total\n"
        "062-067  X.Enfermedades del sistema respiratorio;Total;02 Albacete;2023;397\n"
    )

    # Esperanza de vida
    (health_path / "esperanza_vida.csv").write_text(
        "Sexo;Provincias;Periodo;Total\n" "Ambos sexos;02 Albacete;2023;83,61\n"
    )

    # Socioeconomic data
    socio_path = tmp_path / "socioeconomic_data" / "raw"
    socio_path.mkdir(parents=True)

    # PIB per capita
    (socio_path / "PIB per cap provincias 2000-2021.csv").write_text(
        "Provincia,Valor\n02 Albacete,21000\n"
    )

    # Población provincias
    (socio_path / "poblacion_provincias.csv").write_text(
        "Provincias;Sexo;Periodo;Total\n" "02 Albacete;Total;2021;386.464\n"
    )


def test_real_data_extraction_step(tmp_path: Path, mock_context: Dict[str, Any]):
    """
    Tests that DataExtractionStep correctly runs all real extractors
    after setting up the expected folder and file structure.
    """

    # Prepare directory structure and input files
    initialize_test_data(tmp_path)

    # Execute extraction step
    dataframes: Dict[str, pd.DataFrame] = {}
    step = DataExtractionStep()
    step.execute(dataframes, {"data_path": tmp_path})

    # Assertions
    assert "air_quality" in dataframes
    assert "respiratory_diseases" in dataframes
    assert "life_expectancy" in dataframes
    assert "gdp" in dataframes
    assert "province_population" in dataframes

    for key in (
        "air_quality",
        "respiratory_diseases",
        "life_expectancy",
        "gdp",
        "province_population",
    ):
        df = dataframes[key]
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
