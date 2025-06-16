---
editor_options: 
  markdown: 
    wrap: 72
---

# Spanish Multi-Source Data Pipeline

A complete data processing pipeline that brings together air quality,
health, and socioeconomic data from Spanish provinces, all set up for
further analysis or machine learning tasks.

## Table of Contents

-   [Description](#description)
-   [Data Sources](#data-sources)
-   [Data Processing Pipeline](#data-processing-pipeline)
-   [Usage](#usage)
-   [Main Orchestrator](#main-orchestrator)
-   [Province Name Standardization](#province-name-standardization)
-   [Output](#output)

## Description {#description}

This pipeline builds a unified dataset combining three key types of
data:

-   **Air Quality**: Pollutant levels recorded by sensors across Spain.
-   **Health**: Life expectancy and deaths related to respiratory
    diseases by province.
-   **Socioeconomic**: GDP per capita by province, from 2000 to 2022.

All records are standardized to share the same structure with `Province`
and `Year` as primary keys.

## Data Sources {#data-sources}

### Air Quality

-   [European Environment Agency
    (EEA)](https://discomap.eea.europa.eu/App/AQViewer/index.html?fqn=Airquality_Dissem.b2g.AirQualityStatistics&Country=Spain&inAQReportYN=Yes):
    Data for PM2.5, PM10, NO2, SO2, O3.
-   BOE: Classification of air quality into 6 categories (from "buena"
    to "extremadamente desfavorable").

### Health

-   [INE](https://www.ine.es/jaxiT3/Tabla.htm?t=9935&L=0): Mortality
    data for diseases of the respiratory system (codes 062-067).
-   [INE:](https://www.ine.es/jaxiT3/Tabla.htm?t=1485) Life expectancy
    data by province and gender.

### Socioeconomic

-   GDP per capita data from 2000 to 2022 by province.

## Data Processing Pipeline {#data-processing-pipeline}

The pipeline runs in four phases:

### 1. Air Quality

Reads pollutant data, adds classification based on thresholds.

Example:

INPUT: "air_quality_with_province.csv"

| Air Pollutant | Air Pollutant Description | Data Aggregation Process | Year | Air Pollution Level |
|---------------|---------------|---------------|---------------|---------------|
| no2 | Nitrogen dioxide (air) | Annual mean / 1 calendar year | 1991-01-01 | 80.639 |

| Unit Of Air Pollution Level | Air Quality Station Type | Air Quality Station Area | Altitude | Province |
|---------------|---------------|---------------|---------------|---------------|
| ug/m3 | Background | urban | 593.0 | Madrid |

OUTPUT: Same columns + Quality classification

| Air Pollutant | Air Pollutant Description | Data Aggregation Process | Year | Air Pollution Level |
|---------------|---------------|---------------|---------------|---------------|
| no2 | Nitrogen dioxide (air) | Annual mean / 1 calendar year | 1991-01-01 | 80.639 |

| Unit Of Air Pollution Level | Air Quality Station Type | Air Quality Station Area | Altitude | Province | Quality |
|------------|------------|------------|------------|------------|------------|
| ug/m3 | Background | urban | 593.0 | Madrid | RAZONABLEMENTE BUENA |

### 2. Health

Combines mortality and life expectancy data into a unified format.

Example:

INPUT:

-    "enfermedades_respiratorias.csv":

| Causa de muerte | Sexo | Provincias | Periodo | Total |
|-------------------|--------------|--------------|--------------|--------------|
| 062-067 X.Enfermedades del sistema respiratorio | Total | 02 Albacete | 2023-01-01 | 397.0 |

-   "esperanza_vida.csv":

| Sexo        | Provincias  | Periodo    | Total |
|-------------|-------------|------------|-------|
| Ambos sexos | 02 Albacete | 2023-01-01 | 83.61 |

OUTPUT:

| Province | Period     | Respiratory_diseases_total | Life_expectancy_total |
|----------|------------|----------------------------|-----------------------|
| Albacete | 2023-01-01 | 397                        | 83.61                 |

### 3. Socioeconomic

Converts wide-format GDP data to long format for consistency.

Example:

INPUT: "PIB per cap provincias 2000-2021.csv" (wide format with years as
columns)

| Provincia | 2000  | 2001  | 2002  | ... | 2021  |
|-----------|-------|-------|-------|-----|-------|
| Alava     | 22134 | 23917 | 25679 | ... | 35924 |

OUTPUT: Normalized long format

| Province | anio       | pib     |
|----------|------------|---------|
| Alava    | 2000-01-01 | 22134.0 |
| Alava    | 2001-01-01 | 23917.0 |

### 4. Merge

Combines the outputs of the above processors into one single, unified
dataset.

## Usage {#usage}

### Run Everything

To execute the full pipeline:

``` bash
python main.py
```

This will: - Check folder structure - Process all datasets - Merge and
export final dataset

### Run Individual Steps

``` python
from processors.AirQualityProcessor import AirQualityProcessor
AirQualityProcessor().process()

from processors.HealthProcessor import HealthProcessor
HealthProcessor().process()

from processors.SocioeconomicProcessor import SocioeconomicProcessor
SocioeconomicProcessor().process()

from processors.DataMerger import DataMerger
DataMerger().process()
```

## Main Orchestrator {#main-orchestrator}

The script `main.py` controls the full workflow.

It: - Verifies that input folders exist - Runs each processor in
sequence - Logs each step (with time taken) - Saves a versioned and
general CSV to `data/output/`

Logs are printed to the console and include: - Start/End messages for
each step - Number of rows and columns in the result - Time taken - Any
errors with full tracebacks

## Province Name Standardization {#province-name-standardization}

Names like `"02 Albacete"` or `"Alicante/Alacant"` are cleaned up using
a mapping defined in a JSON file, so merging data from different sources
works smoothly.

**Example standardizations:**\
- `"02 Albacete"` → `"Albacete"`\
- `"Alicante/Alacant"` → `"Alicante"`\
- `"A Coruña"` → `"A Coruña"`

## Output {#output}

The final result is saved to:

-   `data/output/dataset.csv` – current result
-   `data/output/dataset_YYYYMMDD_HHMMSS.csv` – versioned snapshot

Description of output variables:

**Province**: Name of the province where all data is aggregated.
**Year**: Date or year of the data measurement.
**Air_Pollutant**: Code or abbreviation of the air pollutant
measured (e.g., no2).
**Air_Pollutant_Description**: Full name and description of the air
pollutant.
**Data_Aggregation_Process**: Method and time frame used to
aggregate the data (e.g., annual mean).
**Air_Pollution_Level**: Measured concentration value of the
pollutant.
**Unit**: Unit of measurement for the pollution level (e.g.,
micrograms per cubic meter).
**Air_Quality_Station_Type**: Type of station where measurements
were taken (e.g., traffic, background).
**Air_Quality_Station_Area**: Classification of the station’s
location area (e.g., urban, suburban).
**Altitude**: Altitude of the monitoring station in meters above sea
level.
**Quality**: Quality assessment or rating of the air quality.
**Respiratory_Diseases_Total**: Total number of respiratory disease
cases reported in the province.
**Life_Expectancy**: Average life expectancy in the province.
**GDP_per_capita**: Gross Domestic Product per capita in the
province.
