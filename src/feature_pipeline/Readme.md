# Multi-Source Data Pipeline

This module performs common ETL steps to unify **air quality**, **health**, and **socioeconomic** datasets from official sources across Spanish provinces. It handles data loading, initial handling, cleaning, merging, and feature engineering to produce a single, unified dataset ready for analysis or machine learning.

---

## Table of Contents

- [Multi-Source Data Pipeline](#multi-source-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Data Sources](#data-sources)
    - [Air Quality](#air-quality)
    - [Health](#health)
    - [Socioeconomic](#socioeconomic)
  - [Data Processing Pipeline](#data-processing-pipeline)
    - [Air Quality](#air-quality-1)
    - [Health](#health-1)
    - [Socioeconomic](#socioeconomic-1)
    - [Merge](#merge)
    - [Cleansd](#cleansd)
  - [Usage](#usage)
    - [Run Everything](#run-everything)
    - [Run Individual Steps](#run-individual-steps)
  - [Main Orchestrator](#main-orchestrator)
  - [Province Name Standardization](#province-name-standardization)
  - [Output](#output)

---

## Description

The iteration works as follows: the source_processors folder contains modules responsible for reading the raw CSV files of the following types:

* Air Quality — Pollutant levels recorded by sensors across Spain.
* Health — Life expectancy and respiratory disease-related deaths by province.
* Socioeconomic — GDP per capita and population size by provinced.

Each processor reads its corresponding CSV file and performs initial operations such as basic data exploration, null value counting, info display, column renaming, and basic cleaning (e.g., removing unwanted characters or merging data of the same nature). After these steps, a cleaned and preprocessed CSV is generated for each dataset and stored in the corresponding output folder.

All records are standardized to share the same structure, using `Province` and `Year` as primary keys.

---

## Data Sources

All CSV files have been downloaded from the official links listed below. The raw data for each category can be found in the corresponding folder under data/*type*/raw/.
For example, air pollution data is located at:
data/air_quality_data/raw/

### Air Quality

- [EEA (European Environment Agency)](https://discomap.eea.europa.eu/App/AQViewer/index.html?fqn=Airquality_Dissem.b2g.AirQualityStatistics&Country=Spain&inAQReportYN=Yes):  
  Data for PM2.5, PM10, NO2, SO2, O3  
- [BOE](https://www.boe.es/buscar/doc.php?id=BOE-A-2020-10426): Classification of air quality into 6 categories (from "buena" to "extremadamente desfavorable")

### Health

- [INE – Mortality Data](https://www.ine.es/jaxiT3/Tabla.htm?t=9935&L=0):  
  Deaths due to respiratory system diseases (codes 062–067)  
- [INE – Life Expectancy](https://www.ine.es/jaxiT3/Tabla.htm?t=1485):  
  Life expectancy by province and gender

### Socioeconomic

- [GDP](https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736167628&menu=resultados&idp=1254735576581#_tabs-1254736158133) per capita by province from 2000 to 2022
- [Province population](https://www.ine.es/jaxiT3/Tabla.htm?t=2852)
---

## Data Processing Pipeline

The pipeline runs in four phases:

### Air Quality

Reads pollutant data and adds a classification column based on thresholds, collected from BOE.

**Input**: `"air_quality_with_province.csv"`  
**Output**: Same + air quality classification

| Air Pollutant | Air Pollutant Description | Data Aggregation Process      | Year       | Air Pollution Level | Unit Of Air Pollution Level |
| ------------- | ------------------------- | ----------------------------- | ---------- | ------------------- | --------------------------- |
| no2           | Nitrogen dioxide (air)    | Annual mean / 1 calendar year | 1991-01-01 | 80.639              | ug/m3                       |


| Air Quality Station Type | Air Quality Station Area | Longitude | Latitude | Altitude | Province | Quality              |
| ------------------------ | ------------------------ | --------- | -------- | -------- | -------- | -------------------- |
| Background               | urban                    | -3.705    | 40.347   | 593.000  | Madrid   | RAZONABLEMENTE BUENA |


---

### Health

Combines mortality and life expectancy data into a unified format.

**Input**:  
- `"enfermedades_respiratorias.csv"`  
- `"esperanza_vida.csv"`  

**Output**:

| Province | Year       | Respiratory Diseases | Life Expectancy |
|----------|------------|----------------------|-----------------|
| Albacete | 2023-01-01 | 397                  | 83.61           |

---

### Socioeconomic

Converts wide-format GDP data into long format.

**Input**: `"PIB per cap provincias 2000-2021.csv"`  
**Output**:

| Province | Year       | GDP per Capita |
|----------|------------|----------------|
| Alava    | 2000-01-01 | 22134.0        |

---

### Merge

Merges the outputs of the previous processors into a single, unified dataset.

---

### Cleansd

Clean the merged dataset by removing unnecessary observations, such as island provinces, rows with less than 5% missing values per feature, and trimming the data to the desired date range.

## Usage

### Run Everything

```bash
python3 main.py
```

This will:

- Check the folder structure
- Process all datasets individually
- Merge them into a unified dataset
- Clean the merged dataset (filter invalid data, handle missing values, trim by date)
- Export the final dataset

---

### Run Individual Steps

```python
from processors.AirQualityProcessor import AirQualityProcessor
AirQualityProcessor().process()

from processors.HealthProcessor import HealthProcessor
HealthProcessor().process()

from processors.SocioeconomicProcessor import SocioeconomicProcessor
SocioeconomicProcessor().process()

from processors.DataMerger import DataMerger
DataMerger().process()
```

---

## Main Orchestrator

The `main.py` script handles the full pipeline execution.

It will:  
- Verify folder structure  
- Run each processor step-by-step  
- Log progress and performance  
- Save final versioned dataset to `data/output/`

## Province Name Standardization

To ensure smooth merging, names like `"02 Albacete"` or `"Alicante/Alacant"` are standardized using a JSON mapping.

**Examples**:
- `"02 Albacete"` → `"Albacete"`  
- `"Alicante/Alacant"` → `"Alicante"`  
- `"A Coruna"` → `"A_Coruña"`

---

## Output

The final dataset is saved to:

- `data/output/dataset.csv` – latest result  

**Output Variables**:

- **Province**: Name of the province  
- **Year**: Measurement year or date  
- **Air_Pollutant**: Pollutant code (e.g. NO2)  
- **Air_Pollutant_Description**: Full name of pollutant  
- **Data_Aggregation_Process**: Aggregation method  
- **Air_Pollution_Level**: Measured value  
- **Unit**: Measurement unit (e.g., µg/m³)  
- **Air_Quality_Station_Type**: Station type (e.g., background)  
- **Air_Quality_Station_Area**: Area type (e.g., urban)  
- **Altitude**: Elevation of station (in meters)
- **Longitude**: Geographic coordinate specifying east-west position of the station
- **Latitude**: Geographic coordinate specifying north-south position of the station
- **Quality**: Air quality rating  
- **Respiratory_Diseases_Total**: Total respiratory-related deaths  
- **Life_Expectancy**: Average life expectancy  
- **GDP_per_capita**: GDP per capita

---
