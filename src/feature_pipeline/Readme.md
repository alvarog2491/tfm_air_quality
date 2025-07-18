# Multi-Source Data Pipeline

This module performs ETL operations to unify air **quality**, **health**, and **socioeconomic** datasets from official Spanish sources into a single dataset ready for analysis or machine learning.

---

## Table of Contents

- [Multi-Source Data Pipeline](#multi-source-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [The pipeline processes three data types through dedicated processors, merges them into a unified dataset, and applies feature engineering and validation steps.](#the-pipeline-processes-three-data-types-through-dedicated-processors-merges-them-into-a-unified-dataset-and-applies-feature-engineering-and-validation-steps)
  - [Air Quality](#air-quality)
  - [Health](#health)
  - [Socioeconomic](#socioeconomic)
- [Processing Pipeline](#processing-pipeline)
  - [1. Source procesors](#1-source-procesors)
    - [Air Quality Processor](#air-quality-processor)
    - [Health Processor](#health-processor)
    - [Socioeconomic Processor](#socioeconomic-processor)
  - [Pipeline steps](#pipeline-steps)
    - [Merge](#merge)
      - [Feature engineering](#feature-engineering)
      - [Clean](#clean)
      - [Validate](#validate)
  - [Province Name Standardization](#province-name-standardization)
  - [Usage](#usage)
    - [Run Everything](#run-everything)
  - [Main Orchestrator](#main-orchestrator)
  - [Output](#output)

---

## Overview

The pipeline processes three data types through dedicated processors, merges them into a unified dataset, and applies feature engineering and validation steps.
---

## Air Quality

- [EEA (European Environment Agency)](https://discomap.eea.europa.eu/App/AQViewer/index.html?fqn=Airquality_Dissem.b2g.AirQualityStatistics&Country=Spain&inAQReportYN=Yes):  Data for PM2.5, PM10, NO2, SO2, O3  pollutants accross Spanish provinces.
- [BOE](https://www.boe.es/buscar/doc.php?id=BOE-A-2020-10426): Classification of air quality into 6 categories (from "buena" to "extremadamente desfavorable")

## Health

- [INE – Mortality Data](https://www.ine.es/jaxiT3/Tabla.htm?t=9935&L=0): Deaths due to respiratory system diseases (codes 062–067)  
- [INE – Life Expectancy](https://www.ine.es/jaxiT3/Tabla.htm?t=1485): Life expectancy by province and gender

## Socioeconomic

- [GDP](https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736167628&menu=resultados&idp=1254735576581#_tabs-1254736158133) per capita by province from 2000 to 2022
- [Province population](https://www.ine.es/jaxiT3/Tabla.htm?t=2852) size of each province in a specific year
---

# Processing Pipeline

## 1. Source procesors
Each processor handles raw CSV files for a specific data type:

### Air Quality Processor

Reads pollutant data and adds a classification column based on thresholds, collected from BOE y se estandarizan los nombres de las provincias.

**Input**: `"air_quality_with_province.csv"`  
**Output**: Same + air quality classification

| Air Pollutant | Air Pollutant Description | Data Aggregation Process      | Year       | Air Pollution Level | Unit Of Air Pollution Level |
| ------------- | ------------------------- | ----------------------------- | ---------- | ------------------- | --------------------------- |
| no2           | Nitrogen dioxide (air)    | Annual mean / 1 calendar year | 1991-01-01 | 80.639              | ug/m3                       |


| Air Quality Station Type | Air Quality Station Area | Longitude | Latitude | Altitude | Province | Quality              |
| ------------------------ | ------------------------ | --------- | -------- | -------- | -------- | -------------------- |
| Background               | urban                    | -3.705    | 40.347   | 593.000  | Madrid   | RAZONABLEMENTE BUENA |


---

### Health Processor

Combines mortality and life expectancy data into a unified format y se estandarizan los nombres de las provincias.

**Input**:  
- `"enfermedades_respiratorias.csv"`  
- `"esperanza_vida.csv"`  

**Output**:

| Province | Year       | Respiratory Diseases | Life Expectancy |
|----------|------------|----------------------|-----------------|
| Albacete | 2023-01-01 | 397                  | 83.61           |

---

### Socioeconomic Processor

Converts wide-format GDP data into long format y se estandarizan los nombres de las provincias.

**Input**: `"PIB per cap provincias 2000-2021.csv"`  
**Output**:

| Province | Year       | GDP per Capita |
|----------|------------|----------------|
| Alava    | 2000-01-01 | 22134.0        |

Population Size, se descartan las columnas innecesarias y se estandarizan los nombres de las provincias.

**Input**: `"poblacion_provincias.csv"`
**Output**:

| Province | Year        | Population   |
|----------|-------------|--------------|
| Albacete | 2021-01-01  | 386464.      |

---

## Pipeline steps

### Merge

Combines all processed datasets using standardized province names and year as primary keys.

#### Feature engineering

Creates new variables:
* **respiratory_deaths_per_100k** Respiratory deaths per 100,000 inhabitants

#### Clean

Removes unnecessary data:

* island provinces
* rows with less than 5% missing values per feature (>5% Warning is thrown)
* Data outside 2000-2021 range

#### Validate
Ensures data integrity:

* No null values in final dataset
* Correct data types for all features
  
## Province Name Standardization

To ensure smooth merging, names like `"02 Albacete"` or `"Alicante/Alacant"` are standardized using a JSON mapping.

**Examples**:
- `"02 Albacete"` → `"Albacete"`  
- `"Alicante/Alacant"` → `"Alicante"`  
- `"A Coruna"` → `"A_Coruña"`

---

## Usage

### Run Everything

```bash
python3 main.py
```

---

## Main Orchestrator

The `main.py` script orchestrates the full pipeline execution.

It will:  
- Verify folder structure  
- Run each processor step-by-step  
- Run pipeline steps
- Log progress and performance  
- Save final versioned dataset to `data/output/`

## Output

**Location**: data/output/dataset.csv

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
- **Population** Size population of province 
- **respiratory_deaths_per_100k** Respiratory deaths per 100,000 inhabitants
---
