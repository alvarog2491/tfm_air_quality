# 🌍 Spanish Multi-Source Data Pipeline

A complete data processing pipeline that brings together **air quality**, **health**, and **socioeconomic** data from Spanish provinces — all ready for analysis or machine learning tasks.

---

## 📚 Table of Contents

- 📄 [Description](#description)
- 📊 [Data Sources](#data-sources)
- ⚙️ [Data Processing Pipeline](#data-processing-pipeline)
- 🚀 [Usage](#usage)
- 🧠 [Main Orchestrator](#main-orchestrator)
- 🧼 [Province Name Standardization](#province-name-standardization)
- 📁 [Output](#output)

---

## 📄 Description

This pipeline builds a unified dataset combining three key data types:

- 🏭 **Air Quality**: Pollutant levels recorded by sensors across Spain.  
- 🏥 **Health**: Life expectancy and deaths related to respiratory diseases by province.  
- 💰 **Socioeconomic**: GDP per capita by province, from 2000 to 2022.  

All records are standardized to share the same structure, using `Province` and `Year` as primary keys.

---

## 📊 Data Sources

### Air Quality

- [EEA (European Environment Agency)](https://discomap.eea.europa.eu/App/AQViewer/index.html?fqn=Airquality_Dissem.b2g.AirQualityStatistics&Country=Spain&inAQReportYN=Yes):  
  Data for PM2.5, PM10, NO2, SO2, O3  
- BOE: Classification of air quality into 6 categories (from "buena" to "extremadamente desfavorable")

### Health

- [INE – Mortality Data](https://www.ine.es/jaxiT3/Tabla.htm?t=9935&L=0):  
  Deaths due to respiratory system diseases (codes 062–067)  
- [INE – Life Expectancy](https://www.ine.es/jaxiT3/Tabla.htm?t=1485):  
  Life expectancy by province and gender

### Socioeconomic

- GDP per capita by province from 2000 to 2022

---

## ⚙️ Data Processing Pipeline

The pipeline runs in four phases:

### 1️⃣ Air Quality

Reads pollutant data and adds a classification column based on thresholds.

**Input**: `"air_quality_with_province.csv"`  
**Output**: Same + air quality classification

| Air Pollutant | Air Pollutant Description | Data Aggregation Process      | Year       | Air Pollution Level | Unit Of Air Pollution Level |
| ------------- | ------------------------- | ----------------------------- | ---------- | ------------------- | --------------------------- |
| no2           | Nitrogen dioxide (air)    | Annual mean / 1 calendar year | 1991-01-01 | 80.639              | ug/m3                       |


| Air Quality Station Type | Air Quality Station Area | Longitude | Latitude | Altitude | Province | Quality              |
| ------------------------ | ------------------------ | --------- | -------- | -------- | -------- | -------------------- |
| Background               | urban                    | -3.705    | 40.347   | 593.000  | Madrid   | RAZONABLEMENTE BUENA |


---

### 2️⃣ Health

Combines mortality and life expectancy data into a unified format.

**Input**:  
- `"enfermedades_respiratorias.csv"`  
- `"esperanza_vida.csv"`  

**Output**:

| Province | Year       | Respiratory Diseases | Life Expectancy |
|----------|------------|----------------------|-----------------|
| Albacete | 2023-01-01 | 397                  | 83.61           |

---

### 3️⃣ Socioeconomic

Converts wide-format GDP data into long format.

**Input**: `"PIB per cap provincias 2000-2021.csv"`  
**Output**:

| Province | Year       | GDP per Capita |
|----------|------------|----------------|
| Alava    | 2000-01-01 | 22134.0        |

---

### 4️⃣ Merge

Merges the outputs of the previous processors into a single, unified dataset.

---

## 🚀 Usage

### Run Everything

```bash
python main.py
```

This will:  
- Check folder structure  
- Process all datasets  
- Merge and export final dataset

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

## 🧠 Main Orchestrator

The `main.py` script handles the full pipeline execution.

It will:  
- Verify folder structure  
- Run each processor step-by-step  
- Log progress and performance  
- Save final versioned dataset to `data/output/`

Console logs include:  
- Step start/end  
- Row/column count  
- Time taken  
- Any exceptions with full tracebacks

---

## 🧼 Province Name Standardization

To ensure smooth merging, names like `"02 Albacete"` or `"Alicante/Alacant"` are standardized using a JSON mapping.

**Examples**:
- `"02 Albacete"` → `"Albacete"`  
- `"Alicante/Alacant"` → `"Alicante"`  
- `"A Coruna"` → `"A_Coruña"`

---

## 📁 Output

The final dataset is saved to:

- `data/output/dataset.csv` – latest result  
- `data/output/dataset_YYYYMMDD_HHMMSS.csv` – versioned snapshot  

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
- **Quality**: Air quality rating  
- **Respiratory_Diseases_Total**: Total respiratory-related deaths  
- **Life_Expectancy**: Average life expectancy  
- **GDP_per_capita**: GDP per capita

---
