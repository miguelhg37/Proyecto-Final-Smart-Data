# 📊 ETL Medallion en Azure Databricks

![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-ETL-orange?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Engineering-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-0A84FF)
![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-Storage-0078D4?logo=microsoftazure&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-6E56CF)
![Status](https://img.shields.io/badge/Status-Production%20Ready-success)

## 🧠 Descripción del proyecto

Este proyecto implementa un pipeline de datos **end-to-end** en **Azure Databricks** siguiendo la arquitectura **Medallion** para procesar datos desde archivos CSV en la capa **Bronze**, aplicar limpieza y validación en **Silver**, y generar datasets analíticos en **Golden**.

El flujo fue diseñado con enfoque de ingeniería de datos moderna, incorporando:

- separación por capas
- reglas de calidad de datos
- persistencia en **Delta Lake**
- parametrización mediante widgets
- orquestación con **Databricks Jobs** multitarea

---

## ✨ Características principales

- Ingesta de archivos CSV desde **Azure Data Lake Storage Gen2**
- Arquitectura **Bronze → Silver → Golden**
- Validación de calidad de datos
- Separación entre registros **válidos** e **inválidos**
- Persistencia en **Delta**
- Job multitask con subtareas por capa
- Estructura reutilizable y lista para evolucionar a cargas incrementales

---

## 🏗️ Arquitectura funcional

```mermaid
flowchart LR
    A[CSV en ADLS Gen2<br/>Container Bronze] --> B[Notebook Bronze<br/>Lectura e ingesta]
    B --> C[Notebook Silver<br/>Tipificación y calidad]
    C --> D1[Silver Valid]
    C --> D2[Silver Invalid]
    D1 --> E[Notebook Golden<br/>Agregaciones analíticas]
    D2 --> F[Monitoreo de calidad]
    E --> G1[customer_sales]
    E --> G2[product_sales]
    E --> G3[etl_metrics]
```

---

## 🧱 Arquitectura Medallion

### 🔹 Bronze
Contiene los datos crudos en formato CSV.

**Archivos de entrada**
- `bronze_customers.csv`
- `bronze_orders.csv`

**Columnas técnicas**
- `ingestion_timestamp`
- `source_file`

---

### 🔹 Silver
Aplica reglas de transformación y calidad.

**Procesos**
- limpieza y normalización
- tipificación
- validaciones
- separación valid/invalid

**Salidas**
- `customers_valid`
- `customers_invalid`
- `orders_valid`
- `orders_invalid`

---

### 🔹 Golden
Genera datasets analíticos listos para consumo.

**Salidas**
- `customer_sales`
- `product_sales`
- `etl_metrics`

---

## 🗂️ Estructura del proyecto

```text
repo/
├── README.md
├── etl_bronze.py
├── etl_silver.py
├── etl_golden.py
├── databricks_job_multitask.json
└── databricks_bundle_multitask.yml
```

---

## ⚙️ Orquestación del pipeline

```mermaid
flowchart LR
    T1[bronze_ingestion] --> T2[silver_transformation]
    T2 --> T3[golden_aggregation]
```

---
## ⚙️ Workflow en Databricks

![](https://github.com/miguelhg37/Proyecto-Final-Smart-Data/blob/main/Evidencias/Ejecucion%20Workflow%20ETL%20Medallion%20Job.png)

## 🧪 Reglas de calidad de datos

### Customers
- email válido
- status válido
- campos obligatorios

### Orders
- valores positivos
- customer_id válido
- catálogos controlados

---

## 🛠️ Tecnologías utilizadas

- Azure Databricks
- PySpark
- Delta Lake
- ADLS Gen2

---

## 🔧 Parámetros

- bronze_path
- silver_path
- gold_path

---

## 🚀 Ejecución

1. Cargar archivos en Bronze
2. Ejecutar notebooks o job multitask
3. Validar resultados en Golden

---

## ✅ Buenas prácticas

- Arquitectura Medallion
- Data Quality
- Delta Lake
- Orquestación
- Parametrización

---

## 📈 Posibles mejoras

- Incremental (MERGE)
- Particionado
- Unity Catalog
- Monitoreo

---

## 💼 Valor del proyecto

Pipeline productivo alineado a estándares modernos de ingeniería de datos.

---

## 👤 Autor

Miguel Hernandez Guerrero 

[GitHub](https://github.com/miguelhg37) [LinkedIn](https://www.linkedin.com/in/miguelhg1/)

Data Engineering | Databricks | Microsof Azure | Lakehouse