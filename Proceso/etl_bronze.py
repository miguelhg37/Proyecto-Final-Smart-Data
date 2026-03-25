# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Medallion - Notebook Bronze
# MAGIC
# MAGIC ## Objetivo
# MAGIC Realizar la lectura y validacion inicial de archivos CSV desde la capa Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importacion de librerias

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parametrizacion con widgets - Capa Bronze

# COMMAND ----------

dbutils.widgets.text("bronze_path", "abfss://bronze@adlssmartdata2303.dfs.core.windows.net/", "Ruta Bronze")
dbutils.widgets.text("customers_file", "bronze_customers.csv", "Archivo Customers")
dbutils.widgets.text("orders_file", "bronze_orders.csv", "Archivo Orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de widgets - Capa Bronze

# COMMAND ----------

bronze_path = dbutils.widgets.get("bronze_path")
customers_file = dbutils.widgets.get("customers_file")
orders_file = dbutils.widgets.get("orders_file")

customers_path = bronze_path + customers_file
orders_path = bronze_path + orders_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lectura de datos - Capa Bronze

# COMMAND ----------

df_customers_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(customers_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
)

df_orders_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(orders_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validacion de lectura inicial - Capa Bronze

# COMMAND ----------

display(df_customers_bronze)
display(df_orders_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Metricas operativas - Capa Bronze

# COMMAND ----------

df_bronze_metrics = spark.createDataFrame(
    [
        ("customers_bronze", df_customers_bronze.count()),
        ("orders_bronze", df_orders_bronze.count())
    ],
    ["metric_name", "metric_value"]
).withColumn("etl_timestamp", F.current_timestamp())

display(df_bronze_metrics)
