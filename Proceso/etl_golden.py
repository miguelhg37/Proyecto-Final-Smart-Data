# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Medallion - Notebook Golden
# MAGIC
# MAGIC ## Objetivo
# MAGIC Generar datasets analiticos y metricas operativas en la capa Golden a partir de la capa Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importacion de librerias

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parametrizacion con widgets - Capa Golden

# COMMAND ----------

dbutils.widgets.text("silver_path", "abfss://silver@adlssmartdata2303.dfs.core.windows.net/", "Ruta Silver")
dbutils.widgets.text("gold_path", "abfss://golden@adlssmartdata2303.dfs.core.windows.net/", "Ruta Golden")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de widgets - Capa Golden

# COMMAND ----------

silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")

silver_customers_valid_path   = silver_path + "customers_valid"
silver_customers_invalid_path = silver_path + "customers_invalid"
silver_orders_valid_path      = silver_path + "orders_valid"
silver_orders_invalid_path    = silver_path + "orders_invalid"

gold_customer_sales_path      = gold_path + "customer_sales"
gold_product_sales_path       = gold_path + "product_sales"
gold_etl_metrics_path         = gold_path + "etl_metrics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lectura de datos - Capa Silver

# COMMAND ----------

df_customers_silver_saved = spark.read.format("delta").load(silver_customers_valid_path)
df_customers_invalid_saved = spark.read.format("delta").load(silver_customers_invalid_path)
df_orders_silver_saved = spark.read.format("delta").load(silver_orders_valid_path)
df_orders_invalid_saved = spark.read.format("delta").load(silver_orders_invalid_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validacion de lectura inicial - Capa Silver

# COMMAND ----------

display(df_customers_silver_saved)
display(df_orders_silver_saved)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Transformacion analitica - Capa Golden (Customer Sales)

# COMMAND ----------

df_gold_customer_sales = (
    df_orders_silver_saved.alias("o")
    .join(df_customers_silver_saved.alias("c"), on="customer_id", how="inner")
    .groupBy(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "country",
        "customer_segment",
        "status"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_items"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date")
    )
    .withColumn("etl_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Transformacion analitica - Capa Golden (Product Sales)

# COMMAND ----------

df_gold_product_sales = (
    df_orders_silver_saved
    .groupBy("product_name", "order_status", "payment_method")
    .agg(
        F.countDistinct("order_id").alias("num_orders"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("total_amount"), 2).alias("sales_amount"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price")
    )
    .withColumn("etl_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Metricas operativas del ETL - Capa Golden

# COMMAND ----------

df_etl_metrics = spark.createDataFrame(
    [
        ("customers_valid_silver", df_customers_silver_saved.count()),
        ("customers_invalid_silver", df_customers_invalid_saved.count()),
        ("orders_valid_silver", df_orders_silver_saved.count()),
        ("orders_invalid_silver", df_orders_invalid_saved.count()),
        ("gold_customer_sales", df_gold_customer_sales.count()),
        ("gold_product_sales", df_gold_product_sales.count()),
    ],
    ["metric_name", "metric_value"]
).withColumn("etl_timestamp", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validacion de resultados - Capa Golden

# COMMAND ----------

display(df_gold_customer_sales)
display(df_gold_product_sales)
display(df_etl_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Persistencia de datos - Capa Golden

# COMMAND ----------

df_gold_customer_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_customer_sales_path)
df_gold_product_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_product_sales_path)
df_etl_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_etl_metrics_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validacion final de almacenamiento - Capa Golden

# COMMAND ----------

display(spark.read.format("delta").load(gold_customer_sales_path))
display(spark.read.format("delta").load(gold_product_sales_path))
display(spark.read.format("delta").load(gold_etl_metrics_path))
