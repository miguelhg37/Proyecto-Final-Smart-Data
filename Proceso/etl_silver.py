# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Medallion - Notebook Silver
# MAGIC
# MAGIC ## Objetivo
# MAGIC Aplicar estandarizacion, tipificacion y reglas de calidad para generar datasets validos e invalidos en la capa Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importacion de librerias

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parametrizacion con widgets - Capa Silver

# COMMAND ----------

dbutils.widgets.text("bronze_path", "abfss://bronze@adlssmartdata2303.dfs.core.windows.net/", "Ruta Bronze")
dbutils.widgets.text("silver_path", "abfss://silver@adlssmartdata2303.dfs.core.windows.net/", "Ruta Silver")
dbutils.widgets.text("customers_file", "bronze_customers.csv", "Archivo Customers")
dbutils.widgets.text("orders_file", "bronze_orders.csv", "Archivo Orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de widgets - Capa Silver

# COMMAND ----------

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
customers_file = dbutils.widgets.get("customers_file")
orders_file = dbutils.widgets.get("orders_file")

customers_path = bronze_path + customers_file
orders_path = bronze_path + orders_file

silver_customers_valid_path   = silver_path + "customers_valid"
silver_customers_invalid_path = silver_path + "customers_invalid"
silver_orders_valid_path      = silver_path + "orders_valid"
silver_orders_invalid_path    = silver_path + "orders_invalid"

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
# MAGIC ## 5. Estandarizacion y tipificacion - Capa Silver (Customers)

# COMMAND ----------

df_customers_silver_base = (
    df_customers_bronze
    .select(
        F.col("customer_id").cast("int").alias("customer_id"),
        F.initcap(F.trim(F.col("first_name"))).alias("first_name"),
        F.initcap(F.trim(F.col("last_name"))).alias("last_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.initcap(F.trim(F.col("country"))).alias("country"),
        F.to_date(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
        F.initcap(F.trim(F.col("customer_segment"))).alias("customer_segment"),
        F.lower(F.trim(F.col("status"))).alias("status"),
        F.col("ingestion_timestamp"),
        F.col("source_file")
    )
    .dropDuplicates(["customer_id"])
    .withColumn("etl_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reglas de calidad de datos - Capa Silver (Customers)

# COMMAND ----------

customer_invalid_condition = (
    F.col("customer_id").isNull() |
    F.col("email").isNull() |
    (F.trim(F.col("email")) == "") |
    F.col("signup_date").isNull() |
    (~F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")) |
    (~F.col("status").isin("active", "inactive"))
)

df_customers_invalid = (
    df_customers_silver_base
    .withColumn(
        "dq_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("customer_id").isNull(), F.lit("customer_id nulo")),
            F.when(F.col("email").isNull() | (F.trim(F.col("email")) == ""), F.lit("email nulo o vacio")),
            F.when(F.col("signup_date").isNull(), F.lit("signup_date invalido")),
            F.when(~F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), F.lit("email con formato invalido")),
            F.when(~F.col("status").isin("active", "inactive"), F.lit("status fuera de catalogo"))
        )
    )
    .filter(customer_invalid_condition)
)

df_customers_valid = (
    df_customers_silver_base
    .filter(~customer_invalid_condition)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Estandarizacion y tipificacion - Capa Silver (Orders)

# COMMAND ----------

df_orders_silver_base = (
    df_orders_bronze
    .select(
        F.col("order_id").cast("int").alias("order_id"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.to_date(F.col("order_date"), "yyyy-MM-dd").alias("order_date"),
        F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.initcap(F.trim(F.col("payment_method"))).alias("payment_method"),
        F.initcap(F.trim(F.col("order_status"))).alias("order_status"),
        F.col("ingestion_timestamp"),
        F.col("source_file")
    )
    .dropDuplicates(["order_id"])
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("etl_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Reglas de calidad de datos - Capa Silver (Orders)

# COMMAND ----------

valid_customer_ids = df_customers_valid.select("customer_id").dropDuplicates()

df_orders_with_customer_check = (
    df_orders_silver_base.alias("o")
    .join(valid_customer_ids.alias("c"), on="customer_id", how="left")
    .withColumn("customer_exists", F.when(F.col("c.customer_id").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
    .drop(F.col("c.customer_id"))
)

order_invalid_condition = (
    F.col("order_id").isNull() |
    F.col("customer_id").isNull() |
    F.col("order_date").isNull() |
    F.col("product_name").isNull() |
    (F.trim(F.col("product_name")) == "") |
    F.col("quantity").isNull() |
    F.col("unit_price").isNull() |
    (F.col("quantity") <= 0) |
    (F.col("unit_price") <= 0) |
    F.col("total_amount").isNull() |
    (~F.col("payment_method").isin("Card", "Transfer", "Cash")) |
    (~F.col("order_status").isin("Delivered", "Pending", "Cancelled", "Returned")) |
    (~F.col("customer_exists"))
)

df_orders_invalid = (
    df_orders_with_customer_check
    .withColumn(
        "dq_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("order_id").isNull(), F.lit("order_id nulo")),
            F.when(F.col("customer_id").isNull(), F.lit("customer_id nulo")),
            F.when(F.col("order_date").isNull(), F.lit("order_date invalido")),
            F.when(F.col("product_name").isNull() | (F.trim(F.col("product_name")) == ""), F.lit("product_name nulo o vacio")),
            F.when(F.col("quantity").isNull(), F.lit("quantity nulo")),
            F.when(F.col("unit_price").isNull(), F.lit("unit_price nulo")),
            F.when(F.col("quantity") <= 0, F.lit("quantity menor o igual a cero")),
            F.when(F.col("unit_price") <= 0, F.lit("unit_price menor o igual a cero")),
            F.when(F.col("total_amount").isNull(), F.lit("total_amount nulo")),
            F.when(~F.col("payment_method").isin("Card", "Transfer", "Cash"), F.lit("payment_method fuera de catalogo")),
            F.when(~F.col("order_status").isin("Delivered", "Pending", "Cancelled", "Returned"), F.lit("order_status fuera de catalogo")),
            F.when(~F.col("customer_exists"), F.lit("customer_id no existe en customers validos"))
        )
    )
    .filter(order_invalid_condition)
)

df_orders_valid = (
    df_orders_with_customer_check
    .filter(~order_invalid_condition)
    .drop("customer_exists")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validacion de datos transformados - Capa Silver

# COMMAND ----------

display(df_customers_valid)
display(df_customers_invalid)
display(df_orders_valid)
display(df_orders_invalid)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Persistencia de datos - Capa Silver

# COMMAND ----------

df_customers_valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_customers_valid_path)
df_customers_invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_customers_invalid_path)

df_orders_valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_orders_valid_path)
df_orders_invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_orders_invalid_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validacion final de almacenamiento - Capa Silver

# COMMAND ----------

display(spark.read.format("delta").load(silver_customers_valid_path))
display(spark.read.format("delta").load(silver_customers_invalid_path))
display(spark.read.format("delta").load(silver_orders_valid_path))
display(spark.read.format("delta").load(silver_orders_invalid_path))
