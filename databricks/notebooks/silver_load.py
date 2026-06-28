# Databricks notebook source
# MAGIC %md
# MAGIC # silver_load
# MAGIC
# MAGIC Clean bronze tables into **`brazilian_ecommerce_silver`**.
# MAGIC
# MAGIC Run **`bronze_load`** first.
# MAGIC
# MAGIC Add one table per cell as we go (customers, geolocation, order_items, ...).

# COMMAND ----------

dbutils.widgets.text("catalog", "mypractice")
dbutils.widgets.text("bronze_schema", "brazilian_ecommerce_bronze")
dbutils.widgets.text("silver_schema", "brazilian_ecommerce_silver")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

print(f"Bronze : {BRONZE}")
print(f"Silver : {SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## olist_customers_dataset

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.olist_customers_dataset AS
SELECT
    customer_id,
    customer_unique_id,
    LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0') AS customer_zip_code_prefix,
    LOWER(TRIM(customer_city)) AS customer_city,
    LOWER(TRIM(customer_state)) AS customer_state
FROM {BRONZE}.olist_customers_dataset
WHERE customer_id RLIKE '^[a-f0-9]{{32}}$'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remaining tables (add next)
# MAGIC
# MAGIC - `olist_sellers_dataset`
# MAGIC - `olist_geolocation_dataset`
# MAGIC - `olist_order_items_dataset`
# MAGIC - `olist_order_payments_dataset`
# MAGIC - `olist_order_reviews_dataset`
# MAGIC - `olist_orders_dataset`
# MAGIC - `olist_products_dataset`
# MAGIC
# MAGIC `product_category_name_translation` — lookup only; join in products, no silver table needed.

# COMMAND ----------

for row in spark.sql(f"SHOW TABLES IN {SILVER}").collect():
    n = spark.table(f"{SILVER}.{row.tableName}").count()
    print(f"{row.tableName:45s} {n:>10,} rows")
