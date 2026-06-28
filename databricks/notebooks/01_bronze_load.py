# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Bronze load (Olist CSVs)
# MAGIC
# MAGIC Loads all 9 CSVs from the Unity Catalog volume into Delta bronze tables.
# MAGIC
# MAGIC **CSV fixes applied:**
# MAGIC - Explicit schemas (no `inferSchema`) — keeps zip codes as strings, timestamps as timestamps
# MAGIC - UTF-8 encoding
# MAGIC - Renames product column typos (`product_name_lenght` → `product_name_length`)
# MAGIC
# MAGIC **Next steps** (add later): silver transforms, gold models, EDA.

# COMMAND ----------

dbutils.widgets.text("catalog", "mypractice")
dbutils.widgets.text("schema", "brazilian_ecommerce")
dbutils.widgets.text("subfolder", "raw_data/archive")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SUBFOLDER = dbutils.widgets.get("subfolder")

BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{SUBFOLDER}"
TARGET = f"{CATALOG}.{SCHEMA}"

print(f"Reading : {BASE_PATH}")
print(f"Writing : {TARGET}")

# COMMAND ----------

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TABLE_SPECS = {
    "olist_customers_dataset": (
        "olist_customers_dataset.csv",
        StructType([
            StructField("customer_id", StringType()),
            StructField("customer_unique_id", StringType()),
            StructField("customer_zip_code_prefix", StringType()),
            StructField("customer_city", StringType()),
            StructField("customer_state", StringType()),
        ]),
    ),
    "olist_sellers_dataset": (
        "olist_sellers_dataset.csv",
        StructType([
            StructField("seller_id", StringType()),
            StructField("seller_zip_code_prefix", StringType()),
            StructField("seller_city", StringType()),
            StructField("seller_state", StringType()),
        ]),
    ),
    "olist_orders_dataset": (
        "olist_orders_dataset.csv",
        StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_status", StringType()),
            StructField("order_purchase_timestamp", TimestampType()),
            StructField("order_approved_at", TimestampType()),
            StructField("order_delivered_carrier_date", TimestampType()),
            StructField("order_delivered_customer_date", TimestampType()),
            StructField("order_estimated_delivery_date", TimestampType()),
        ]),
    ),
    "olist_order_items_dataset": (
        "olist_order_items_dataset.csv",
        StructType([
            StructField("order_id", StringType()),
            StructField("order_item_id", IntegerType()),
            StructField("product_id", StringType()),
            StructField("seller_id", StringType()),
            StructField("shipping_limit_date", TimestampType()),
            StructField("price", DoubleType()),
            StructField("freight_value", DoubleType()),
        ]),
    ),
    "olist_order_payments_dataset": (
        "olist_order_payments_dataset.csv",
        StructType([
            StructField("order_id", StringType()),
            StructField("payment_sequential", IntegerType()),
            StructField("payment_type", StringType()),
            StructField("payment_installments", IntegerType()),
            StructField("payment_value", DoubleType()),
        ]),
    ),
    "olist_order_reviews_dataset": (
        "olist_order_reviews_dataset.csv",
        StructType([
            StructField("review_id", StringType()),
            StructField("order_id", StringType()),
            StructField("review_score", IntegerType()),
            StructField("review_comment_title", StringType()),
            StructField("review_comment_message", StringType()),
            StructField("review_creation_date", TimestampType()),
            StructField("review_answer_timestamp", TimestampType()),
        ]),
    ),
    "olist_products_dataset": (
        "olist_products_dataset.csv",
        StructType([
            StructField("product_id", StringType()),
            StructField("product_category_name", StringType()),
            StructField("product_name_lenght", IntegerType()),
            StructField("product_description_lenght", IntegerType()),
            StructField("product_photos_qty", IntegerType()),
            StructField("product_weight_g", DoubleType()),
            StructField("product_length_cm", DoubleType()),
            StructField("product_height_cm", DoubleType()),
            StructField("product_width_cm", DoubleType()),
        ]),
    ),
    "olist_geolocation_dataset": (
        "olist_geolocation_dataset.csv",
        StructType([
            StructField("geolocation_zip_code_prefix", StringType()),
            StructField("geolocation_lat", DoubleType()),
            StructField("geolocation_lng", DoubleType()),
            StructField("geolocation_city", StringType()),
            StructField("geolocation_state", StringType()),
        ]),
    ),
    "product_category_name_translation": (
        "product_category_name_translation.csv",
        StructType([
            StructField("product_category_name", StringType()),
            StructField("product_category_name_english", StringType()),
        ]),
    ),
}

PRODUCT_RENAMES = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
}

# COMMAND ----------

tables = {}
for table_name, (filename, schema) in TABLE_SPECS.items():
    df = (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(f"{BASE_PATH}/{filename}")
    )
    if table_name == "olist_products_dataset":
        for old, new in PRODUCT_RENAMES.items():
            df = df.withColumnRenamed(old, new)
    tables[table_name] = df
    df.write.mode("overwrite").saveAsTable(f"{TARGET}.{table_name}")
    print(f"{table_name:45s} {df.count():>10,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot check
# MAGIC
# MAGIC Zip codes should be strings (e.g. `01037`, not `1037`). Timestamps should be `timestamp`, not `string`.

# COMMAND ----------

display(spark.table(f"{TARGET}.olist_customers_dataset").select("customer_zip_code_prefix").limit(5))
display(spark.table(f"{TARGET}.olist_orders_dataset").select("order_purchase_timestamp").limit(5))
