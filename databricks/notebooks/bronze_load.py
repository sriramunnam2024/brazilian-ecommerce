# Databricks notebook source
# MAGIC %md
# MAGIC # bronze_load
# MAGIC
# MAGIC **Step 0** — Verify bronze schema, volume, and CSV files exist. If not, copy from the seed volume in `default`.
# MAGIC
# MAGIC **Step 1** — Load CSVs into Delta tables in `brazilian_ecommerce_bronze` (explicit schemas, no `inferSchema`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Config
# MAGIC
# MAGIC | Setting | Default | Meaning |
# MAGIC |---------|---------|---------|
# MAGIC | `catalog` | mypractice | Unity Catalog name |
# MAGIC | `bronze_schema` | brazilian_ecommerce_bronze | Where bronze tables + volume live |
# MAGIC | `bronze_volume` | raw_data | Managed volume under bronze schema |
# MAGIC | `source_schema` | default | Where your uploaded CSVs are today |
# MAGIC | `source_volume` | brazilian_ecommerce_raw_data | Seed volume (from Catalog screenshot) |

# COMMAND ----------

dbutils.widgets.text("catalog", "mypractice")
dbutils.widgets.text("bronze_schema", "brazilian_ecommerce_bronze")
dbutils.widgets.text("bronze_volume", "raw_data")
dbutils.widgets.text("source_schema", "default")
dbutils.widgets.text("source_volume", "brazilian_ecommerce_raw_data")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")
SOURCE_SCHEMA = dbutils.widgets.get("source_schema")
SOURCE_VOLUME = dbutils.widgets.get("source_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SOURCE_PATH = f"/Volumes/{CATALOG}/{SOURCE_SCHEMA}/{SOURCE_VOLUME}"
BRONZE_VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}"
BRONZE_CSV_PATH = f"{BRONZE_VOLUME_PATH}/archive"

EXPECTED_CSVS = [
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Verify (and create / copy if missing)

# COMMAND ----------

def schema_exists(catalog: str, schema: str) -> bool:
    rows = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
    return any(r.databaseName == schema for r in rows)

def volume_exists(catalog: str, schema: str, volume: str) -> bool:
    rows = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}").collect()
    return any(r.volume_name == volume for r in rows)

def list_csv_names(path: str):
    try:
        return sorted(f.name for f in dbutils.fs.ls(path) if f.name.endswith(".csv"))
    except Exception:
        return []

def find_csv_folder(base_path: str) -> str:
    """CSVs may sit at volume root or in a subfolder (e.g. archive/)."""
    if list_csv_names(base_path):
        return base_path.rstrip("/")
    for entry in dbutils.fs.ls(base_path):
        if entry.isDir() and list_csv_names(entry.path):
            return entry.path.rstrip("/")
    return base_path.rstrip("/")

# --- 1) Bronze schema ---
if schema_exists(CATALOG, BRONZE_SCHEMA):
    print(f"OK  schema exists : {BRONZE}")
else:
    print(f"MISSING schema   : {BRONZE}  -> creating")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")

# --- 2) Bronze volume ---
if volume_exists(CATALOG, BRONZE_SCHEMA, BRONZE_VOLUME):
    print(f"OK  volume exists : {BRONZE_VOLUME_PATH}")
else:
    print(f"MISSING volume   : {BRONZE_VOLUME_PATH}  -> creating")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE}.{BRONZE_VOLUME}")

# --- 3) CSV files in bronze volume ---
bronze_csvs = list_csv_names(BRONZE_CSV_PATH)
missing = sorted(set(EXPECTED_CSVS) - set(bronze_csvs))

if not missing:
    print(f"OK  all {len(EXPECTED_CSVS)} CSVs in {BRONZE_CSV_PATH}")
    for name in bronze_csvs:
        print(f"    {name}")
else:
    print(f"MISSING {len(missing)} CSV(s) in bronze volume:")
    for name in missing:
        print(f"    {name}")

    source_csv_dir = find_csv_folder(SOURCE_PATH)
    source_csvs = list_csv_names(source_csv_dir)
    print(f"\nCopying from seed volume: {source_csv_dir}")

    if not source_csvs:
        raise FileNotFoundError(
            f"No CSV files found under {SOURCE_PATH}. Upload data to the seed volume first."
        )

    dbutils.fs.mkdirs(BRONZE_CSV_PATH)

    for filename in EXPECTED_CSVS:
        src = f"{source_csv_dir}/{filename}"
        dst = f"{BRONZE_CSV_PATH}/{filename}"
        if filename not in source_csvs:
            print(f"WARN  not in seed volume, skipped: {filename}")
            continue
        dbutils.fs.cp(src, dst, True)
        print(f"COPY  {filename}")

    bronze_csvs = list_csv_names(BRONZE_CSV_PATH)
    still_missing = sorted(set(EXPECTED_CSVS) - set(bronze_csvs))
    if still_missing:
        raise FileNotFoundError(f"Still missing after copy: {still_missing}")
    print(f"\nOK  all CSVs now in {BRONZE_CSV_PATH}")

# Path used by the load step below
BASE_PATH = BRONZE_CSV_PATH
print(f"\nReady to load from: {BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load bronze Delta tables

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
    df.write.mode("overwrite").saveAsTable(f"{BRONZE}.{table_name}")
    print(f"{table_name:45s} {df.count():>10,} rows")

# COMMAND ----------

display(spark.table(f"{BRONZE}.olist_customers_dataset").select("customer_zip_code_prefix").limit(5))
display(spark.table(f"{BRONZE}.olist_orders_dataset").select("order_purchase_timestamp").limit(5))
