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

from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/olist_customers_dataset.csv"

customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_unique_id", StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType()),
])

customers_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")   # columns that don't fit schema land here
    .schema(customers_schema)
    .csv(CSV_FILE)
)

display(customers_raw.limit(5))

# COMMAND ----------

total = customers_raw.count()

rescued = customers_raw.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = customers_raw.select(
    F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer_id"),
    F.sum(F.col("customer_unique_id").isNull().cast("int")).alias("null_customer_unique_id"),
    F.sum(F.col("customer_zip_code_prefix").isNull().cast("int")).alias("null_zip"),
    F.sum(F.col("customer_city").isNull().cast("int")).alias("null_city"),
    F.sum(F.col("customer_state").isNull().cast("int")).alias("null_state"),
)

dupes = (
    customers_raw
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

bad_zip = customers_raw.filter(
    ~F.col("customer_zip_code_prefix").rlike(r"^\d{5}$")
).count()

bad_state = customers_raw.filter(
    ~F.col("customer_state").rlike(r"^[A-Z]{2}$")
).count()

print(f"Total rows        : {total:,}")
print(f"Rescued rows      : {rescued:,}")
print(f"Duplicate IDs     : {dupes:,}")
print(f"Bad zip format    : {bad_zip:,}")
print(f"Bad state format  : {bad_state:,}")

display(null_checks)

# COMMAND ----------

# File metadata from the volume (run once per CSV path)
file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

customers_bronze = (
    customers_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

from pyspark.sql import functions as F

customers_good = customers_bronze.filter(
    F.col("_rescued_data").isNull()
    & F.col("customer_id").isNotNull()
    & F.col("customer_id").rlike(r"^[a-f0-9]{32}$")
)

customers_quarantine = customers_bronze.filter(
    F.col("_rescued_data").isNotNull()
    | F.col("customer_id").isNull()
    | ~F.col("customer_id").rlike(r"^[a-f0-9]{32}$")
)

good_count = customers_good.count()
bad_count = customers_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"

customers_good.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.olist_customers_dataset"
)

if bad_count > 0:
    customers_quarantine.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.olist_customers_dataset_quarantine"
    )
    print(f"Saved quarantine: {BRONZE}.olist_customers_dataset_quarantine")
else:
    print("No quarantine rows — skipped quarantine table.")

print(f"Saved bronze: {BRONZE}.olist_customers_dataset")
print(f"Rows: {good_count:,}")

# COMMAND ----------

display(spark.table(f"{BRONZE}.olist_customers_dataset").limit(50))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE mypractice.brazilian_ecommerce_bronze.olist_customers_dataset;

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/olist_sellers_dataset.csv"

sellers_schema = StructType([
    StructField("seller_id", StringType()),
    StructField("seller_zip_code_prefix", StringType()),
    StructField("seller_city", StringType()),
    StructField("seller_state", StringType()),
])

sellers_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(sellers_schema)
    .csv(CSV_FILE)
)

display(sellers_raw.limit(5))

# COMMAND ----------

total = sellers_raw.count()
rescued = sellers_raw.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = sellers_raw.select(
    F.sum(F.col("seller_id").isNull().cast("int")).alias("null_seller_id"),
    F.sum(F.col("seller_zip_code_prefix").isNull().cast("int")).alias("null_zip"),
    F.sum(F.col("seller_city").isNull().cast("int")).alias("null_city"),
    F.sum(F.col("seller_state").isNull().cast("int")).alias("null_state"),
)

dupes = (
    sellers_raw.groupBy("seller_id").count()
    .filter(F.col("count") > 1).count()
)

bad_zip = sellers_raw.filter(
    ~F.col("seller_zip_code_prefix").rlike(r"^\d{5}$")
).count()

bad_state = sellers_raw.filter(
    ~F.col("seller_state").rlike(r"^[A-Z]{2}$")
).count()

print(f"Total rows        : {total:,}")
print(f"Rescued rows      : {rescued:,}")
print(f"Duplicate IDs     : {dupes:,}")
print(f"Bad zip format    : {bad_zip:,}")
print(f"Bad state format  : {bad_state:,}")

display(null_checks)

# COMMAND ----------

file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

sellers_bronze = (
    sellers_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

sellers_good = sellers_bronze.filter(
    F.col("_rescued_data").isNull()
    & F.col("seller_id").isNotNull()
    & F.col("seller_id").rlike(r"^[a-f0-9]{32}$")
)

sellers_quarantine = sellers_bronze.filter(
    F.col("_rescued_data").isNotNull()
    | F.col("seller_id").isNull()
    | ~F.col("seller_id").rlike(r"^[a-f0-9]{32}$")
)

good_count = sellers_good.count()
bad_count = sellers_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

sellers_good.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.olist_sellers_dataset"
)

if bad_count > 0:
    sellers_quarantine.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.olist_sellers_dataset_quarantine"
    )

print(f"Saved: {BRONZE}.olist_sellers_dataset ({good_count:,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.olist_sellers_dataset;

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/olist_orders_dataset.csv"

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])

orders_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(orders_schema)
    .csv(CSV_FILE)
)

display(orders_raw.limit(5))
orders_raw.printSchema()

# COMMAND ----------

total = orders_raw.count()
rescued = orders_raw.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = orders_raw.select(
    F.sum(F.col("order_id").isNull().cast("int")).alias("null_order_id"),
    F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer_id"),
    F.sum(F.col("order_status").isNull().cast("int")).alias("null_order_status"),
    F.sum(F.col("order_purchase_timestamp").isNull().cast("int")).alias("null_purchase_ts"),
    F.sum(F.col("order_approved_at").isNull().cast("int")).alias("null_approved_at"),
    F.sum(F.col("order_delivered_carrier_date").isNull().cast("int")).alias("null_carrier_date"),
    F.sum(F.col("order_delivered_customer_date").isNull().cast("int")).alias("null_customer_date"),
    F.sum(F.col("order_estimated_delivery_date").isNull().cast("int")).alias("null_estimated_date"),
)

dupes = (
    orders_raw.groupBy("order_id").count()
    .filter(F.col("count") > 1).count()
)

bad_order_id = orders_raw.filter(
    ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
).count()

bad_customer_id = orders_raw.filter(
    ~F.col("customer_id").rlike(r"^[a-f0-9]{32}$")
).count()

print(f"Total rows        : {total:,}")
print(f"Rescued rows      : {rescued:,}")
print(f"Duplicate order_id: {dupes:,}")
print(f"Bad order_id      : {bad_order_id:,}")
print(f"Bad customer_id   : {bad_customer_id:,}")

display(null_checks)

# COMMAND ----------

file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

orders_bronze = (
    orders_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

orders_good = orders_bronze.filter(
    F.col("_rescued_data").isNull()
    & F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("order_id").rlike(r"^[a-f0-9]{32}$")
    & F.col("customer_id").rlike(r"^[a-f0-9]{32}$")
    & F.col("order_purchase_timestamp").isNotNull()
    & F.col("order_estimated_delivery_date").isNotNull()
)

orders_quarantine = orders_bronze.filter(
    F.col("_rescued_data").isNotNull()
    | F.col("order_id").isNull()
    | F.col("customer_id").isNull()
    | ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
    | ~F.col("customer_id").rlike(r"^[a-f0-9]{32}$")
    | F.col("order_purchase_timestamp").isNull()
    | F.col("order_estimated_delivery_date").isNull()
)

good_count = orders_good.count()
bad_count = orders_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

orders_good.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.olist_orders_dataset"
)

if bad_count > 0:
    orders_quarantine.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.olist_orders_dataset_quarantine"
    )

print(f"Saved: {BRONZE}.olist_orders_dataset ({good_count:,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.olist_orders_dataset;

# COMMAND ----------

from pyspark.sql.types import (
    DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/olist_order_items_dataset.csv"

order_items_schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType()),
])

order_items_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(order_items_schema)
    .csv(CSV_FILE)
)

display(order_items_raw.limit(5))
order_items_raw.printSchema()

# COMMAND ----------

total = order_items_raw.count()
rescued = order_items_raw.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = order_items_raw.select(
    F.sum(F.col("order_id").isNull().cast("int")).alias("null_order_id"),
    F.sum(F.col("order_item_id").isNull().cast("int")).alias("null_order_item_id"),
    F.sum(F.col("product_id").isNull().cast("int")).alias("null_product_id"),
    F.sum(F.col("seller_id").isNull().cast("int")).alias("null_seller_id"),
    F.sum(F.col("shipping_limit_date").isNull().cast("int")).alias("null_shipping_date"),
    F.sum(F.col("price").isNull().cast("int")).alias("null_price"),
    F.sum(F.col("freight_value").isNull().cast("int")).alias("null_freight"),
)

dupes = (
    order_items_raw
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

bad_order_id = order_items_raw.filter(
    ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
).count()

bad_product_id = order_items_raw.filter(
    ~F.col("product_id").rlike(r"^[a-f0-9]{32}$")
).count()

bad_seller_id = order_items_raw.filter(
    ~F.col("seller_id").rlike(r"^[a-f0-9]{32}$")
).count()

negative_price = order_items_raw.filter(F.col("price") < 0).count()
negative_freight = order_items_raw.filter(F.col("freight_value") < 0).count()

print(f"Total rows           : {total:,}")
print(f"Rescued rows         : {rescued:,}")
print(f"Duplicate line items : {dupes:,}")
print(f"Bad order_id         : {bad_order_id:,}")
print(f"Bad product_id       : {bad_product_id:,}")
print(f"Bad seller_id        : {bad_seller_id:,}")
print(f"Negative price       : {negative_price:,}")
print(f"Negative freight     : {negative_freight:,}")

display(null_checks)

# COMMAND ----------

file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

order_items_bronze = (
    order_items_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

order_items_good = order_items_bronze.filter(
    F.col("_rescued_data").isNull()
    & F.col("order_id").isNotNull()
    & F.col("order_item_id").isNotNull()
    & F.col("product_id").isNotNull()
    & F.col("seller_id").isNotNull()
    & F.col("order_id").rlike(r"^[a-f0-9]{32}$")
    & F.col("product_id").rlike(r"^[a-f0-9]{32}$")
    & F.col("seller_id").rlike(r"^[a-f0-9]{32}$")
    & F.col("price").isNotNull()
    & F.col("freight_value").isNotNull()
    & (F.col("price") >= 0)
    & (F.col("freight_value") >= 0)
)

order_items_quarantine = order_items_bronze.filter(
    F.col("_rescued_data").isNotNull()
    | F.col("order_id").isNull()
    | F.col("order_item_id").isNull()
    | F.col("product_id").isNull()
    | F.col("seller_id").isNull()
    | ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
    | ~F.col("product_id").rlike(r"^[a-f0-9]{32}$")
    | ~F.col("seller_id").rlike(r"^[a-f0-9]{32}$")
    | F.col("price").isNull()
    | F.col("freight_value").isNull()
    | (F.col("price") < 0)
    | (F.col("freight_value") < 0)
)

good_count = order_items_good.count()
bad_count = order_items_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

order_items_good.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.olist_order_items_dataset"
)

if bad_count > 0:
    order_items_quarantine.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.olist_order_items_dataset_quarantine"
    )

print(f"Saved: {BRONZE}.olist_order_items_dataset ({good_count:,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.olist_order_items_dataset;

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/olist_order_payments_dataset.csv"

payments_schema = StructType([
    StructField("order_id", StringType()),
    StructField("payment_sequential", IntegerType()),
    StructField("payment_type", StringType()),
    StructField("payment_installments", IntegerType()),
    StructField("payment_value", DoubleType()),
])

payments_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(payments_schema)
    .csv(CSV_FILE)
)

display(payments_raw.limit(5))
payments_raw.printSchema()

# COMMAND ----------

total = payments_raw.count()
rescued = payments_raw.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = payments_raw.select(
    F.sum(F.col("order_id").isNull().cast("int")).alias("null_order_id"),
    F.sum(F.col("payment_sequential").isNull().cast("int")).alias("null_payment_sequential"),
    F.sum(F.col("payment_type").isNull().cast("int")).alias("null_payment_type"),
    F.sum(F.col("payment_installments").isNull().cast("int")).alias("null_installments"),
    F.sum(F.col("payment_value").isNull().cast("int")).alias("null_payment_value"),
)

dupes = (
    payments_raw
    .groupBy("order_id", "payment_sequential")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

bad_order_id = payments_raw.filter(
    ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
).count()

negative_value = payments_raw.filter(F.col("payment_value") < 0).count()
zero_installments = payments_raw.filter(F.col("payment_installments") < 1).count()

print(f"Total rows              : {total:,}")
print(f"Rescued rows            : {rescued:,}")
print(f"Duplicate payment rows  : {dupes:,}")
print(f"Bad order_id            : {bad_order_id:,}")
print(f"Negative payment_value  : {negative_value:,}")
print(f"Installments < 1        : {zero_installments:,}")

display(null_checks)

# COMMAND ----------

file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

payments_bronze = (
    payments_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

payments_good = payments_bronze.filter(
    F.col("_rescued_data").isNull()
    & F.col("order_id").isNotNull()
    & F.col("payment_sequential").isNotNull()
    & F.col("payment_type").isNotNull()
    & F.col("payment_installments").isNotNull()
    & F.col("payment_value").isNotNull()
    & F.col("order_id").rlike(r"^[a-f0-9]{32}$")
    & (F.col("payment_value") >= 0)
    & (F.col("payment_installments") >= 1)
)

payments_quarantine = payments_bronze.filter(
    F.col("_rescued_data").isNotNull()
    | F.col("order_id").isNull()
    | F.col("payment_sequential").isNull()
    | F.col("payment_type").isNull()
    | F.col("payment_installments").isNull()
    | F.col("payment_value").isNull()
    | ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
    | (F.col("payment_value") < 0)
    | (F.col("payment_installments") < 1)
)

good_count = payments_good.count()
bad_count = payments_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

display(
    payments_quarantine.select(
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value", "_rescued_data"
    )
)

# COMMAND ----------

payments_good.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.olist_order_payments_dataset"
)

if bad_count > 0:
    payments_quarantine.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.olist_order_payments_dataset_quarantine"
    )

print(f"Saved: {BRONZE}.olist_order_payments_dataset ({good_count:,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.olist_order_payments_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.olist_order_payments_dataset_quarantine;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mypractice.brazilian_ecommerce_bronze.olist_order_payments_dataset_quarantine;

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/olist_order_reviews_dataset.csv"

reviews_schema = StructType([
    StructField("review_id", StringType()),
    StructField("order_id", StringType()),
    StructField("review_score", StringType()),          # string at bronze — score validated below
    StructField("review_comment_title", StringType()),
    StructField("review_comment_message", StringType()),
    StructField("review_creation_date", StringType()),  # string at bronze — try_cast in profile
    StructField("review_answer_timestamp", StringType()),
])

reviews_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(reviews_schema)
    .csv(CSV_FILE)
)

display(reviews_raw.limit(10))

# COMMAND ----------

reviews_check = reviews_raw.withColumn(
    "score_int", F.expr("try_cast(review_score AS INT)")
).withColumn(
    "created_ts", F.expr("try_cast(review_creation_date AS TIMESTAMP)")
).withColumn(
    "answer_ts", F.expr("try_cast(review_answer_timestamp AS TIMESTAMP)")
)

total = reviews_check.count()
rescued = reviews_check.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = reviews_check.select(
    F.sum(F.col("review_id").isNull().cast("int")).alias("null_review_id"),
    F.sum(F.col("order_id").isNull().cast("int")).alias("null_order_id"),
    F.sum(F.col("review_score").isNull().cast("int")).alias("null_review_score"),
    F.sum(F.col("review_comment_title").isNull().cast("int")).alias("null_title"),
    F.sum(F.col("review_comment_message").isNull().cast("int")).alias("null_message"),
    F.sum(F.col("created_ts").isNull().cast("int")).alias("null_creation_ts"),
    F.sum(F.col("answer_ts").isNull().cast("int")).alias("null_answer_ts"),
)

bad_review_id = reviews_check.filter(
    ~F.col("review_id").rlike(r"^[a-f0-9]{32}$")
).count()

bad_order_id = reviews_check.filter(
    ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
).count()

bad_score = reviews_check.filter(
    ~F.col("score_int").between(1, 5)
).count()

dupes = (
    reviews_check.groupBy("review_id").count()
    .filter(F.col("count") > 1).count()
)

print(f"Total rows           : {total:,}")
print(f"Rescued rows         : {rescued:,}")
print(f"Bad review_id (UUID) : {bad_review_id:,}")
print(f"Bad order_id (UUID)  : {bad_order_id:,}")
print(f"Invalid score (1-5)  : {bad_score:,}")
print(f"Duplicate review_id  : {dupes:,}")

display(null_checks)

# COMMAND ----------

display(
    reviews_check.filter(
        F.col("_rescued_data").isNotNull()
        | ~F.col("review_id").rlike(r"^[a-f0-9]{32}$")
        | ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
        | ~F.col("score_int").between(1, 5)
    ).select(
        "review_id", "order_id", "review_score",
        "review_creation_date", "review_comment_title", "_rescued_data"
    ).limit(30)
)

# COMMAND ----------

file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

reviews_bronze = (
    reviews_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

reviews_checked = reviews_bronze.withColumn(
    "score_int", F.expr("try_cast(review_score AS INT)")
)

reviews_good = reviews_checked.filter(
    F.col("_rescued_data").isNull()
    & F.col("review_id").rlike(r"^[a-f0-9]{32}$")
    & F.col("order_id").rlike(r"^[a-f0-9]{32}$")
    & F.col("score_int").between(1, 5)
)

reviews_quarantine = reviews_checked.filter(
    F.col("_rescued_data").isNotNull()
    | ~F.col("review_id").rlike(r"^[a-f0-9]{32}$")
    | ~F.col("order_id").rlike(r"^[a-f0-9]{32}$")
    | ~F.col("score_int").between(1, 5)
)

good_count = reviews_good.count()
bad_count = reviews_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

print(f"Good: {good_count:,}  Quarantine: {bad_count:,}")
print(f"Sum check: {good_count + bad_count:,} vs total {total:,}")

# COMMAND ----------

# Drop helper column before save
reviews_good_clean = reviews_good.drop("score_int")
reviews_quarantine_clean = reviews_quarantine.drop("score_int")

reviews_good_clean.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.olist_order_reviews_dataset"
)

if bad_count > 0:
    reviews_quarantine_clean.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.olist_order_reviews_dataset_quarantine"
    )

print(f"Saved: {BRONZE}.olist_order_reviews_dataset ({good_count:,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.olist_order_reviews_dataset;

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/olist_products_dataset.csv"

products_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_category_name", StringType()),
    StructField("product_name_lenght", IntegerType()),        # typo kept — raw bronze
    StructField("product_description_lenght", IntegerType()),
    StructField("product_photos_qty", IntegerType()),
    StructField("product_weight_g", DoubleType()),
    StructField("product_length_cm", DoubleType()),
    StructField("product_height_cm", DoubleType()),
    StructField("product_width_cm", DoubleType()),
])

products_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(products_schema)
    .csv(CSV_FILE)
)

display(products_raw.limit(5))
products_raw.printSchema()

# COMMAND ----------

total = products_raw.count()
rescued = products_raw.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = products_raw.select(
    F.sum(F.col("product_id").isNull().cast("int")).alias("null_product_id"),
    F.sum(F.col("product_category_name").isNull().cast("int")).alias("null_category"),
    F.sum(F.col("product_name_lenght").isNull().cast("int")).alias("null_name_length"),
    F.sum(F.col("product_description_lenght").isNull().cast("int")).alias("null_desc_length"),
    F.sum(F.col("product_weight_g").isNull().cast("int")).alias("null_weight"),
)

dupes = (
    products_raw.groupBy("product_id").count()
    .filter(F.col("count") > 1).count()
)

bad_product_id = products_raw.filter(
    ~F.col("product_id").rlike(r"^[a-f0-9]{32}$")
).count()

negative_weight = products_raw.filter(F.col("product_weight_g") < 0).count()

print(f"Total rows           : {total:,}")
print(f"Rescued rows         : {rescued:,}")
print(f"Duplicate product_id : {dupes:,}")
print(f"Bad product_id       : {bad_product_id:,}")
print(f"Null category        : {null_checks.collect()[0]['null_category']:,}")
print(f"Negative weight      : {negative_weight:,}")

display(null_checks)

# COMMAND ----------

file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

products_bronze = (
    products_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

products_good = products_bronze.filter(
    F.col("_rescued_data").isNull()
    & F.col("product_id").isNotNull()
    & F.col("product_id").rlike(r"^[a-f0-9]{32}$")
)

products_quarantine = products_bronze.filter(
    F.col("_rescued_data").isNotNull()
    | F.col("product_id").isNull()
    | ~F.col("product_id").rlike(r"^[a-f0-9]{32}$")
)

good_count = products_good.count()
bad_count = products_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

products_good.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.olist_products_dataset"
)

if bad_count > 0:
    products_quarantine.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.olist_products_dataset_quarantine"
    )

print(f"Saved: {BRONZE}.olist_products_dataset ({good_count:,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.olist_products_dataset;

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/olist_geolocation_dataset.csv"

geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType()),
    StructField("geolocation_lat", DoubleType()),
    StructField("geolocation_lng", DoubleType()),
    StructField("geolocation_city", StringType()),
    StructField("geolocation_state", StringType()),
])

geolocation_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(geolocation_schema)
    .csv(CSV_FILE)
)

display(geolocation_raw.limit(5))

# COMMAND ----------

total = geolocation_raw.count()
rescued = geolocation_raw.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = geolocation_raw.select(
    F.sum(F.col("geolocation_zip_code_prefix").isNull().cast("int")).alias("null_zip"),
    F.sum(F.col("geolocation_lat").isNull().cast("int")).alias("null_lat"),
    F.sum(F.col("geolocation_lng").isNull().cast("int")).alias("null_lng"),
    F.sum(F.col("geolocation_city").isNull().cast("int")).alias("null_city"),
    F.sum(F.col("geolocation_state").isNull().cast("int")).alias("null_state"),
)

unique_zips = geolocation_raw.select("geolocation_zip_code_prefix").distinct().count()

dup_zip_rows = total - unique_zips

bad_zip = geolocation_raw.filter(
    ~F.col("geolocation_zip_code_prefix").rlike(r"^\d{5}$")
).count()

bad_state = geolocation_raw.filter(
    ~F.col("geolocation_state").rlike(r"^[A-Z]{2}$")
).count()

outside_brazil = geolocation_raw.filter(
    ~F.col("geolocation_lat").between(-33, 5)
    | ~F.col("geolocation_lng").between(-73, -35)
).count()

print(f"Total rows              : {total:,}")
print(f"Unique zip prefixes     : {unique_zips:,}")
print(f"Extra rows (dup zips)   : {dup_zip_rows:,}")
print(f"Rescued rows            : {rescued:,}")
print(f"Bad zip format          : {bad_zip:,}")
print(f"Bad state format        : {bad_state:,}")
print(f"Outside Brazil bbox     : {outside_brazil:,}")

display(null_checks)

# COMMAND ----------

file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

geolocation_bronze = (
    geolocation_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

geolocation_good = geolocation_bronze.filter(
    F.col("_rescued_data").isNull()
    & F.col("geolocation_zip_code_prefix").isNotNull()
    & F.col("geolocation_lat").isNotNull()
    & F.col("geolocation_lng").isNotNull()
    & F.col("geolocation_zip_code_prefix").rlike(r"^\d{5}$")
)

geolocation_quarantine = geolocation_bronze.filter(
    F.col("_rescued_data").isNotNull()
    | F.col("geolocation_zip_code_prefix").isNull()
    | F.col("geolocation_lat").isNull()
    | F.col("geolocation_lng").isNull()
    | ~F.col("geolocation_zip_code_prefix").rlike(r"^\d{5}$")
)

good_count = geolocation_good.count()
bad_count = geolocation_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

geolocation_good.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.olist_geolocation_dataset"
)

if bad_count > 0:
    geolocation_quarantine.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.olist_geolocation_dataset_quarantine"
    )

print(f"Saved: {BRONZE}.olist_geolocation_dataset ({good_count:,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.olist_geolocation_dataset;

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
BASE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{BRONZE_VOLUME}/archive"
CSV_FILE = f"{BASE_PATH}/product_category_name_translation.csv"

translation_schema = StructType([
    StructField("product_category_name", StringType()),
    StructField("product_category_name_english", StringType()),
])

translation_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(translation_schema)
    .csv(CSV_FILE)
)

display(translation_raw.limit(10))

# COMMAND ----------

total = translation_raw.count()
rescued = translation_raw.filter(F.col("_rescued_data").isNotNull()).count()

null_checks = translation_raw.select(
    F.sum(F.col("product_category_name").isNull().cast("int")).alias("null_pt_name"),
    F.sum(F.col("product_category_name_english").isNull().cast("int")).alias("null_en_name"),
)

dup_pt = (
    translation_raw
    .groupBy("product_category_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

dup_en = (
    translation_raw
    .groupBy("product_category_name_english")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

blank_pt = translation_raw.filter(
    F.trim(F.col("product_category_name")) == ""
).count()

blank_en = translation_raw.filter(
    F.trim(F.col("product_category_name_english")) == ""
).count()

print(f"Total rows           : {total:,}")
print(f"Rescued rows         : {rescued:,}")
print(f"Duplicate PT names   : {dup_pt:,}")
print(f"Duplicate EN names   : {dup_en:,}")
print(f"Blank PT names       : {blank_pt:,}")
print(f"Blank EN names       : {blank_en:,}")

display(null_checks)

# COMMAND ----------

file_info = dbutils.fs.ls(CSV_FILE)[0]
source_modified_at = F.from_unixtime(F.lit(file_info.modificationTime / 1000))

translation_bronze = (
    translation_raw
    .withColumn("_source_file", F.lit(CSV_FILE))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_modified_at", source_modified_at)
)

# COMMAND ----------

translation_good = translation_bronze.filter(
    F.col("_rescued_data").isNull()
    & F.col("product_category_name").isNotNull()
    & F.col("product_category_name_english").isNotNull()
    & (F.trim(F.col("product_category_name")) != "")
    & (F.trim(F.col("product_category_name_english")) != "")
)

translation_quarantine = translation_bronze.filter(
    F.col("_rescued_data").isNotNull()
    | F.col("product_category_name").isNull()
    | F.col("product_category_name_english").isNull()
    | (F.trim(F.col("product_category_name")) == "")
    | (F.trim(F.col("product_category_name_english")) == "")
)

good_count = translation_good.count()
bad_count = translation_quarantine.count()

print(f"Good rows       : {good_count:,}")
print(f"Quarantine rows : {bad_count:,}")

# COMMAND ----------

translation_good.write.mode("overwrite").saveAsTable(
    f"{BRONZE}.product_category_name_translation"
)

if bad_count > 0:
    translation_quarantine.write.mode("overwrite").saveAsTable(
        f"{BRONZE}.product_category_name_translation_quarantine"
    )

print(f"Saved: {BRONZE}.product_category_name_translation ({good_count:,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table mypractice.brazilian_ecommerce_bronze.product_category_name_translation;
