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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.olist_customers_dataset AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_unique_id,
# MAGIC     LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0') AS customer_zip_code_prefix,
# MAGIC     LOWER(TRIM(customer_city)) AS customer_city,
# MAGIC     UPPER(TRIM(customer_state)) AS customer_state
# MAGIC FROM mypractice.brazilian_ecommerce_bronze.olist_customers_dataset
# MAGIC WHERE customer_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND customer_unique_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND customer_zip_code_prefix RLIKE '^\\d{5}$'
# MAGIC   AND customer_state RLIKE '^[A-Z]{2}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count FROM mypractice.brazilian_ecommerce_silver.olist_customers_dataset;
# MAGIC -- expect ~99,441

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_state, COUNT(*) AS n
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_customers_dataset
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY n DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.olist_sellers_dataset AS
# MAGIC SELECT
# MAGIC     seller_id,
# MAGIC     LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0') AS seller_zip_code_prefix,
# MAGIC     LOWER(TRIM(seller_city)) AS seller_city,
# MAGIC     UPPER(TRIM(seller_state)) AS seller_state
# MAGIC FROM mypractice.brazilian_ecommerce_bronze.olist_sellers_dataset
# MAGIC WHERE seller_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND seller_zip_code_prefix RLIKE '^\\d{5}$'
# MAGIC   AND seller_state RLIKE '^[A-Z]{2}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as rowcount from mypractice.brazilian_ecommerce_silver.olist_sellers_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.product_category_name_translation AS
# MAGIC SELECT
# MAGIC     LOWER(TRIM(product_category_name)) AS product_category_name,
# MAGIC     LOWER(TRIM(product_category_name_english)) AS product_category_name_english
# MAGIC FROM mypractice.brazilian_ecommerce_bronze.product_category_name_translation
# MAGIC WHERE product_category_name IS NOT NULL
# MAGIC   AND TRIM(product_category_name) <> ''
# MAGIC   AND product_category_name_english IS NOT NULL
# MAGIC   AND TRIM(product_category_name_english) <> ''

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as rowcount from mypractice.brazilian_ecommerce_silver.product_category_name_translation;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.olist_products_dataset AS
# MAGIC SELECT
# MAGIC     p.product_id,
# MAGIC     LOWER(TRIM(p.product_category_name)) AS product_category_name,
# MAGIC     t.product_category_name_english,
# MAGIC     CAST(p.product_name_lenght AS INT) AS product_name_length,
# MAGIC     CAST(p.product_description_lenght AS INT) AS product_description_length,
# MAGIC     CAST(p.product_photos_qty AS INT) AS product_photos_qty,
# MAGIC     CAST(p.product_weight_g AS DOUBLE) AS product_weight_g,
# MAGIC     CAST(p.product_length_cm AS DOUBLE) AS product_length_cm,
# MAGIC     CAST(p.product_height_cm AS DOUBLE) AS product_height_cm,
# MAGIC     CAST(p.product_width_cm AS DOUBLE) AS product_width_cm
# MAGIC FROM mypractice.brazilian_ecommerce_bronze.olist_products_dataset p
# MAGIC LEFT JOIN mypractice.brazilian_ecommerce_silver.product_category_name_translation t
# MAGIC   ON LOWER(TRIM(p.product_category_name)) = t.product_category_name
# MAGIC WHERE p.product_id RLIKE '^[a-f0-9]{32}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as rowcount from mypractice.brazilian_ecommerce_silver.olist_products_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total,
# MAGIC   SUM(CASE WHEN product_category_name_english IS NULL THEN 1 ELSE 0 END) AS missing_english
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_products_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.olist_geolocation_dataset AS
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     LPAD(CAST(geolocation_zip_code_prefix AS STRING), 5, '0') AS geolocation_zip_code_prefix,
# MAGIC     geolocation_lat,
# MAGIC     geolocation_lng,
# MAGIC     LOWER(TRIM(geolocation_city)) AS geolocation_city,
# MAGIC     UPPER(TRIM(geolocation_state)) AS geolocation_state
# MAGIC   FROM mypractice.brazilian_ecommerce_bronze.olist_geolocation_dataset
# MAGIC   WHERE geolocation_zip_code_prefix RLIKE '^\\d{5}$'
# MAGIC     AND geolocation_lat BETWEEN -33 AND 5
# MAGIC     AND geolocation_lng BETWEEN -73 AND -35
# MAGIC     AND geolocation_state RLIKE '^[A-Z]{2}$'
# MAGIC ),
# MAGIC deduped AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY geolocation_zip_code_prefix
# MAGIC       ORDER BY geolocation_lat, geolocation_lng
# MAGIC     ) AS rn
# MAGIC   FROM cleaned
# MAGIC )
# MAGIC SELECT
# MAGIC   geolocation_zip_code_prefix,
# MAGIC   geolocation_lat,
# MAGIC   geolocation_lng,
# MAGIC   geolocation_city,
# MAGIC   geolocation_state
# MAGIC FROM deduped
# MAGIC WHERE rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as rowcount from mypractice.brazilian_ecommerce_silver.olist_geolocation_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows, COUNT(DISTINCT geolocation_zip_code_prefix) AS unique_zips
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_geolocation_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN product_category_name IS NULL THEN 'null_category'
# MAGIC     ELSE 'no_translation_match'
# MAGIC   END AS reason,
# MAGIC   COUNT(*) AS n
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_products_dataset
# MAGIC WHERE product_category_name_english IS NULL
# MAGIC GROUP BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_category_name,
# MAGIC   COUNT(*) AS n
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_products_dataset
# MAGIC WHERE product_category_name_english IS NULL
# MAGIC GROUP BY product_category_name
# MAGIC ORDER BY n DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.olist_orders_dataset AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
# MAGIC     CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
# MAGIC     CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
# MAGIC     CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
# MAGIC     CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
# MAGIC FROM mypractice.brazilian_ecommerce_bronze.olist_orders_dataset
# MAGIC WHERE order_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND customer_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND order_status IS NOT NULL
# MAGIC   AND TRIM(order_status) <> ''

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rowcount FROM mypractice.brazilian_ecommerce_silver.olist_orders_dataset;
# MAGIC -- expect ~99,441
# MAGIC
# MAGIC SELECT order_status, COUNT(*) AS n
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_orders_dataset
# MAGIC GROUP BY order_status
# MAGIC ORDER BY n DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.olist_order_items_dataset AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     CAST(order_item_id AS INT) AS order_item_id,
# MAGIC     product_id,
# MAGIC     seller_id,
# MAGIC     CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
# MAGIC     CAST(price AS DOUBLE) AS price,
# MAGIC     CAST(freight_value AS DOUBLE) AS freight_value
# MAGIC FROM mypractice.brazilian_ecommerce_bronze.olist_order_items_dataset
# MAGIC WHERE order_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND product_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND seller_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND order_item_id IS NOT NULL
# MAGIC   AND price IS NOT NULL
# MAGIC   AND freight_value IS NOT NULL
# MAGIC   AND price >= 0
# MAGIC   AND freight_value >= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rowcount FROM mypractice.brazilian_ecommerce_silver.olist_order_items_dataset;
# MAGIC -- expect ~112,650

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total,
# MAGIC   COUNT(DISTINCT order_id) AS distinct_orders
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_order_items_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.olist_order_payments_dataset AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     CAST(payment_sequential AS INT) AS payment_sequential,
# MAGIC     CAST(payment_type AS STRING) AS payment_type,
# MAGIC     CAST(payment_installments AS INT) AS payment_installments,
# MAGIC     CAST(payment_value AS DOUBLE) AS payment_value
# MAGIC FROM mypractice.brazilian_ecommerce_bronze.olist_order_payments_dataset
# MAGIC WHERE order_id RLIKE '^[a-f0-9]{32}$'
# MAGIC   AND payment_sequential IS NOT NULL
# MAGIC   AND payment_installments IS NOT NULL
# MAGIC   AND payment_value IS NOT NULL
# MAGIC   AND payment_installments >= 1
# MAGIC   AND payment_value >= 0
# MAGIC   AND LOWER(TRIM(payment_type)) IN (
# MAGIC     'credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rowcount
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_order_payments_dataset;
# MAGIC -- expect ~103,884 (bronze good rows minus any extra silver filters)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT payment_type, COUNT(*) AS n
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_order_payments_dataset
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY n DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_silver.olist_order_reviews_dataset AS
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     review_id,
# MAGIC     order_id,
# MAGIC     CAST(TRY_CAST(review_score AS INT) AS INT) AS review_score,
# MAGIC     CAST(TRY_CAST(review_creation_date AS TIMESTAMP) AS TIMESTAMP) AS review_creation_date,
# MAGIC     CAST(TRY_CAST(review_answer_timestamp AS TIMESTAMP) AS TIMESTAMP) AS review_answer_timestamp,
# MAGIC     TRIM(review_comment_title) AS review_comment_title,
# MAGIC     TRIM(review_comment_message) AS review_comment_message,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_creation_date) AS rn
# MAGIC   FROM mypractice.brazilian_ecommerce_bronze.olist_order_reviews_dataset
# MAGIC   WHERE review_id RLIKE '^[a-f0-9]{32}$'
# MAGIC     AND order_id RLIKE '^[a-f0-9]{32}$'
# MAGIC     AND TRY_CAST(review_score AS INT) BETWEEN 1 AND 5
# MAGIC     AND TRY_CAST(review_creation_date AS TIMESTAMP) IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   review_id, order_id, review_score,
# MAGIC   review_creation_date, review_answer_timestamp,
# MAGIC   review_comment_title, review_comment_message
# MAGIC FROM cleaned
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rowcount
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_order_reviews_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT review_score, COUNT(*) AS n
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_order_reviews_dataset
# MAGIC GROUP BY review_score
# MAGIC ORDER BY review_score;

# COMMAND ----------

for row in spark.sql("SHOW TABLES IN mypractice.brazilian_ecommerce_silver").collect():
    if row.isTemporary or row.tableName.startswith("_"):
        continue
    n = spark.table(f"mypractice.brazilian_ecommerce_silver.{row.tableName}").count()
    print(f"{row.tableName:45s} {n:>10,} rows")
