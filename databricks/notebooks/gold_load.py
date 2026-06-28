# Databricks notebook source
# MAGIC %md
# MAGIC # gold_load
# MAGIC
# MAGIC Business-ready tables / materialized views in **`brazilian_ecommerce_gold`**.
# MAGIC
# MAGIC Run **`silver_load`** first.
# MAGIC
# MAGIC Add materialized views one at a time as silver tables are ready.

# COMMAND ----------

dbutils.widgets.text("catalog", "mypractice")
dbutils.widgets.text("silver_schema", "brazilian_ecommerce_silver")
dbutils.widgets.text("gold_schema", "brazilian_ecommerce_gold")

CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")

SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD = f"{CATALOG}.{GOLD_SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

print(f"Silver : {SILVER}")
print(f"Gold   : {GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Planned views (add when silver is complete)
# MAGIC
# MAGIC | View | Purpose |
# MAGIC |------|---------|
# MAGIC | `mv_monthly_revenue` | Orders and revenue by month |
# MAGIC | `mv_revenue_by_category` | Top categories by revenue |
# MAGIC | `mv_delivery_performance` | Late delivery rate by state |
# MAGIC | `mv_payment_method_breakdown` | Payment type share |
# MAGIC | `mv_order_funnel` | Count by order_status |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_gold.mv_order_funnel AS
# MAGIC SELECT
# MAGIC     order_status,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_all_orders
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_orders_dataset
# MAGIC GROUP BY order_status
# MAGIC ORDER BY order_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mypractice.brazilian_ecommerce_gold.mv_order_funnel;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_gold.mv_monthly_revenue AS
# MAGIC SELECT
# MAGIC     DATE_TRUNC('MONTH', o.order_purchase_timestamp) AS order_month,
# MAGIC     COUNT(DISTINCT o.order_id) AS delivered_orders,
# MAGIC     COUNT(*) AS line_items,
# MAGIC     ROUND(SUM(oi.price), 2) AS product_revenue,
# MAGIC     ROUND(SUM(oi.freight_value), 2) AS freight_revenue,
# MAGIC     ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_orders_dataset o
# MAGIC INNER JOIN mypractice.brazilian_ecommerce_silver.olist_order_items_dataset oi
# MAGIC     ON o.order_id = oi.order_id
# MAGIC WHERE o.order_status = 'delivered'
# MAGIC GROUP BY DATE_TRUNC('MONTH', o.order_purchase_timestamp)
# MAGIC ORDER BY order_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS months, ROUND(SUM(total_revenue), 2) AS lifetime_revenue
# MAGIC FROM mypractice.brazilian_ecommerce_gold.mv_monthly_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_gold.mv_revenue_by_category AS
# MAGIC SELECT
# MAGIC     COALESCE(p.product_category_name_english, p.product_category_name, 'unknown') AS category_english,
# MAGIC     p.product_category_name AS category_portuguese,
# MAGIC     COUNT(DISTINCT oi.order_id) AS delivered_orders,
# MAGIC     COUNT(*) AS line_items,
# MAGIC     ROUND(SUM(oi.price), 2) AS product_revenue,
# MAGIC     ROUND(SUM(oi.freight_value), 2) AS freight_revenue,
# MAGIC     ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_order_items_dataset oi
# MAGIC INNER JOIN mypractice.brazilian_ecommerce_silver.olist_orders_dataset o
# MAGIC     ON oi.order_id = o.order_id
# MAGIC INNER JOIN mypractice.brazilian_ecommerce_silver.olist_products_dataset p
# MAGIC     ON oi.product_id = p.product_id
# MAGIC WHERE o.order_status = 'delivered'
# MAGIC GROUP BY
# MAGIC     COALESCE(p.product_category_name_english, p.product_category_name, 'unknown'),
# MAGIC     p.product_category_name
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category_english, total_revenue
# MAGIC FROM mypractice.brazilian_ecommerce_gold.mv_revenue_by_category
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_gold.mv_payment_method_breakdown AS
# MAGIC SELECT
# MAGIC     p.payment_type,
# MAGIC     COUNT(*) AS payment_lines,
# MAGIC     COUNT(DISTINCT p.order_id) AS delivered_orders,
# MAGIC     ROUND(SUM(p.payment_value), 2) AS total_payment_value,
# MAGIC     ROUND(AVG(p.payment_installments), 2) AS avg_installments,
# MAGIC     ROUND(100.0 * SUM(p.payment_value) / SUM(SUM(p.payment_value)) OVER (), 2) AS pct_of_payment_value
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_order_payments_dataset p
# MAGIC INNER JOIN mypractice.brazilian_ecommerce_silver.olist_orders_dataset o
# MAGIC     ON p.order_id = o.order_id
# MAGIC WHERE o.order_status = 'delivered'
# MAGIC GROUP BY p.payment_type
# MAGIC ORDER BY total_payment_value DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mypractice.brazilian_ecommerce_gold.mv_payment_method_breakdown;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mypractice.brazilian_ecommerce_gold.mv_delivery_performance AS
# MAGIC SELECT
# MAGIC     c.customer_state,
# MAGIC     COUNT(*) AS delivered_orders,
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS late_deliveries,
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(
# MAGIC             CASE
# MAGIC                 WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) / COUNT(*),
# MAGIC         2
# MAGIC     ) AS late_delivery_pct,
# MAGIC     ROUND(
# MAGIC         AVG(
# MAGIC             DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS avg_days_vs_estimate
# MAGIC FROM mypractice.brazilian_ecommerce_silver.olist_orders_dataset o
# MAGIC INNER JOIN mypractice.brazilian_ecommerce_silver.olist_customers_dataset c
# MAGIC     ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_status = 'delivered'
# MAGIC   AND o.order_delivered_customer_date IS NOT NULL
# MAGIC   AND o.order_estimated_delivery_date IS NOT NULL
# MAGIC GROUP BY c.customer_state
# MAGIC ORDER BY delivered_orders DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(delivered_orders) AS total_delivered,
# MAGIC     SUM(late_deliveries) AS total_late,
# MAGIC     ROUND(100.0 * SUM(late_deliveries) / SUM(delivered_orders), 2) AS overall_late_pct
# MAGIC FROM mypractice.brazilian_ecommerce_gold.mv_delivery_performance;

# COMMAND ----------

for row in spark.sql("SHOW TABLES IN mypractice.brazilian_ecommerce_gold").collect():
    if row.isTemporary or row.tableName.startswith("_"):
        continue
    n = spark.table(f"mypractice.brazilian_ecommerce_gold.{row.tableName}").count()
    print(f"{row.tableName:45s} {n:>10,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold business rules
# MAGIC
# MAGIC - **Revenue / payments / category:** `order_status = 'delivered'` only
# MAGIC - **Categories:** `COALESCE(english, portuguese, 'unknown')`
# MAGIC - **Late delivery:** `delivered_customer_date > estimated_delivery_date`
# MAGIC - **Source:** silver only — no bronze, no quarantine
# MAGIC - **Refresh:** one-time batch build (replace tables on re-run)
