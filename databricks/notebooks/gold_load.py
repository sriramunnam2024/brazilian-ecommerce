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

for row in spark.sql(f"SHOW TABLES IN {GOLD}").collect():
    n = spark.table(f"{GOLD}.{row.tableName}").count()
    print(f"{row.tableName:45s} {n:>10,} rows")
