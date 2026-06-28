# Databricks notebook source
# MAGIC %md
# MAGIC # 00_cleanup_brazilian_schemas
# MAGIC
# MAGIC **Destructive** — drops medallion tables/schemas so you can re-run bronze → silver → gold from scratch.
# MAGIC
# MAGIC **Never drops** the seed volume `default.brazilian_ecommerce_raw_data` unless you explicitly enable it.
# MAGIC
# MAGIC ## How to use
# MAGIC 1. Run **Config** and **Inventory** first.
# MAGIC 2. Leave `dry_run = true` and run **Execute cleanup** — review the plan.
# MAGIC 3. Set `dry_run = false`, type the confirmation phrase, then run **Execute cleanup** again.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "mypractice")
dbutils.widgets.text("bronze_schema", "brazilian_ecommerce_bronze")
dbutils.widgets.text("silver_schema", "brazilian_ecommerce_silver")
dbutils.widgets.text("gold_schema", "brazilian_ecommerce_gold")
dbutils.widgets.text("bronze_volume", "raw_data")

dbutils.widgets.dropdown("drop_gold", "true", ["true", "false"], "Drop gold schema")
dbutils.widgets.dropdown("drop_silver", "true", ["true", "false"], "Drop silver schema")
dbutils.widgets.dropdown("drop_bronze", "true", ["true", "false"], "Drop bronze schema + volume")
dbutils.widgets.dropdown(
    "drop_seed_volume",
    "false",
    ["false", "true"],
    "Drop seed volume (default.brazilian_ecommerce_raw_data)",
)
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (preview only)")
dbutils.widgets.text("confirm_phrase", "", "Type DROP BRAZILIAN ECOMMERCE to execute")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")
BRONZE_VOLUME = dbutils.widgets.get("bronze_volume")

DROP_GOLD = dbutils.widgets.get("drop_gold") == "true"
DROP_SILVER = dbutils.widgets.get("drop_silver") == "true"
DROP_BRONZE = dbutils.widgets.get("drop_bronze") == "true"
DROP_SEED_VOLUME = dbutils.widgets.get("drop_seed_volume") == "true"
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
CONFIRM_PHRASE = dbutils.widgets.get("confirm_phrase").strip()

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD = f"{CATALOG}.{GOLD_SCHEMA}"
SEED_VOLUME = f"{CATALOG}.default.brazilian_ecommerce_raw_data"
BRONZE_VOLUME_FQN = f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_VOLUME}"

REQUIRED_CONFIRMATION = "DROP BRAZILIAN ECOMMERCE"

print(f"Catalog        : {CATALOG}")
print(f"Bronze         : {BRONZE}  (drop={DROP_BRONZE})")
print(f"Silver         : {SILVER}  (drop={DROP_SILVER})")
print(f"Gold           : {GOLD}  (drop={DROP_GOLD})")
print(f"Bronze volume  : {BRONZE_VOLUME_FQN}")
print(f"Seed volume    : {SEED_VOLUME}  (drop={DROP_SEED_VOLUME})")
print(f"Dry run        : {DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory (what exists today)

# COMMAND ----------

def schema_exists(schema_fqn: str) -> bool:
    catalog, schema = schema_fqn.split(".", 1)
    rows = spark.sql(
        f"""
        SELECT 1
        FROM {catalog}.information_schema.schemata
        WHERE schema_name = '{schema}'
        """
    ).collect()
    return len(rows) > 0


def list_tables(schema_fqn: str) -> list[str]:
    if not schema_exists(schema_fqn):
        return []
    return [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {schema_fqn}").collect()
        if not row.isTemporary and not row.tableName.startswith("_")
    ]


def list_volumes(schema_fqn: str) -> list[str]:
    if not schema_exists(schema_fqn):
        return []
    try:
        return [
            row.volume_name
            for row in spark.sql(f"SHOW VOLUMES IN {schema_fqn}").collect()
        ]
    except Exception as exc:
        print(f"Could not list volumes in {schema_fqn}: {exc}")
        return []


def print_inventory(label: str, schema_fqn: str) -> None:
    print(f"\n=== {label}: {schema_fqn} ===")
    if not schema_exists(schema_fqn):
        print("  (schema does not exist)")
        return

    tables = list_tables(schema_fqn)
    if tables:
        for table_name in sorted(tables):
            try:
                row_count = spark.table(f"{schema_fqn}.{table_name}").count()
                print(f"  table  {table_name:45s} {row_count:>12,} rows")
            except Exception as exc:
                print(f"  table  {table_name:45s}  (count failed: {exc})")
    else:
        print("  (no tables)")

    volumes = list_volumes(schema_fqn)
    if volumes:
        for volume_name in sorted(volumes):
            print(f"  volume {volume_name}")


print_inventory("Gold", GOLD)
print_inventory("Silver", SILVER)
print_inventory("Bronze", BRONZE)

if DROP_SEED_VOLUME:
    print(f"\n=== Seed volume (optional): {SEED_VOLUME} ===")
    try:
        seed_files = dbutils.fs.ls(f"/Volumes/{CATALOG}/default/brazilian_ecommerce_raw_data")
        print(f"  files in seed volume: {len(seed_files)}")
    except Exception as exc:
        print(f"  seed volume not found or not readable: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute cleanup
# MAGIC
# MAGIC Drops in order: **gold → silver → bronze** (child layers first).

# COMMAND ----------

def run_sql(statement: str) -> None:
    print(statement)
    if not DRY_RUN:
        spark.sql(statement)


def drop_schema_objects(schema_fqn: str, label: str, enabled: bool) -> None:
    if not enabled:
        print(f"\nSkipping {label} ({schema_fqn})")
        return

    print(f"\n--- {label}: {schema_fqn} ---")
    if not schema_exists(schema_fqn):
        print("  schema does not exist; nothing to drop")
        return

    for table_name in sorted(list_tables(schema_fqn)):
        run_sql(f"DROP TABLE IF EXISTS {schema_fqn}.{table_name}")

    for volume_name in sorted(list_volumes(schema_fqn)):
        run_sql(f"DROP VOLUME IF EXISTS {schema_fqn}.{volume_name}")

    run_sql(f"DROP SCHEMA IF EXISTS {schema_fqn} CASCADE")


if not DRY_RUN and CONFIRM_PHRASE != REQUIRED_CONFIRMATION:
    raise ValueError(
        f"Confirmation phrase mismatch. Set confirm_phrase widget to exactly: {REQUIRED_CONFIRMATION}"
    )

if not DROP_GOLD and not DROP_SILVER and not DROP_BRONZE and not DROP_SEED_VOLUME:
    raise ValueError("Nothing selected to drop. Enable at least one drop_* widget.")

print("=" * 72)
print("CLEANUP PLAN" if DRY_RUN else "EXECUTING CLEANUP")
print("=" * 72)

drop_schema_objects(GOLD, "Gold", DROP_GOLD)
drop_schema_objects(SILVER, "Silver", DROP_SILVER)
drop_schema_objects(BRONZE, "Bronze", DROP_BRONZE)

if DROP_SEED_VOLUME:
    print(f"\n--- Seed volume: {SEED_VOLUME} ---")
    run_sql(f"DROP VOLUME IF EXISTS {SEED_VOLUME}")

print("\nDone." if not DRY_RUN else "\nDry run complete — no objects were dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify (after real cleanup)

# COMMAND ----------

for label, schema_fqn in [
    ("Gold", GOLD),
    ("Silver", SILVER),
    ("Bronze", BRONZE),
]:
    exists = schema_exists(schema_fqn)
    table_count = len(list_tables(schema_fqn)) if exists else 0
    volume_count = len(list_volumes(schema_fqn)) if exists else 0
    print(
        f"{label:6s} {schema_fqn:45s}  "
        f"schema_exists={exists}  tables={table_count}  volumes={volume_count}"
    )
