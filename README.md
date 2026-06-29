# Brazilian E-commerce (Olist)

A medallion data pipeline for the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on Databricks with Unity Catalog.

The project loads 9 CSV files through **bronze → silver → gold** layers, with optional cleanup and a multi-task workflow job.

## Dataset

- **Source:** [Kaggle — Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Publisher:** Olist
- **Files:** 9 CSVs (customers, sellers, orders, order items, payments, reviews, products, geolocation, category translation)

Download from Kaggle and upload the CSVs to a Unity Catalog volume before running the bronze notebook.

## Architecture

```
CSV (volume)
    │
    ▼
Bronze   — raw ingest, explicit schemas, metadata, quarantine where needed
    │
    ▼
Silver   — typed, cleaned, normalized tables
    │
    ▼
Gold     — business KPI tables for analytics
```

### Unity Catalog schemas (default)

| Layer  | Schema                         |
|--------|--------------------------------|
| Bronze | `mypractice.brazilian_ecommerce_bronze` |
| Silver | `mypractice.brazilian_ecommerce_silver` |
| Gold   | `mypractice.brazilian_ecommerce_gold`   |

### Gold tables

| Table | Purpose |
|-------|---------|
| `mv_order_funnel` | Order counts by status |
| `mv_monthly_revenue` | Revenue by month (delivered orders) |
| `mv_revenue_by_category` | Revenue by product category |
| `mv_payment_method_breakdown` | Payment type share |
| `mv_delivery_performance` | Late delivery rate by state |

Gold objects are **managed tables** (not materialized views).

## Repository structure

```
brazilian-ecommerce/
├── databricks/
│   ├── notebooks/
│   │   ├── 00_cleanup_brazilian_schemas.py   # Drop schemas (dry-run guarded)
│   │   ├── bronze_load.py                    # CSV → bronze
│   │   ├── silver_load.py                    # Bronze → silver
│   │   └── gold_load.py                      # Silver → gold
│   ├── resources/
│   │   └── medallion_job.yml                 # Asset bundle job definition
│   └── job_configs/
│       └── medallion_pipeline.job.json       # Job JSON template
├── databricks.yml                            # Databricks Asset Bundle config
└── README.md
```

## Prerequisites

- Databricks workspace with Unity Catalog
- Catalog with write access (default: `mypractice`)
- CSVs uploaded to a seed volume, e.g.  
  `/Volumes/mypractice/default/brazilian_ecommerce_raw_data`
- Git repo connected to Databricks (Repos) for notebook sync

## Running the pipeline

### Notebooks (manual)

Run in order:

1. `bronze_load`
2. `silver_load`
3. `gold_load`

Each notebook uses **widgets** for catalog and schema names (defaults match the table above).

### Workflow job

Create a Databricks job with three tasks:

```
bronze_task → silver_task → gold_task
```

Point each task at the matching notebook in this repo. Pass widget values as **task parameters**. Cleanup is **not** part of the job — run it manually when resetting the environment.

See `databricks/job_configs/medallion_pipeline.job.json` for a starting template.

### Reset (cleanup)

Run `00_cleanup_brazilian_schemas` with:

- `dry_run = true` first (preview)
- `dry_run = false` and confirmation phrase `DROP BRAZILIAN ECOMMERCE` to execute

By default, cleanup does **not** drop the seed volume in `default`.

### Databricks CLI bundle (optional)

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run medallion_pipeline -t dev
```

Edit `databricks.yml` with your workspace host and Repos path before deploying.

## Data quality notes

- **Reviews:** Some CSV rows have column shifting from multiline comments; invalid rows are filtered in silver.
- **Geolocation:** Bronze keeps duplicate zip rows; silver dedupes to one row per zip.
- **Products:** A small number of products have no English category translation; gold uses `COALESCE` fallbacks.
- **Payments:** Rows with `payment_installments = 0` are quarantined at bronze.

## License

The Olist dataset is subject to the terms on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). This repository contains pipeline code only, not the dataset files.
