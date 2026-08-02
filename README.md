# databricks-medallion-pipeline

# Databricks Medallion Pipeline — Delta Lake, Unity Catalog & Automated Data Quality

An end-to-end data engineering pipeline built on **Databricks** that ingests raw multi-source data,
processes it through the **Medallion Architecture** (Bronze → Silver → Gold), and publishes
governed, analytics-ready Delta tables — with automated data quality checks, schema evolution,
time-travel, and Unity Catalog governance baked in.

This project demonstrates a production-style pattern for building reliable, auditable ETL
pipelines on the Databricks Lakehouse, similar to pipelines used in enterprise analytics and
regulated-data (e.g., healthcare/pharma) environments.

---

## Architecture

```
                 ┌─────────────────────────────────────────────────────────┐
                 │                     DATABRICKS LAKEHOUSE                  │
                 │                                                           │
  Raw Sources    │   BRONZE            SILVER              GOLD             │
 ┌───────────┐   │  ┌─────────┐      ┌──────────┐      ┌──────────────┐     │
 │  CSV/JSON │───┼─▶│ Raw      │─────▶│ Cleansed │─────▶│ Business-     │     │
 │  REST API │   │  │ ingestion│      │ & deduped│      │ ready marts   │     │
 │  Files    │   │  │ (as-is)  │      │ (typed,  │      │ (aggregated,  │     │
 └───────────┘   │  └─────────┘      │  validated)│    │  KPI-ready)    │     │
                 │       │           └──────────┘      └──────────────┘     │
                 │       │                 │                    │           │
                 │       ▼                 ▼                    ▼           │
                 │  Delta Lake tables (schema evolution + time travel)      │
                 │       │                 │                    │           │
                 │       ▼                 ▼                    ▼           │
                 │  Data Quality Framework (null / duplicate / referential  │
                 │  integrity / row-count reconciliation checks + alerts)   │
                 │                                                           │
                 │  Unity Catalog: governance, access control, lineage      │
                 └─────────────────────────────────────────────────────────┘
```

## What this project shows

| Area | Implementation |
|---|---|
| **Ingestion** | PySpark autoloader-style batch ingestion of CSV/JSON into Bronze Delta tables, preserving raw data as-is |
| **Transformation** | Silver layer cleansing — type casting, deduplication, null handling, standardization |
| **Aggregation** | Gold layer builds business-facing marts (revenue, region, product KPIs) |
| **Delta Lake** | Schema evolution (`mergeSchema`), time-travel queries, `OPTIMIZE` + `ZORDER`, partitioning |
| **Data Quality** | Reusable PySpark data quality framework — null checks, duplicate detection, referential integrity, row-count reconciliation between layers |
| **Governance** | Unity Catalog setup script for catalog/schema structure, table grants, and lineage-friendly naming |
| **Orchestration** | Databricks Workflows job config (`config/databricks_job.yaml`) to schedule Bronze → Silver → Gold as a DAG with retries |
| **Testing** | `pytest` unit tests for the data quality framework using local Spark session |

## Repo structure

```
databricks-medallion-pipeline/
├── README.md                      ← you are here
├── requirements.txt
├── LICENSE
├── config/
│   ├── pipeline_config.yaml        # table paths, schema, thresholds
│   └── databricks_job.yaml         # Databricks Workflows job definition
├── src/
│   ├── bronze_ingestion.py         # Raw ingestion into Bronze Delta tables
│   ├── silver_transformation.py    # Cleansing, dedup, standardization
│   ├── gold_aggregation.py         # Business KPI marts
│   ├── data_quality_checks.py      # Reusable DQ framework
│   ├── unity_catalog_setup.py      # Catalog/schema/grants setup
│   └── utils/
│       └── spark_session.py        # Spark/Delta session factory (local + Databricks)
├── notebooks/
│   └── 01_pipeline_walkthrough.py  # Databricks-notebook-format walkthrough (source format, importable directly)
├── tests/
│   └── test_data_quality.py        # pytest unit tests for DQ framework
└── docs/
    └── sample_run_output.md        # example console output from a local run
```

## How to run this locally (to verify before pushing to Databricks)

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Run each layer in order
python src/bronze_ingestion.py
python src/silver_transformation.py
python src/gold_aggregation.py
python src/data_quality_checks.py
```

Sample source data is generated automatically on first run under `./data/raw/` so the pipeline
is runnable end-to-end with no external setup.

## How to run this on Databricks

1. Create a Databricks workspace + a Unity-Catalog-enabled metastore (or use an existing one).
2. Import this repo via **Repos → Add Repo** (Databricks Repos supports Git folders directly).
3. Run `src/unity_catalog_setup.py` once in a notebook/job to create the catalog and schemas.
4. Import `config/databricks_job.yaml` under **Workflows → Jobs → Create Job from YAML**, or
   recreate the 3-task DAG (bronze → silver → gold) manually, pointing each task at the
   corresponding script.
5. Attach a cluster (single-node is enough for this dataset size) and run the job.

## What to do with this before adding it to your resume / GitHub

1. `git init`, commit this folder, and push it to a new **public** GitHub repo
   (e.g. `databricks-medallion-pipeline`).
2. Update the `LICENSE` author line and the contact details in this README if you want your name on it.
3. (Optional) Replace the synthetic sample data generator in `bronze_ingestion.py` with a real
   public dataset (e.g., a Kaggle retail/sales dataset) so the numbers in `docs/sample_run_output.md`
   reflect a real run.
4. (Optional) Take 2–3 screenshots of the Databricks job DAG succeeding and the Gold table
   results, and add them to `docs/` — this is the single most convincing thing to link from your resume.
5. Add the GitHub URL to your resume under this project's title.

---
*Built as a portfolio project to demonstrate Databricks Lakehouse / Medallion Architecture patterns.*
