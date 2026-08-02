# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Pipeline Walkthrough
# MAGIC This notebook is in Databricks' "notebook source" format — importing it via
# MAGIC **Repos → Add Repo** will open it as a normal Databricks notebook with proper cells.
# MAGIC It runs the same Bronze → Silver → Gold pipeline as the `src/` scripts, but interactively,
# MAGIC so each step's output can be inspected in the notebook UI.

# COMMAND ----------

import sys
sys.path.append("../src")

from bronze_ingestion import run as run_bronze
from silver_transformation import run as run_silver
from gold_aggregation import run as run_gold
from data_quality_checks import run_all as run_dq

# COMMAND ----------

# MAGIC %md ## Step 1 — Bronze: raw ingestion

# COMMAND ----------

run_bronze()

# COMMAND ----------

# MAGIC %md ## Step 2 — Silver: cleanse, dedupe, standardize

# COMMAND ----------

run_silver()

# COMMAND ----------

# MAGIC %md ## Step 3 — Data Quality gate

# COMMAND ----------

run_dq()

# COMMAND ----------

# MAGIC %md ## Step 4 — Gold: business KPI marts

# COMMAND ----------

run_gold()

# COMMAND ----------

# MAGIC %md ## Inspect results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If registered as tables in Unity Catalog, you could instead run:
# MAGIC -- SELECT * FROM main.sales_analytics.gold_revenue_by_region ORDER BY total_revenue DESC
