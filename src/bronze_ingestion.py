"""
Bronze layer: ingest raw source data as-is into a Delta table.

Responsibilities:
- Read raw CSV (or, on Databricks, an Auto Loader stream from a Volume/S3/ADLS path).
- Generate a small synthetic sample dataset on first run so this repo is runnable with
  zero external setup — swap this out for a real source when you adapt the project.
- Write to the Bronze Delta table with `mergeSchema` enabled, preserving raw fidelity
  and adding an ingestion timestamp + source file column for lineage.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta

import yaml
from pyspark.sql.functions import current_timestamp, lit

from utils.spark_session import get_spark_session

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def generate_sample_source(path: str, n_rows: int = 5000) -> None:
    """Create a synthetic sales CSV so the pipeline runs end-to-end with no external data."""
    if os.path.exists(path):
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)
    random.seed(42)
    regions = ["North", "South", "East", "West"]
    products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Gizmo Z"]
    start_date = datetime(2024, 1, 1)

    with open(path, "w") as f:
        f.write("order_id,customer_id,order_date,region,product,amount\n")
        for i in range(1, n_rows + 1):
            # Deliberately inject some dirty data so the DQ layer has something to catch.
            order_id = i if random.random() > 0.002 else ""  # occasional missing id
            customer_id = f"CUST{random.randint(1, 800)}"
            order_date = (start_date + timedelta(days=random.randint(0, 540))).date()
            region = random.choice(regions)
            product = random.choice(products)
            amount = round(random.uniform(10, 2500), 2) if random.random() > 0.001 else ""
            f.write(f"{order_id},{customer_id},{order_date},{region},{product},{amount}\n")
        # Inject a handful of exact duplicate rows to exercise duplicate detection.
        for i in range(1, 15):
            f.write(f"{i},CUST1,{start_date.date()},North,Widget A,99.99\n")


def run() -> None:
    cfg = load_config()
    spark = get_spark_session("bronze-ingestion")

    generate_sample_source(cfg["paths"]["raw_source"])

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(cfg["paths"]["raw_source"])
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(cfg["paths"]["raw_source"]))
    )

    row_count = df.count()

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(cfg["paths"]["bronze"])
    )

    print(f"[bronze_ingestion] wrote {row_count} rows to {cfg['paths']['bronze']}")


if __name__ == "__main__":
    run()
