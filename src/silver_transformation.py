"""
Silver layer: cleanse, type, deduplicate, and standardize Bronze data.

- Drops rows that fail hard null checks on key business columns.
- Deduplicates on the natural key.
- Casts columns to proper types.
- Adds a partition column (order_year) and writes partitioned Delta output.
- Uses `mergeSchema` to tolerate upstream schema additions without breaking the job.
"""

from __future__ import annotations

import os

import yaml
from pyspark.sql.functions import col, to_date, year

from utils.spark_session import get_spark_session

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def run() -> None:
    cfg = load_config()
    spark = get_spark_session("silver-transformation")

    bronze_df = spark.read.format("delta").load(cfg["paths"]["bronze"])
    bronze_count = bronze_df.count()

    null_check_cols = cfg["data_quality"]["null_check_columns"]
    dedup_key = cfg["data_quality"]["duplicate_check_key"]

    silver_df = (
        bronze_df.dropna(subset=null_check_cols)
        .dropDuplicates([dedup_key])
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("order_year", year(col("order_date")))
        .withColumn("amount", col("amount").cast("double"))
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "order_year",
            "region",
            "product",
            "amount",
            "_ingested_at",
        )
    )

    silver_count = silver_df.count()

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy(cfg["optimize"]["partition_column_silver"])
        .save(cfg["paths"]["silver"])
    )

    dropped = bronze_count - silver_count
    print(
        f"[silver_transformation] bronze rows={bronze_count}, silver rows={silver_count}, "
        f"dropped by DQ/dedup={dropped}"
    )


if __name__ == "__main__":
    run()
