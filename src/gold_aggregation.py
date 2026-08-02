"""
Gold layer: build business-facing KPI marts from Silver data, and apply Delta
performance optimizations (OPTIMIZE + ZORDER) typical of a production Databricks pipeline.
"""

from __future__ import annotations

import os

import yaml
from pyspark.sql.functions import col, count, round as spark_round, sum as spark_sum

from utils.spark_session import get_spark_session

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def run() -> None:
    cfg = load_config()
    spark = get_spark_session("gold-aggregation")

    silver_df = spark.read.format("delta").load(cfg["paths"]["silver"])

    revenue_by_region = silver_df.groupBy("region").agg(
        spark_round(spark_sum("amount"), 2).alias("total_revenue"),
        count("order_id").alias("order_count"),
    )

    revenue_by_product = silver_df.groupBy("product").agg(
        spark_round(spark_sum("amount"), 2).alias("total_revenue"),
        count("order_id").alias("order_count"),
    )

    (
        revenue_by_region.write.format("delta")
        .mode("overwrite")
        .save(cfg["paths"]["gold_revenue_by_region"])
    )
    (
        revenue_by_product.write.format("delta")
        .mode("overwrite")
        .save(cfg["paths"]["gold_revenue_by_product"])
    )

    print("[gold_aggregation] revenue_by_region:")
    revenue_by_region.orderBy(col("total_revenue").desc()).show(truncate=False)

    print("[gold_aggregation] revenue_by_product:")
    revenue_by_product.orderBy(col("total_revenue").desc()).show(truncate=False)

    # On a real Databricks workspace (not local OSS Delta), OPTIMIZE + ZORDER accelerate
    # downstream BI queries. Guarded in a try/except since ZORDER requires the Databricks
    # runtime's optimized Delta engine, not plain OSS Delta Lake used in local runs.
    try:
        spark.sql(f"OPTIMIZE delta.`{cfg['paths']['gold_revenue_by_region']}` ZORDER BY (region)")
    except Exception as e:
        print(f"[gold_aggregation] skipping OPTIMIZE/ZORDER (expected on local OSS Delta): {e}")


if __name__ == "__main__":
    run()
