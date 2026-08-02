"""
Spark/Delta session factory.

Works in two modes:
- Local mode: builds a local SparkSession with Delta Lake support (used for `python src/*.py` runs
  and for pytest).
- Databricks mode: if a SparkSession already exists in scope (as it does inside a Databricks
  notebook/job), that session is reused instead of creating a new one.
"""

from __future__ import annotations

import os


def get_spark_session(app_name: str = "medallion-pipeline"):
    """Return a SparkSession, configured for Delta Lake, that works both locally and on Databricks."""
    try:
        # On Databricks, `spark` is already injected into the notebook/job global scope.
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except Exception:
        pass

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", os.environ.get("SPARK_WAREHOUSE_DIR", "./spark-warehouse"))
    )

    try:
        from delta import configure_spark_with_delta_pip

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        # delta-spark not installed yet — fall back to a plain Spark session so the script
        # still runs (Delta-specific calls will simply fail with a clear error).
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
