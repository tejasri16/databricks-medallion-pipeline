"""
One-time Unity Catalog setup: creates the catalog, schema, and grants used by this pipeline.

Run this inside a Databricks notebook/job attached to a Unity-Catalog-enabled workspace.
It is a no-op (with a clear message) when run locally, since Unity Catalog is a Databricks
workspace feature and isn't available in local/OSS Spark.
"""

from __future__ import annotations

import os

import yaml

from utils.spark_session import get_spark_session

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def run() -> None:
    cfg = load_config()
    spark = get_spark_session("unity-catalog-setup")

    catalog = cfg["catalog"]
    schema = cfg["schema"]

    statements = [
        f"CREATE CATALOG IF NOT EXISTS {catalog}",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}",
        # Example grants — adjust group names to your workspace's SCIM-provisioned groups.
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `data-engineers`",
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `data-engineers`",
        f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `analysts`",
    ]

    for stmt in statements:
        try:
            spark.sql(stmt)
            print(f"[unity_catalog_setup] OK: {stmt}")
        except Exception as e:
            # Expected when run outside a Unity-Catalog-enabled Databricks workspace.
            print(f"[unity_catalog_setup] skipped (requires Databricks + Unity Catalog): {stmt} -> {e}")


if __name__ == "__main__":
    run()
