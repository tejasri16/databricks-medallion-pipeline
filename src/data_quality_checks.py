"""
Reusable Data Quality framework for Delta tables.

Checks implemented:
1. Null checks on required columns
2. Duplicate detection on a natural key
3. Referential integrity between a child table and a parent table
4. Row-count reconciliation between two layers (e.g. Bronze -> Silver), within a tolerance

Each check returns a DQResult; `run_all` prints a summary and raises if any check marked
`critical=True` fails, so this can be wired into a Databricks job as a hard gate before Gold runs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List

import yaml

from utils.spark_session import get_spark_session

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")


@dataclass
class DQResult:
    check_name: str
    passed: bool
    details: str
    critical: bool = True


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def check_nulls(df, columns: List[str]) -> DQResult:
    bad_counts = {c: df.filter(df[c].isNull()).count() for c in columns}
    offenders = {c: n for c, n in bad_counts.items() if n > 0}
    passed = len(offenders) == 0
    details = "no nulls found" if passed else f"nulls found: {offenders}"
    return DQResult("null_check", passed, details)


def check_duplicates(df, key_column: str) -> DQResult:
    total = df.count()
    distinct = df.select(key_column).distinct().count()
    passed = total == distinct
    details = f"total={total}, distinct_keys={distinct}" if not passed else "no duplicate keys"
    return DQResult("duplicate_check", passed, details)


def check_referential_integrity(child_df, parent_df, child_key: str, parent_key: str) -> DQResult:
    orphans = child_df.join(
        parent_df.select(parent_key).distinct(),
        child_df[child_key] == parent_df[parent_key],
        "left_anti",
    ).count()
    passed = orphans == 0
    details = "all keys resolve to parent" if passed else f"{orphans} orphan rows found"
    return DQResult("referential_integrity", passed, details)


def check_row_count_reconciliation(source_df, target_df, tolerance_pct: float = 0.0) -> DQResult:
    source_count = source_df.count()
    target_count = target_df.count()
    if source_count == 0:
        passed = target_count == 0
    else:
        diff_pct = abs(source_count - target_count) / source_count * 100
        passed = diff_pct <= tolerance_pct
    details = f"source={source_count}, target={target_count}"
    return DQResult("row_count_reconciliation", passed, details, critical=False)


def run_all() -> List[DQResult]:
    cfg = load_config()
    spark = get_spark_session("data-quality-checks")

    bronze_df = spark.read.format("delta").load(cfg["paths"]["bronze"])
    silver_df = spark.read.format("delta").load(cfg["paths"]["silver"])

    results: List[DQResult] = [
        check_nulls(silver_df, cfg["data_quality"]["null_check_columns"]),
        check_duplicates(silver_df, cfg["data_quality"]["duplicate_check_key"]),
        check_row_count_reconciliation(
            bronze_df.dropna(subset=cfg["data_quality"]["null_check_columns"]).dropDuplicates(
                [cfg["data_quality"]["duplicate_check_key"]]
            ),
            silver_df,
            tolerance_pct=cfg["data_quality"]["row_count_tolerance_pct"],
        ),
    ]

    print("\n[data_quality_checks] Summary")
    print("-" * 60)
    critical_failures = []
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"{status:5s} | {r.check_name:28s} | {r.details}")
        if not r.passed and r.critical:
            critical_failures.append(r)
    print("-" * 60)

    if critical_failures:
        names = ", ".join(r.check_name for r in critical_failures)
        raise SystemExit(f"[data_quality_checks] CRITICAL DQ FAILURE: {names}")

    return results


if __name__ == "__main__":
    run_all()
