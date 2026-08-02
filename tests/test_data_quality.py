"""
Unit tests for the data quality framework. Uses a local Spark session (no Delta table I/O
needed since the check functions operate on in-memory DataFrames).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pyspark.sql import SparkSession

from data_quality_checks import (
    check_duplicates,
    check_nulls,
    check_referential_integrity,
    check_row_count_reconciliation,
)


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[2]").appName("dq-tests").getOrCreate()
    yield session
    session.stop()


def test_check_nulls_passes_when_clean(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    result = check_nulls(df, ["id", "name"])
    assert result.passed


def test_check_nulls_fails_when_dirty(spark):
    df = spark.createDataFrame([(1, "a"), (None, "b")], ["id", "name"])
    result = check_nulls(df, ["id", "name"])
    assert not result.passed


def test_check_duplicates_detects_dupe_keys(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["id", "name"])
    result = check_duplicates(df, "id")
    assert not result.passed


def test_check_duplicates_passes_on_unique_keys(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    result = check_duplicates(df, "id")
    assert result.passed


def test_referential_integrity_detects_orphans(spark):
    child = spark.createDataFrame([(1, 100), (2, 999)], ["order_id", "customer_id"])
    parent = spark.createDataFrame([(100,)], ["customer_id"])
    result = check_referential_integrity(child, parent, "customer_id", "customer_id")
    assert not result.passed


def test_row_count_reconciliation_within_tolerance(spark):
    source = spark.createDataFrame([(i,) for i in range(100)], ["id"])
    target = spark.createDataFrame([(i,) for i in range(100)], ["id"])
    result = check_row_count_reconciliation(source, target, tolerance_pct=0.0)
    assert result.passed
