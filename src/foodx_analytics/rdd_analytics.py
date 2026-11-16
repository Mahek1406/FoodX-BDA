"""RDD-based MapReduce analytics for the FoodX order history."""

from dataclasses import dataclass
from operator import add
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


@dataclass(frozen=True)
class RddAnalyticsResult:
    """RDD outputs and action metrics produced from one input DataFrame."""

    order_status_counts: DataFrame
    customer_spend: DataFrame
    input_record_count: int
    distinct_customer_count: int


def _status_pairs(orders: DataFrame) -> Any:
    """Create key-value pairs used by the order-status MapReduce operation."""
    return (
        orders.select("Order Status")
        .rdd
        .map(lambda row: (row["Order Status"] or "Unknown", 1))
        .filter(lambda pair: pair[0] != "Unknown")
    )


def _customer_spend_pairs(orders: DataFrame) -> Any:
    """Create customer and spend pairs while ignoring unusable numeric values."""
    return (
        orders.select("Customer ID", "Total")
        .rdd
        .map(
            lambda row: (
                row["Customer ID"],
                float(row["Total"]) if row["Total"] is not None else None,
            )
        )
        .filter(lambda pair: pair[0] is not None and pair[1] is not None)
    )


def mapreduce_order_status_counts(
    spark: SparkSession,
    orders: DataFrame,
) -> DataFrame:
    """Count order statuses with RDD map, filter, reduceByKey, and sortByKey."""
    status_counts = (
        _status_pairs(orders)
        .reduceByKey(add)
        .sortByKey()
        .collect()
    )
    schema = StructType(
        [
            StructField("Order Status", StringType(), nullable=False),
            StructField("total_orders", LongType(), nullable=False),
        ]
    )
    return spark.createDataFrame(status_counts, schema=schema)


def mapreduce_customer_spend(
    spark: SparkSession,
    orders: DataFrame,
) -> DataFrame:
    """Aggregate customer spending with an RDD reduceByKey action."""
    customer_spend = (
        _customer_spend_pairs(orders)
        .reduceByKey(add)
        .mapValues(lambda total: round(total, 2))
        .sortBy(lambda pair: (-pair[1], str(pair[0])))
        .collect()
    )
    schema = StructType(
        [
            StructField("Customer ID", StringType(), nullable=False),
            StructField("total_spending", DoubleType(), nullable=False),
        ]
    )
    return spark.createDataFrame(customer_spend, schema=schema)


def run_rdd_analytics(
    spark: SparkSession,
    orders: DataFrame,
) -> RddAnalyticsResult:
    """Run the phase-two RDD transformations and actions."""
    customer_ids = (
        orders.select("Customer ID")
        .rdd
        .map(lambda row: row["Customer ID"])
        .filter(lambda customer_id: customer_id is not None)
        .distinct()
    )
    return RddAnalyticsResult(
        order_status_counts=mapreduce_order_status_counts(spark, orders),
        customer_spend=mapreduce_customer_spend(spark, orders),
        input_record_count=orders.rdd.count(),
        distinct_customer_count=customer_ids.count(),
    )
