"""Caching, persistence, and broadcast-join examples for FoodX."""

from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, count, round


@dataclass(frozen=True)
class PersistenceMetrics:
    """Observed action counts from cached and persisted DataFrames."""

    input_records: int
    cached_records: int
    persisted_records: int
    cache_storage_level: str
    persist_storage_level: str


def measure_cache_and_persistence(orders: DataFrame) -> PersistenceMetrics:
    """Materialize one cached and one explicitly persisted DataFrame."""
    cached_orders = orders.select("Order ID", "Total").cache()
    persisted_orders = orders.select(
        "Order ID",
        "Total",
        "Order Status",
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        input_records = orders.count()
        cached_records = cached_orders.count()
        persisted_records = persisted_orders.count()
        return PersistenceMetrics(
            input_records=input_records,
            cached_records=cached_records,
            persisted_records=persisted_records,
            cache_storage_level=str(cached_orders.storageLevel),
            persist_storage_level=str(persisted_orders.storageLevel),
        )
    finally:
        cached_orders.unpersist()
        persisted_orders.unpersist()


def broadcast_restaurant_summary(orders: DataFrame) -> DataFrame:
    """Join orders to a small restaurant dimension using a broadcast join."""
    restaurant_dimension = (
        orders.select("Restaurant ID", "Restaurant name", "City")
        .dropDuplicates(["Restaurant ID"])
        .withColumnRenamed("Restaurant name", "dimension_restaurant_name")
        .withColumnRenamed("City", "dimension_city")
    )
    orders_without_dimension_columns = orders.drop("Restaurant name", "City")
    enriched_orders = orders_without_dimension_columns.join(
        broadcast(restaurant_dimension),
        on="Restaurant ID",
        how="left",
    )
    return (
        enriched_orders.groupBy(
            "Restaurant ID",
            "dimension_restaurant_name",
            "dimension_city",
        )
        .agg(
            count("Order ID").alias("total_orders"),
            round(count("Customer ID"), 0).alias("orders_with_customer_id"),
        )
        .orderBy(col("total_orders").desc())
    )
