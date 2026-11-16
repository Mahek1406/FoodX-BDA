"""Spark SQL Window Function analytics for FoodX."""

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    dense_rank,
    desc,
    lag,
    row_number,
    round,
    sum,
)


def restaurant_window_rankings(orders: DataFrame) -> DataFrame:
    """Rank restaurants by order volume and calculate their share of orders."""
    restaurant_totals = (
        orders.groupBy("Restaurant ID", "Restaurant name")
        .agg(
            count("Order ID").alias("total_orders"),
            round(avg("Total"), 2).alias("average_order_value"),
        )
    )
    ranking_window = Window.orderBy(desc("total_orders"), col("Restaurant name"))
    total_orders_window = Window.rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing,
    )
    return (
        restaurant_totals
        .withColumn("restaurant_rank", dense_rank().over(ranking_window))
        .withColumn(
            "all_restaurant_orders",
            sum("total_orders").over(total_orders_window),
        )
        .withColumn(
            "order_share",
            round(col("total_orders") / col("all_restaurant_orders"), 4),
        )
        .drop("all_restaurant_orders")
        .orderBy("restaurant_rank")
    )


def customer_order_windows(orders: DataFrame) -> DataFrame:
    """Add order sequence, previous order value, and running spend per customer."""
    customer_window = (
        Window.partitionBy("Customer ID")
        .orderBy(col("Order Timestamp").asc_nulls_last(), col("Order ID"))
    )
    running_spend_window = (
        customer_window.rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow,
        )
    )
    return (
        orders.filter(
            (col("Order Status") == "Delivered")
            & col("Customer ID").isNotNull()
        )
        .select(
            "Customer ID",
            "Order ID",
            "Order Timestamp",
            "Total",
            "Restaurant name",
        )
        .withColumn("customer_order_number", row_number().over(customer_window))
        .withColumn("previous_order_total", lag("Total").over(customer_window))
        .withColumn(
            "running_customer_spend",
            round(sum("Total").over(running_spend_window), 2),
        )
    )
