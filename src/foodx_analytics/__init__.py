"""Reusable analytics building blocks for the FoodX project."""

from .data_loader import load_raw_orders, prepare_orders
from .optimization import (
    broadcast_restaurant_summary,
    measure_cache_and_persistence,
)
from .rdd_analytics import run_rdd_analytics
from .spark_session import create_spark_session
from .storage_partition import (
    benchmark_partition_strategies,
    benchmark_storage_formats,
    storage_output_prerequisite_error,
)
from .window_analytics import customer_order_windows, restaurant_window_rankings

__all__ = [
    "benchmark_partition_strategies",
    "benchmark_storage_formats",
    "broadcast_restaurant_summary",
    "create_spark_session",
    "customer_order_windows",
    "load_raw_orders",
    "measure_cache_and_persistence",
    "prepare_orders",
    "restaurant_window_rankings",
    "run_rdd_analytics",
    "storage_output_prerequisite_error",
]
