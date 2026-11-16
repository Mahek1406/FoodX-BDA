"""Run the Phase 2 and Phase 3 FoodX extensions on the real dataset."""

import sys
from pathlib import Path

from pyspark.sql import DataFrame

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from foodx_analytics.config import (  # noqa: E402
    BENCHMARKS_DIR,
    PROCESSED_CSV_DIR,
    PROCESSED_PARQUET_DIR,
    RESULTS_DIR,
)
from foodx_analytics.data_loader import load_raw_orders, prepare_orders  # noqa: E402
from foodx_analytics.paths import ensure_project_data_directories  # noqa: E402
from foodx_analytics.rdd_analytics import run_rdd_analytics  # noqa: E402
from foodx_analytics.spark_session import create_spark_session  # noqa: E402
from foodx_analytics.storage_partition import (  # noqa: E402
    benchmark_partition_strategies,
    benchmark_storage_formats,
    storage_output_prerequisite_error,
)


def write_result(frame: DataFrame, path: Path) -> None:
    """Write a small analytical table as one dashboard-friendly CSV file."""
    frame.toPandas().to_csv(path, index=False)


def main() -> None:
    """Execute RDD analytics and measured storage/partition comparisons."""
    ensure_project_data_directories()
    spark = create_spark_session("FoodX Phases 2 and 3")
    try:
        orders = prepare_orders(load_raw_orders(spark))
        rdd_results = run_rdd_analytics(spark, orders)
        write_result(
            rdd_results.order_status_counts,
            RESULTS_DIR / "rdd_order_status_counts.csv",
        )
        write_result(
            rdd_results.customer_spend,
            RESULTS_DIR / "rdd_customer_spend.csv",
        )

        partition_results = benchmark_partition_strategies(spark, orders)
        write_result(
            partition_results,
            BENCHMARKS_DIR / "partition_benchmark.csv",
        )

        storage_error = storage_output_prerequisite_error()
        if storage_error:
            print(f"Storage benchmark skipped: {storage_error}")
        else:
            storage_results = benchmark_storage_formats(
                spark,
                orders,
                PROCESSED_CSV_DIR / "food_orders",
                PROCESSED_PARQUET_DIR / "food_orders",
            )
            write_result(
                storage_results,
                BENCHMARKS_DIR / "storage_benchmark.csv",
            )

        print(
            f"RDD input records: {rdd_results.input_record_count:,}; "
            f"distinct customers: {rdd_results.distinct_customer_count:,}"
        )
        print(f"Benchmark outputs written under {BENCHMARKS_DIR} and {RESULTS_DIR}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
