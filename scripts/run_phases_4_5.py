"""Run the Phase 4 and Phase 5 FoodX extensions on the real dataset."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from foodx_analytics.config import RESULTS_DIR  # noqa: E402
from foodx_analytics.data_loader import load_raw_orders, prepare_orders  # noqa: E402
from foodx_analytics.optimization import (  # noqa: E402
    broadcast_restaurant_summary,
    measure_cache_and_persistence,
)
from foodx_analytics.paths import ensure_project_data_directories  # noqa: E402
from foodx_analytics.spark_session import create_spark_session  # noqa: E402
from foodx_analytics.window_analytics import (  # noqa: E402
    customer_order_windows,
    restaurant_window_rankings,
)


def write_result(frame, path: Path) -> None:
    """Write a small analytical table as a regular CSV file."""
    frame.toPandas().to_csv(path, index=False)


def main() -> None:
    """Execute Window Function and Spark optimization analytics."""
    ensure_project_data_directories()
    spark = create_spark_session("FoodX Phases 4 and 5")
    try:
        orders = prepare_orders(load_raw_orders(spark))

        rankings = restaurant_window_rankings(orders)
        write_result(rankings, RESULTS_DIR / "window_restaurant_rankings.csv")

        customer_windows = customer_order_windows(orders)
        write_result(customer_windows, RESULTS_DIR / "window_customer_orders.csv")

        persistence = measure_cache_and_persistence(orders)
        broadcast_summary = broadcast_restaurant_summary(orders)
        write_result(
            broadcast_summary,
            RESULTS_DIR / "broadcast_restaurant_summary.csv",
        )

        persistence_frame = spark.createDataFrame(
            [
                (
                    persistence.input_records,
                    persistence.cached_records,
                    persistence.persisted_records,
                    persistence.cache_storage_level,
                    persistence.persist_storage_level,
                )
            ],
            [
                "input_records",
                "cached_records",
                "persisted_records",
                "cache_storage_level",
                "persist_storage_level",
            ],
        )
        write_result(
            persistence_frame,
            RESULTS_DIR / "persistence_metrics.csv",
        )

        print(
            f"Window rows: {customer_windows.count():,}; "
            f"broadcast summary rows: {broadcast_summary.count():,}"
        )
        print(
            f"Cached records: {persistence.cached_records:,}; "
            f"persisted records: {persistence.persisted_records:,}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
