"""Project paths and shared output names."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "order_history_kaggle_data.csv"
RESULTS_DIR = DATA_DIR / "results"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_CSV_DIR = PROCESSED_DIR / "csv"
PROCESSED_PARQUET_DIR = PROCESSED_DIR / "parquet"
BENCHMARKS_DIR = DATA_DIR / "benchmarks"

RESULT_FILENAMES = {
    "restaurant": "restaurant_analysis.csv",
    "hourly_demand": "hourly_demand.csv",
    "daily_demand": "daily_demand.csv",
    "delivery": "delivery_analysis.csv",
    "segments": "customer_segments.csv",
    "recommendations": "recommendations.csv",
}
