"""Filesystem helpers for creating FoodX's generated-data directories."""

from pathlib import Path

from .config import (
    BENCHMARKS_DIR,
    PROCESSED_CSV_DIR,
    PROCESSED_PARQUET_DIR,
    RESULTS_DIR,
)


def ensure_project_data_directories() -> tuple[Path, ...]:
    """Create directories reserved for generated outputs and benchmarks."""
    directories = (
        RESULTS_DIR,
        PROCESSED_CSV_DIR,
        PROCESSED_PARQUET_DIR,
        BENCHMARKS_DIR,
    )
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    return directories
