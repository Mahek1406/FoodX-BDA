"""Partitioning, CSV/Parquet storage, and measured comparisons."""

import os
from pathlib import Path
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def storage_output_prerequisite_error() -> str | None:
    """Explain why Spark file output cannot run when Windows Hadoop is absent."""
    if os.name != "nt":
        return None

    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if not hadoop_home:
        return (
            "Windows Spark file output requires HADOOP_HOME and "
            "bin\\winutils.exe."
        )

    winutils = Path(hadoop_home) / "bin" / "winutils.exe"
    if not winutils.is_file():
        return f"Windows Spark file output requires winutils.exe at {winutils}."

    return None


def _measure_count(frame: DataFrame, strategy: str) -> tuple[str, int, int, float]:
    """Materialize one partition strategy and return its measured statistics."""
    partition_count = frame.rdd.getNumPartitions()
    started = perf_counter()
    record_count = frame.count()
    elapsed_seconds = perf_counter() - started
    return strategy, partition_count, record_count, elapsed_seconds


def benchmark_partition_strategies(
    spark: SparkSession,
    orders: DataFrame,
    target_partitions: int = 4,
) -> DataFrame:
    """Measure baseline, repartition, and coalesce reads on the same DataFrame."""
    if target_partitions < 2:
        raise ValueError("target_partitions must be at least 2.")

    repartitioned = orders.repartition(target_partitions)
    coalesced = repartitioned.coalesce(max(1, target_partitions // 2))
    measurements = [
        _measure_count(orders, "baseline"),
        _measure_count(repartitioned, "repartition"),
        _measure_count(coalesced, "coalesce"),
    ]
    schema = StructType(
        [
            StructField("strategy", StringType(), nullable=False),
            StructField("partitions", IntegerType(), nullable=False),
            StructField("record_count", LongType(), nullable=False),
            StructField("elapsed_seconds", DoubleType(), nullable=False),
        ]
    )
    return spark.createDataFrame(measurements, schema=schema)


def write_csv_and_parquet(
    orders: DataFrame,
    csv_path: Path,
    parquet_path: Path,
) -> None:
    """Write the prepared data to Spark CSV and Parquet output directories."""
    prerequisite_error = storage_output_prerequisite_error()
    if prerequisite_error:
        raise EnvironmentError(prerequisite_error)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    (
        orders.write
        .mode("overwrite")
        .option("header", True)
        .csv(str(csv_path))
    )
    orders.write.mode("overwrite").parquet(str(parquet_path))


def _directory_size(path: Path) -> int:
    """Return the size of all files created under a Spark output directory."""
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def benchmark_storage_formats(
    spark: SparkSession,
    orders: DataFrame,
    csv_path: Path,
    parquet_path: Path,
) -> DataFrame:
    """Write, read, and measure CSV and Parquet using actual Spark operations."""
    write_csv_and_parquet(orders, csv_path, parquet_path)
    measurements = []
    for format_name, path in (("csv", csv_path), ("parquet", parquet_path)):
        started = perf_counter()
        if format_name == "csv":
            loaded = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(str(path))
            )
        else:
            loaded = spark.read.parquet(str(path))
        record_count = loaded.count()
        measurements.append(
            (
                format_name,
                record_count,
                _directory_size(path),
                perf_counter() - started,
            )
        )

    schema = StructType(
        [
            StructField("format", StringType(), nullable=False),
            StructField("record_count", LongType(), nullable=False),
            StructField("storage_bytes", LongType(), nullable=False),
            StructField("read_elapsed_seconds", DoubleType(), nullable=False),
        ]
    )
    return spark.createDataFrame(measurements, schema=schema)
