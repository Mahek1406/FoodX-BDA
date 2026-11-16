"""Spark-session construction shared by notebooks and scripts."""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "FoodX BDA Analytics") -> SparkSession:
    """Create a local Spark session suitable for running FoodX on Windows."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
