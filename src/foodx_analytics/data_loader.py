"""Loading and preparation of the FoodX order-history dataset."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    regexp_extract,
    to_timestamp,
    trim,
    when,
    date_format,
    hour,
)

from .config import RAW_DATA_PATH


REQUIRED_COLUMNS = {
    "Restaurant ID",
    "Restaurant name",
    "Order ID",
    "Order Placed At",
    "Order Status",
    "Distance",
    "Total",
    "Customer ID",
}

NUMERIC_COLUMNS = (
    "Restaurant ID",
    "Order ID",
    "Bill subtotal",
    "Packaging charges",
    "Restaurant discount (Promo)",
    "Restaurant discount (Flat offs, Freebies & others)",
    "Gold discount",
    "Brand pack discount",
    "Total",
    "Rating",
    "Restaurant compensation (Cancellation)",
    "Restaurant penalty (Rejection)",
    "KPT duration (minutes)",
    "Rider wait time (minutes)",
)


def load_raw_orders(
    spark: SparkSession,
    path: Path = RAW_DATA_PATH,
) -> DataFrame:
    """Load the verified raw CSV using the dataset's quoted-field format."""
    if not path.is_file():
        raise FileNotFoundError(f"FoodX raw dataset was not found: {path}")

    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path))
    )

    missing_columns = REQUIRED_COLUMNS.difference(orders.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"FoodX dataset is missing required columns: {missing}")

    return orders


def prepare_orders(orders: DataFrame) -> DataFrame:
    """Apply shared cleaning and derived fields used by later analytics phases."""
    prepared = orders.dropDuplicates()

    for column_name in NUMERIC_COLUMNS:
        if column_name in prepared.columns:
            prepared = prepared.withColumn(
                column_name,
                col(column_name).cast("double"),
            )

    prepared = prepared.withColumn(
        "Order Timestamp",
        to_timestamp(col("Order Placed At"), "hh:mm a, MMMM dd yyyy"),
    )
    prepared = prepared.withColumn("Order Hour", hour(col("Order Timestamp")))
    prepared = prepared.withColumn(
        "Order Day",
        date_format(col("Order Timestamp"), "EEEE"),
    )
    prepared = prepared.withColumn(
        "Distance KM",
        when(
            lower(trim(col("Distance"))) == "<1km",
            0.5,
        ).otherwise(
            regexp_extract(col("Distance"), r"[\d.]+", 0).cast("double"),
        ),
    )

    return prepared
