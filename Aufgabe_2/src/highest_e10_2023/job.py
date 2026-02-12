from __future__ import annotations
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
from pyspark.storagelevel import StorageLevel
import logging

log = logging.getLogger(__name__)

DEFAULT_SCHEMA = StructType(
    [
        StructField("date", TimestampType(), True),
        StructField("station_uuid", StringType(), True),
        StructField("diesel", DoubleType(), True),
        StructField("e5", DoubleType(), True),
        StructField("e10", DoubleType(), True),
        StructField("diesel_change", IntegerType(), True),
        StructField("e5_change", IntegerType(), True),
        StructField("e10_change", IntegerType(), True),
    ]
)


@dataclass(frozen=True)
class JobResult:
    # Overall max N distinct e10 prices
    max_prices: list[float]
    max_records: DataFrame

    # Max under certain threshold (e.g. 3 Euro)
    max_prices_under_threshold: list[float]
    max_records_under_threshold: DataFrame


def read_price_csv(
    spark: SparkSession,
    input_path: str,
    schema: StructType = DEFAULT_SCHEMA,
    header: bool = True,
) -> DataFrame:
    """Reads CSV files containing price data.

    Args:
        spark (SparkSession): Spark session
        input_path (str): Input path to CSV files
        schema (StructType, optional): Defines the schema of the CSV files. Defaults to DEFAULT_SCHEMA.
        header (bool, optional): Defines whether the CSV files have a header row. Defaults to True.

    Returns:
        DataFrame: DataFrame containing the price data
    """
    return (
        spark.read.schema(schema)
        .option("header", "true" if header else "false")
        .option("inferSchema", "false")
        .csv(input_path)
        .select(F.col("date"), F.col("station_uuid"), F.col("e10"))
    )


def compute_top_price_records(
    base: DataFrame,
    price_col: str,
    top_n: int = 3,
    threshold: float | None = None,
) -> tuple[list[float], DataFrame]:
    """Return top N price values and all records with those prices, optionally filtering by a price threshold.

    Args:
        base (DataFrame): Base DataFrame to compute on
        price_col (str): Name of the column containing the price to analyze
        top_n (int, optional): Top N distinct prices to return. Defaults to 3.
        threshold (float | None, optional): Price threshold to filter out prices above this value. Defaults to None.

    Returns:
        tuple[list[float], DataFrame]: List of top N price values and DataFrame of all records with those prices
    """
    # Start with the base DataFrame and apply threshold filtering if specified
    df = base
    if threshold is not None:
        df = df.where(F.col(price_col) < F.lit(threshold))

    # Get top N distinct prices
    top_prices_df = (
        df.select(price_col).distinct().orderBy(F.col(price_col).desc()).limit(top_n)
    )

    # Collect the top N prices to the driver
    rows = top_prices_df.take(top_n)

    # return empty results if no valid prices found after threshold filtering
    if not rows:
        return [], df.limit(0)

    prices = [float(r[price_col]) for r in rows]

    # Get all records that have one of the top N prices
    records = df.join(F.broadcast(top_prices_df), on=price_col, how="inner").withColumn(
        "year_month", F.date_format(F.col("date"), "yyyy-MM")
    )
    return prices, records


def compute_max_e10_records(
    df: DataFrame, top_n: int, threshold: float | None = None
) -> JobResult:
    """Compute max e10 price and return all rows tied for max.

    Args:
        df (DataFrame): Raw Data -> DataFrame
        top_n (int): Number of top distinct E10 prices to find
        threshold (float | None, optional): Price threshold to filter out prices above this value. Defaults to None.
    Returns:
        JobResult: Result of the Spark Computation -> Return max price and all records with that price
    """
    # Select only relevant columns and filter out invalid e10 prices (nulls and <= 0)
    filtered = (
        df.select(
            F.col("date"),
            F.col("station_uuid"),
            F.col("e10").alias("e10_price"),
        )
        .where(F.col("e10_price").isNotNull() & (F.col("e10_price") > 0))
        .persist(StorageLevel.MEMORY_AND_DISK)  # Can also be .cache()
    )

    try:
        # Compute overall max prices
        max_prices, max_records = compute_top_price_records(
            filtered, price_col="e10_price", top_n=top_n, threshold=None
        )
        # If no valid prices found, raise error
        if not max_prices:
            filtered.unpersist()
            raise ValueError(
                "No valid e10 prices found after filtering (nulls and <= 0)."
            )
        # Compute max prices under threshold
        max_prices_under_threshold, max_records_under_threshold = (
            compute_top_price_records(
                filtered, price_col="e10_price", top_n=top_n, threshold=threshold
            )
        )

        return JobResult(
            max_prices=max_prices,
            max_records=max_records,
            max_prices_under_threshold=max_prices_under_threshold,
            max_records_under_threshold=max_records_under_threshold,
        )

    finally:
        filtered.unpersist()


def write_results(df_max: DataFrame, df_under: DataFrame, output_path: str) -> None:
    """Return Results to CSV file or partitioned files by month.

    Args:
        df_max (DataFrame): dataframe containing the max results
        df_under (DataFrame): dataframe containing the results under threshold
        output_path (str): Output path for the results
    """
    (
        df_max.coalesce(
            1
        )  # Coalesce to 1 partition to write a single CSV file, can be removed to write multiple files because of heavy bottleneck
        .write.mode("overwrite")
        .option("header", "true")
        .csv(output_path + "max")
    )
    (
        df_under.coalesce(
            1
        )  # Coalesce to 1 partition to write a single CSV file, can be removed to write multiple files because of heavy bottleneck
        .write.mode("overwrite")
        .option("header", "true")
        .csv(output_path + "under_threshold")
    )
