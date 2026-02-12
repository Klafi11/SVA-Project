from __future__ import annotations
from pyspark.sql import SparkSession


def build_spark(
    app_name: str,
    shuffle_partitions: int = 200,
    aqe_enabled: bool = True,
) -> SparkSession:
    """Builds and configures a Spark session.

    Args:
        app_name (str): Name of the Spark application
        shuffle_partitions (int, optional): Number of shuffle partitions. Defaults to 200.
        aqe_enabled (bool, optional): Whether Adaptive Query Execution is enabled. Defaults to True.

    Returns:
        SparkSession: Configured Spark session
    """
    # Initialize Spark session builder
    b = SparkSession.builder.appName(app_name)

    # Configure Spark settings
    b = b.config("spark.sql.shuffle.partitions", shuffle_partitions)
    b = b.config("spark.sql.adaptive.enabled", str(aqe_enabled).lower())

    if aqe_enabled:
        b = b.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        b = b.config("spark.sql.adaptive.skewJoin.enabled", "true")

    return b.getOrCreate()
