from __future__ import annotations
import argparse
from highest_e10_2023.job import (
    compute_max_e10_records,
    read_price_csv,
    write_results,
)
from highest_e10_2023.spark import build_spark
import logging
from highest_e10_2023.logging import setup_logging

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=" Find the highest E10 price in 2023 and all the ties"
    )
    p.add_argument(
        "--input-path",
        type=str,
        required=True,
        help="Input path to CSV files containing E10 price data",
    )
    p.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Output path to write the results",
    )

    p.add_argument(
        "--threshold-max-e10",
        type=float,
        default=3.0,
        help="Threshold for maximum E10 price",
    )
    p.add_argument(
        "--top-n",
        type=int,
        default=3,
        help="Number of top distinct E10 prices to find",
    )
    p.add_argument(
        "--shuffle-partitions",
        type=int,
        default=200,
        help="Number of shuffle partitions",
    )
    p.add_argument(
        "--disable-aqe",
        action="store_true",
        help="Disable Adaptive Query Execution (AQE)",
    )

    return p.parse_args()


def main() -> None:
    """Main entry point for the highest E10 price job."""
    setup_logging()

    log.info("Starting highest E10 price job")

    args = parse_args()

    spark = build_spark(
        app_name="Highest E10 Price 2023",
        shuffle_partitions=args.shuffle_partitions,
        aqe_enabled=not args.disable_aqe,
    )

    log.info("Spark session created")

    try:
        df = read_price_csv(spark, args.input_path)
        log.info("Input data read successfully")

        res = compute_max_e10_records(
            df, top_n=args.top_n, threshold=args.threshold_max_e10
        )
        log.info(f"Computed max E10 prices: {res.max_prices}")
        log.info(
            f"Computed max E10 prices under threshold: {res.max_prices_under_threshold}"
        )

        write_results(
            df_max=res.max_records,
            df_under=res.max_records_under_threshold,
            output_path=args.output_path,
        )
    finally:
        spark.stop()
