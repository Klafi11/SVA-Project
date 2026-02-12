import datetime
import pytest
from pyspark.sql import Row
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F

from highest_e10_2023.job import compute_max_e10_records, read_price_csv


def test_read_price_csv(spark, tmp_path):
    """
    Testing of the read_price_csv function.

    :param spark: Default Spark session fixture
    """
    input_data = [
        Row(
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="dfc32f99-aa65-4fd4-b16e-a2cc53f9be15",
            diesel=1.894,
            e5=1.829,
            e10=1.949,
            diesel_change=1,
            e5_change=0,
            e10_change=1,
        ),
        Row(
            date=datetime.datetime(2023, 8, 12, 0, 0, 10),
            station_uuid="fb69eb8d-6404-4dda-808e-ae839a3ffa7d",
            diesel=1.799,
            e5=1.759,
            e10=1.949,
            diesel_change=0,
            e5_change=1,
            e10_change=1,
        ),
        Row(
            date=datetime.datetime(2023, 12, 8, 0, 0, 10),
            station_uuid="68a52029-8a60-496c-ba3b-e0f35ec9689f",
            diesel=1.899,
            e5=1.769,
            e10=1.829,
            diesel_change=0,
            e5_change=0,
            e10_change=0,
        ),
        Row(
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="75b5a2e6-8ee6-49a3-93a2-f58b034bdecc",
            diesel=1.699,
            e5=1.659,
            e10=1.793,
            diesel_change=1,
            e5_change=0,
            e10_change=1,
        ),
    ]

    input_df = spark.createDataFrame(input_data)

    input_path = tmp_path / "sample_prices"

    input_df.write.mode("overwrite").csv(str(input_path), header=True)
    df = read_price_csv(spark, str(input_path))

    expected_df = input_df.select(F.col("date"), F.col("station_uuid"), F.col("e10"))

    assert_df_equality(df, expected_df, ignore_row_order=True)


def test_compute_max_e10_records(spark):
    """
    Testing of the compute_max_e10_records function.

    :param spark: Default Spark session fixture
    """
    input_data = [
        Row(
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="dfc32f99-aa65-4fd4-b16e-a2cc53f9be15",
            e10=1.949,
        ),
        Row(
            date=datetime.datetime(2023, 8, 12, 0, 0, 10),
            station_uuid="fb69eb8d-6404-4dda-808e-ae839a3ffa7d",
            e10=1.949,
        ),
        Row(
            date=datetime.datetime(2023, 12, 8, 0, 0, 10),
            station_uuid="68a52029-8a60-496c-ba3b-e0f35ec9689f",
            e10=1.829,
        ),
        Row(
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="75b5a2e6-8ee6-49a3-93a2-f58b034bdecc",
            e10=1.793,
        ),
    ]

    input_df = spark.createDataFrame(input_data)

    result = compute_max_e10_records(input_df, top_n=3, threshold=1.90)

    expected_max_price = [1.949, 1.829, 1.793]

    expected_data = [
        Row(
            e10_price=1.949,
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="dfc32f99-aa65-4fd4-b16e-a2cc53f9be15",
            year_month="2023-01",
        ),
        Row(
            e10_price=1.949,
            date=datetime.datetime(2023, 8, 12, 0, 0, 10),
            station_uuid="fb69eb8d-6404-4dda-808e-ae839a3ffa7d",
            year_month="2023-08",
        ),
        Row(
            e10_price=1.829,
            date=datetime.datetime(2023, 12, 8, 0, 0, 10),
            station_uuid="68a52029-8a60-496c-ba3b-e0f35ec9689f",
            year_month="2023-12",
        ),
        Row(
            e10_price=1.793,
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="75b5a2e6-8ee6-49a3-93a2-f58b034bdecc",
            year_month="2023-01",
        ),
    ]

    expected_df = spark.createDataFrame(expected_data)

    assert result.max_prices == expected_max_price
    assert_df_equality(result.max_records, expected_df, ignore_row_order=True)

    # under treshold

    expected_max_price_under_threshold = [1.829, 1.793]

    expected_under_rows = [
        Row(
            e10_price=1.829,
            date=datetime.datetime(2023, 12, 8, 0, 0, 10),
            station_uuid="68a52029-8a60-496c-ba3b-e0f35ec9689f",
            year_month="2023-12",
        ),
        Row(
            e10_price=1.793,
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="75b5a2e6-8ee6-49a3-93a2-f58b034bdecc",
            year_month="2023-01",
        ),
    ]

    assert result.max_prices_under_threshold == expected_max_price_under_threshold
    expected_under_df = spark.createDataFrame(expected_under_rows)
    assert_df_equality(
        result.max_records_under_threshold, expected_under_df, ignore_row_order=True
    )


def test_compute_max_e10_records_with_nulls(spark):
    """
    Testing of the compute_max_e10_records function with null and negative prices.
    :param spark: Default Spark session fixture
    """
    input_data = [
        Row(
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="dfc32f99-aa65-4fd4-b16e-a2cc53f9be15",
            e10=1.949,
        ),
        Row(
            date=datetime.datetime(2023, 8, 12, 0, 0, 10),
            station_uuid="fb69eb8d-6404-4dda-808e-ae839a3ffa7d",
            e10=None,
        ),
        Row(
            date=datetime.datetime(2023, 12, 8, 0, 0, 10),
            station_uuid="68a52029-8a60-496c-ba3b-e0f35ec9689f",
            e10=-1.829,
        ),
        Row(
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="75b5a2e6-8ee6-49a3-93a2-f58b034bdecc",
            e10=1.793,
        ),
    ]

    input_df = spark.createDataFrame(input_data)

    result = compute_max_e10_records(input_df, top_n=3, threshold=2.0)

    expected_max_price = [1.949, 1.793]

    expected_data = [
        Row(
            e10_price=1.949,
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="dfc32f99-aa65-4fd4-b16e-a2cc53f9be15",
            year_month="2023-01",
        ),
        Row(
            e10_price=1.793,
            date=datetime.datetime(2023, 1, 6, 0, 0, 10),
            station_uuid="75b5a2e6-8ee6-49a3-93a2-f58b034bdecc",
            year_month="2023-01",
        ),
    ]

    expected_df = spark.createDataFrame(expected_data)

    assert result.max_prices == expected_max_price
    assert result.max_prices_under_threshold == expected_max_price

    assert_df_equality(result.max_records, expected_df, ignore_row_order=True)
    assert_df_equality(
        result.max_records_under_threshold, expected_df, ignore_row_order=True
    )
