from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from clean_taxi_trips import OUTPUT_COLUMNS, PARTITION_COLUMNS, clean_trips  # noqa: E402

YELLOW_COLS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "Airport_fee",
    "service",
    "year",
    "month",
]


def yellow_row(**overrides):
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 30),
        "passenger_count": 1.0,
        "trip_distance": 3.2,
        "PULocationID": 161,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 18.5,
        "tip_amount": 4.0,
        "total_amount": 26.0,
        "Airport_fee": 0.0,
        "service": "yellow",
        "year": "2024",
        "month": "01",
    }
    row.update(overrides)
    return tuple(row[c] for c in YELLOW_COLS)


def yellow_df(spark, *rows):
    return spark.createDataFrame(list(rows), YELLOW_COLS)


def test_output_schema_matches_contract(spark):
    out = clean_trips(yellow_df(spark, yellow_row()))
    expected = [n for n, _ in OUTPUT_COLUMNS] + PARTITION_COLUMNS
    assert out.columns == expected
    types = dict(out.dtypes)
    assert types["passenger_count"] == "int"
    assert types["payment_type"] == "int"
    assert types["pickup_datetime"] == "timestamp"
    assert types["year"] == "int" and types["month"] == "int"


def test_valid_row_is_kept_and_derived_fields_set(spark):
    [row] = clean_trips(yellow_df(spark, yellow_row())).collect()
    assert row.trip_duration_min == 30.0
    assert (row.service, row.year, row.month) == ("yellow", 2024, 1)
    assert row.vendorid == 1 and row.pulocationid == 161


def test_invalid_rows_are_dropped(spark):
    df = yellow_df(
        spark,
        yellow_row(trip_distance=0.0),
        yellow_row(fare_amount=-5.0),
        yellow_row(tpep_pickup_datetime=None),
        yellow_row(tpep_dropoff_datetime=datetime(2024, 1, 15, 7, 0)),  # before pickup
        yellow_row(tpep_dropoff_datetime=datetime(2024, 1, 17, 8, 0)),  # > 24h
    )
    assert clean_trips(df).count() == 0


def test_pickups_outside_source_month_are_dropped(spark):
    df = yellow_df(
        spark,
        yellow_row(
            tpep_pickup_datetime=datetime(2008, 12, 31, 23, 0),
            tpep_dropoff_datetime=datetime(2008, 12, 31, 23, 20),
        ),
    )
    assert clean_trips(df).count() == 0


def test_duplicates_are_removed(spark):
    df = yellow_df(spark, yellow_row(), yellow_row(tip_amount=9.9))
    assert clean_trips(df).count() == 1


def test_green_trips_use_lpep_columns(spark):
    cols = [
        "VendorID",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "service",
        "year",
        "month",
    ]
    df = spark.createDataFrame(
        [
            (
                2,
                datetime(2024, 1, 3, 9, 0),
                datetime(2024, 1, 3, 9, 12),
                1,
                1.1,
                74,
                75,
                2,
                7.0,
                "green",
                "2024",
                "01",
            )
        ],
        cols,
    )
    [row] = clean_trips(df).collect()
    assert row.service == "green"
    assert row.trip_duration_min == 12.0
    assert row.airport_fee is None  # green files have no airport_fee column


def test_unsupported_services_are_filtered(spark):
    assert clean_trips(yellow_df(spark, yellow_row(service="fhvhv"))).count() == 0


def test_case_variant_columns_are_merged(spark):
    cols = YELLOW_COLS + ["airport_fee"]
    rows = [
        yellow_row(Airport_fee=None) + (1.75,),
        yellow_row(PULocationID=1, Airport_fee=1.25) + (None,),
    ]
    out = clean_trips(spark.createDataFrame(rows, cols))
    fees = sorted(r.airport_fee for r in out.collect())
    assert fees == [1.25, 1.75]
