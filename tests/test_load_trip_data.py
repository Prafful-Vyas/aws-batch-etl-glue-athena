import argparse

import pytest

from load_trip_data import parse_month, raw_key


def test_raw_key_uses_hive_partitions():
    assert raw_key("yellow", 2024, 1) == (
        "raw/nyc-taxi/service=yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet"
    )


def test_parse_month():
    assert parse_month("2024-03") == (2024, 3)


@pytest.mark.parametrize("value", ["2024-13", "2024-1", "24-01", "2024/01"])
def test_parse_month_rejects_bad_input(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_month(value)
