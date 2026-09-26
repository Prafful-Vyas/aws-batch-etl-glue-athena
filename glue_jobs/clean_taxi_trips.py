"""Glue ETL job: raw NYC TLC trip records -> cleaned, partitioned Parquet.

The transformation logic (`clean_trips`) is plain PySpark with no awsglue
dependency so it can be unit tested locally; everything Glue-specific
(catalog read, bookmarks, Data Quality, job commit) lives in `main()`.
"""

import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SUPPORTED_SERVICES = ("yellow", "green")

# Output schema of processed/nyc-taxi/. Must stay in sync with the
# `trips_processed` table defined in infra/main.tf (local.processed_columns).
OUTPUT_COLUMNS = [
    ("vendorid", "int"),
    ("pickup_datetime", "timestamp"),
    ("dropoff_datetime", "timestamp"),
    ("passenger_count", "int"),
    ("trip_distance", "double"),
    ("ratecodeid", "int"),
    ("pulocationid", "int"),
    ("dolocationid", "int"),
    ("payment_type", "int"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("congestion_surcharge", "double"),
    ("airport_fee", "double"),
    ("total_amount", "double"),
    ("trip_duration_min", "double"),
]
PARTITION_COLUMNS = ["service", "year", "month"]
DEDUP_KEY = ["service", "vendorid", "pickup_datetime", "pulocationid", "dolocationid"]

MAX_TRIP_DURATION_MIN = 24 * 60

# Evaluated against every batch before it is written. The filters in
# clean_trips() already enforce the first four rules, so a failure there means
# the transform itself regressed; the last two catch upstream data drift.
DQ_RULESET = """
Rules = [
    ColumnValues "trip_distance" > 0,
    ColumnValues "fare_amount" >= 0,
    IsComplete "pickup_datetime",
    IsComplete "dropoff_datetime",
    ColumnValues "passenger_count" <= 6 with threshold > 0.9,
    Uniqueness "service" "vendorid" "pickup_datetime" "pulocationid" "dolocationid" > 0.99
]
"""


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Lower-case every column, merging columns that differ only by case.

    TLC renamed some columns between releases (e.g. `airport_fee` vs
    `Airport_fee`), so a multi-month read can contain both spellings.
    """
    positional = df.toDF(*[f"__c{i}" for i in range(len(df.columns))])
    groups: dict[str, list[str]] = {}
    for i, name in enumerate(df.columns):
        groups.setdefault(name.lower(), []).append(f"__c{i}")
    return positional.select(
        [F.coalesce(*[F.col(c) for c in cols]).alias(name) for name, cols in groups.items()]
    )


def first_present(df: DataFrame, *names: str):
    """Coalesce whichever of `names` exist in `df` (null literal if none do)."""
    present = [F.col(n) for n in names if n in df.columns]
    return F.coalesce(*present) if present else F.lit(None)


def clean_trips(df: DataFrame) -> DataFrame:
    """Normalize yellow/green trips to one schema and drop invalid rows.

    Expects the raw catalog table's columns, including the Hive partition
    columns `service`, `year`, `month` taken from the raw S3 path.
    """
    df = normalize_column_names(df).filter(F.col("service").isin(*SUPPORTED_SERVICES))

    canonical = df.select(
        F.col("service"),
        F.col("year").cast("int").alias("src_year"),
        F.col("month").cast("int").alias("src_month"),
        first_present(df, "vendorid").alias("vendorid"),
        # yellow uses tpep_*, green uses lpep_*
        first_present(df, "tpep_pickup_datetime", "lpep_pickup_datetime")
        .cast("timestamp")
        .alias("pickup_datetime"),
        first_present(df, "tpep_dropoff_datetime", "lpep_dropoff_datetime")
        .cast("timestamp")
        .alias("dropoff_datetime"),
        *[
            first_present(df, name).alias(name)
            for name, _ in OUTPUT_COLUMNS
            if name not in ("vendorid", "pickup_datetime", "dropoff_datetime", "trip_duration_min")
        ],
    )

    duration_min = (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0

    cleaned = (
        canonical.filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("pickup_datetime").isNotNull() & F.col("dropoff_datetime").isNotNull())
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .withColumn("trip_duration_min", duration_min)
        .filter(F.col("trip_duration_min") <= MAX_TRIP_DURATION_MIN)
        # Monthly files contain a handful of trips with pickup dates years
        # outside the file's month; keep them out of unrelated partitions.
        .filter(
            (F.year("pickup_datetime") == F.col("src_year"))
            & (F.month("pickup_datetime") == F.col("src_month"))
        )
        .withColumn("year", F.col("src_year"))
        .withColumn("month", F.col("src_month"))
    )

    typed = cleaned.select(
        *[F.col(name).cast(dtype).alias(name) for name, dtype in OUTPUT_COLUMNS],
        *[F.col(name) for name in PARTITION_COLUMNS],
    )
    return typed.dropDuplicates(DEDUP_KEY)


def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from awsgluedq.transforms import EvaluateDataQuality
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "source_database", "source_table", "target_path", "dq_fail_on_error"],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    # Only replace the partitions present in this batch; the default ("static")
    # would wipe every previously processed month on each bookmarked run.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    logger = glue_context.get_logger()

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    raw_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=args["source_database"],
        table_name=args["source_table"],
        transformation_ctx="raw_dyf",  # enables job bookmarking for this source
    )
    # Numeric columns flip between int64 and double across monthly files.
    raw_df = raw_dyf.resolveChoice(choice="cast:double").toDF()

    if raw_df.rdd.isEmpty():
        logger.info("No new raw files since the last bookmark; nothing to do.")
        job.commit()
        return

    cleaned = clean_trips(raw_df).persist()

    dq_results = EvaluateDataQuality.apply(
        frame=DynamicFrame.fromDF(cleaned, glue_context, "cleaned"),
        ruleset=DQ_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "clean_taxi_trips",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
    ).toDF()
    failed = dq_results.filter(F.col("Outcome") != "Passed").collect()
    for row in failed:
        logger.warn(f"DQ rule failed: {row['Rule']} -> {row['FailureReason']}")
    if failed and args["dq_fail_on_error"].lower() == "true":
        raise RuntimeError(f"{len(failed)} data quality rule(s) failed; not writing batch.")

    # Hash-partitioning on the output keys sends each partition to one task,
    # so each service/year/month is written as a single file.
    (
        cleaned.repartition(*PARTITION_COLUMNS)
        .write.mode("overwrite")
        .partitionBy(*PARTITION_COLUMNS)
        .option("compression", "snappy")
        .parquet(args["target_path"])
    )

    job.commit()


if __name__ == "__main__":
    main()
