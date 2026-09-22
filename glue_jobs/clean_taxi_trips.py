import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_database", "source_table", "target_path"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["source_database"],
    table_name=args["source_table"],
    transformation_ctx="raw_dyf",
)
df = raw_dyf.toDF()

pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "pickup_datetime"
dropoff_col = "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "dropoff_datetime"

cleaned = (
    df.withColumn("passenger_count", F.col("passenger_count").cast("int"))
    .withColumn("payment_type", F.col("payment_type").cast("int"))
    .withColumn(pickup_col, F.col(pickup_col).cast("timestamp"))
    .withColumn(dropoff_col, F.col(dropoff_col).cast("timestamp"))
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("fare_amount") >= 0)
    .filter(F.col(pickup_col).isNotNull() & F.col(dropoff_col).isNotNull())
    .filter(F.col(dropoff_col) > F.col(pickup_col))
    .withColumn(
        "trip_duration_min",
        (F.unix_timestamp(dropoff_col) - F.unix_timestamp(pickup_col)) / 60.0,
    )
    .withColumn("year", F.year(pickup_col))
    .withColumn("month", F.month(pickup_col))
    .dropDuplicates(["vendorid", pickup_col, "pulocationid", "dolocationid"])
)

(
    cleaned.repartition("year", "month")
    .write.mode("overwrite")
    .partitionBy("year", "month")
    .option("compression", "snappy")
    .parquet(args["target_path"])
)

job.commit()
