# Low-Level Design — Batch ETL Pipeline (S3 + Glue + Athena)

## 1. S3 bucket & key structure

Bucket: `s3://<account-alias>-nyc-taxi-dea/` (bucket names are global — pick something unique)

```
raw/nyc-taxi/service=yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
raw/nyc-taxi/service=green/year=2024/month=01/green_tripdata_2024-01.parquet
raw/nyc-taxi/service=fhvhv/year=2024/month=01/fhvhv_tripdata_2024-01.parquet

processed/nyc-taxi/service=yellow/year=2024/month=01/part-0000-<uuid>.snappy.parquet

analytics/athena-results/
analytics/agg/daily_trip_summary/year=2024/month=01/
```

- Partition keys chosen as Hive-style `key=value` directories so Glue Crawler and Athena partition projection both work.
- Target processed file size: 128–512 MB per partition file (use `repartition`/`coalesce` in the Spark job) — avoids the "small files problem" that kills Athena/Glue performance.
- **Lifecycle policy:** `raw/` → transition to S3 Glacier Instant Retrieval after 90 days (source files are re-downloadable but keep a cheap archive); `processed/` stays Standard (actively queried).

## 2. Glue Data Catalog

| Database | Table | Source |
|---|---|---|
| `nyc_taxi_raw` | `trips_raw` | Crawler over `raw/nyc-taxi/` |
| `nyc_taxi_processed` | `trips_processed` | Crawler (or job's `enableUpdateCatalog`) over `processed/nyc-taxi/` |

Raw table columns (subset, yellow service): `vendorid (int)`, `tpep_pickup_datetime (timestamp)`, `tpep_dropoff_datetime (timestamp)`, `passenger_count (double)`, `trip_distance (double)`, `pulocationid (int)`, `dolocationid (int)`, `payment_type (bigint)`, `fare_amount (double)`, `tip_amount (double)`, `total_amount (double)`.

Processed table adds: `trip_duration_min (double)`, `service (string, partition)`, `year (int, partition)`, `month (int, partition)`.

## 3. Glue ETL job (PySpark)

**Job name:** `clean-nyc-taxi-trips`
**Type:** Spark, Glue version 4.0 (Spark 3.3)
**Worker type:** `G.1X`, `NumberOfWorkers: 5` (tune up for full-year backfills)

Key job parameters:

```
--job-bookmark-option        job-bookmark-enable
--enable-metrics              true
--enable-continuous-cloudwatch-log true
--TempDir                     s3://<bucket>/tmp/glue/
--additional-python-modules   (none needed; pure PySpark)
```

Transformation logic:

1. Read raw table via `glueContext.create_dynamic_frame.from_catalog(database="nyc_taxi_raw", table_name="trips_raw", transformation_ctx="raw_dyf")` — the `transformation_ctx` is what enables bookmarking.
2. Convert to DataFrame; drop rows where `trip_distance <= 0`, `fare_amount < 0`, `pickup_datetime is null or dropoff_datetime is null`, or `dropoff_datetime <= pickup_datetime`.
3. Cast `passenger_count` to `int`, `payment_type` to `int` (schema-on-read gives doubles/bigints inconsistently across months — normalize explicitly).
4. Derive `trip_duration_min = (unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60`.
5. Deduplicate on a composite key (`vendorid`, `pickup_datetime`, `pulocationid`, `dolocationid`) in case of overlapping monthly re-uploads.
6. `repartition(1)` per output partition (year/month/service) before write to control file size.
7. Write with `glueContext.write_dynamic_frame.from_options(..., connection_type="s3", format="glueparquet", connection_options={"path": "s3://.../processed/nyc-taxi/", "partitionKeys": ["service","year","month"]}, format_options={"compression": "snappy"})`.

## 4. Glue Data Quality (DQDL)

Attach a Data Quality ruleset to the processed table, evaluated on each crawler/job run:

```
Rules = [
    ColumnValues "trip_distance" > 0,
    ColumnValues "fare_amount" >= 0,
    IsComplete "tpep_pickup_datetime",
    IsComplete "tpep_dropoff_datetime",
    ColumnValues "passenger_count" between 0 and 6,
    Uniqueness "vendorid,tpep_pickup_datetime,pulocationid,dolocationid" > 0.99
]
```

Failed-rule action: publish to CloudWatch metric + fail the job run if uniqueness or completeness drops below threshold (configurable "Pass"/"Fail" ruleset action).

## 5. IAM (least privilege)

**Role:** `GlueETLRole-nyc-taxi`

- `s3:GetObject`, `s3:ListBucket` scoped to `arn:aws:s3:::<bucket>/raw/*` and the bucket ARN with prefix condition.
- `s3:PutObject`, `s3:DeleteObject` scoped to `arn:aws:s3:::<bucket>/processed/*` and `/tmp/glue/*`.
- `glue:GetTable`, `glue:GetPartitions`, `glue:BatchCreatePartition`, `glue:UpdateTable` scoped to the two Glue databases.
- `kms:Decrypt`, `kms:GenerateDataKey` scoped to the project's customer-managed KMS key.
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` for `/aws-glue/*`.
- No `s3:*` or `glue:*` wildcards — every action explicit.

## 6. Athena

- Dedicated **workgroup** `nyc-taxi-analytics` with:
  - Query result location: `s3://<bucket>/analytics/athena-results/`
  - "Publish query metrics to CloudWatch" enabled.
  - Per-query bytes-scanned limit (e.g., 5 GB) to prevent accidental full-table scans blowing up cost.
- Table registered with **partition projection** (avoids needing `MSCK REPAIR TABLE` after every new month):

```sql
ALTER TABLE nyc_taxi_processed.trips_processed SET TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.year.type' = 'integer',
  'projection.year.range' = '2020,2026',
  'projection.month.type' = 'integer',
  'projection.month.range' = '1,12',
  'projection.service.type' = 'enum',
  'projection.service.values' = 'yellow,green,fhvhv',
  'storage.location.template' = 's3://<bucket>/processed/nyc-taxi/service=${service}/year=${year}/month=${month}/'
);
```

- Sample query showing partition pruning:

```sql
SELECT service, COUNT(*) trips, AVG(fare_amount) avg_fare
FROM nyc_taxi_processed.trips_processed
WHERE year = 2024 AND month = 1
GROUP BY service;
```

## 7. Monitoring & governance

- EventBridge rule matching `aws.glue` **Glue Job State Change** events with `state IN [FAILED, TIMEOUT, ERROR]` for this job → SNS topic `<project_name>-alerts` (chosen over a raw CloudWatch metric alarm on `numFailedTasks` because the job-state-change event is emitted immediately on completion, rather than waiting on a metric evaluation period).
- CloudWatch Alarm on job run **duration** exceeding a threshold (catches silent data-volume regressions) — left as a follow-up enhancement, not yet in the Terraform.
- CloudTrail (org trail or account trail) captures all Glue/Athena/S3 API calls for audit.
- Glue Data Catalog table versioning enabled so schema drift between monthly files is visible.

## 8. Cost estimate (order of magnitude)

- Glue ETL: `G.1X` = 1 DPU/worker; 5 workers × ~10 min/monthly run ≈ 0.8 DPU-hour ≈ a few cents per run.
- S3 storage: raw ~500 MB/month, processed ~150 MB/month (Parquet+Snappy) — negligible at Standard rates for a personal project.
- Athena: $5/TB scanned — partitioning + Parquet keeps typical exploratory queries under a few MB–GB.

## 9. Terraform resource inventory

All of the above is implemented in [`infra/main.tf`](../infra/main.tf), calling the vendored `kms_key` and `s3_data_lake_bucket` modules (`infra/modules/`) for the pieces every project needs.

| Resource | Terraform address | Notes |
|---|---|---|
| CMK | `module.kms.aws_kms_key.this` | rotation enabled, aliased `alias/<project_name>-key` |
| S3 bucket (all 3 tiers) | `module.data_lake.aws_s3_bucket.this` | one bucket, prefix-separated tiers; `raw/` lifecycle rule to Glacier IR at 90 days |
| Glue databases | `aws_glue_catalog_database.raw` / `.processed` | `nyc_taxi_raw`, `nyc_taxi_processed` |
| Glue IAM role + inline policy | `aws_iam_role.glue_etl` / `aws_iam_role_policy.glue_etl` | least-privilege scoping described in §5 above, generated with the `<project_name>-` prefix rather than the illustrative `GlueETLRole-nyc-taxi` name used earlier in this doc |
| Crawlers | `aws_glue_crawler.raw` / `.processed` | must be run manually (or scheduled) after `apply` — Terraform creates them but doesn't invoke them |
| ETL script upload | `aws_s3_object.etl_script` | uploads [`glue_jobs/clean_taxi_trips.py`](../glue_jobs/clean_taxi_trips.py); `etag` set to the file's MD5 so `apply` re-uploads automatically on script changes |
| Glue job | `aws_glue_job.clean_taxi_trips` | job-bookmarking enabled by default per §3 |
| Athena workgroup | `aws_athena_workgroup.analytics` | bytes-scanned cutoff and SSE-KMS result encryption |
| Alerting | `aws_cloudwatch_event_rule.glue_job_failed` + `module.alerts` | see §7 |

**Apply order caveat:** `terraform apply` provisions the crawlers and job but does not run them — that's a deliberate boundary (Terraform manages infrastructure, not data pipeline execution). After `apply`, manually trigger `aws glue start-crawler --name <raw_crawler_name>` once, then the Glue job, before querying Athena.

**Destroy safety:** `module.data_lake` defaults `force_destroy = true` so `terraform destroy` can delete the bucket even with data in it — convenient for a personal project, but it means destroying is genuinely destructive to anything you've landed in `raw/`/`processed/`/`analytics/`. Download anything worth keeping first, or override `force_destroy = false` in `terraform.tfvars`.
