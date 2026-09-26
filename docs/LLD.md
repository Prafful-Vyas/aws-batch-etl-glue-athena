# Low-Level Design — Batch ETL Pipeline (S3 + Glue + Athena)

## 1. S3 bucket & key structure

Bucket: `s3://<account-alias>-nyc-taxi-dea/` (bucket names are global — pick something unique)

```
raw/nyc-taxi/service=yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
raw/nyc-taxi/service=green/year=2024/month=01/green_tripdata_2024-01.parquet

reference/taxi_zones/taxi_zone_lookup.csv

processed/nyc-taxi/service=yellow/year=2024/month=1/part-00000-<uuid>.snappy.parquet

scripts/clean_taxi_trips.py          # Glue job script, uploaded by Terraform
tmp/glue/                            # Glue TempDir
analytics/athena-results/
```

- Partition keys are Hive-style `key=value` directories so both the Glue Crawler (raw) and Athena partition projection (processed) work.
- Raw months are zero-padded (`month=01`, as landed); processed months are integers written by Spark (`month=1`), which is what the projection's integer type expects.
- **One file per processed partition:** the job hash-repartitions on `service, year, month` before writing, so each partition is written by one task. A month of yellow data is ~50 MB of Parquet — well inside the 128–512 MB guideline's "don't make lots of small files" intent. For much bigger partitions, add a salt or `maxRecordsPerFile`.
- **Lifecycle policy:** `raw/` → S3 Glacier Instant Retrieval after 90 days (source files are re-downloadable but keep a cheap archive; GIR stays readable by Glue/Athena). `processed/` stays Standard (actively queried).

## 2. Glue Data Catalog

| Database | Table | Managed by |
|---|---|---|
| `nyc_taxi_raw` | `nyc_taxi` | Raw crawler over `raw/nyc-taxi/` (the crawler names a table after its folder) |
| `nyc_taxi_processed` | `trips_processed` | Terraform (`aws_glue_catalog_table.trips_processed`), partition projection |
| `nyc_taxi_processed` | `taxi_zones` | Terraform, OpenCSVSerde over `reference/taxi_zones/` |
| `nyc_taxi_processed` | `daily_trip_summary` (view) | Created by saved query `06_create_daily_trip_summary_view` |

**Raw crawler configuration:** `TableGroupingPolicy = CombineCompatibleSchemas` and `TableLevelConfiguration = 3` force a single table at `raw/nyc-taxi/` (bucket = level 1) even though yellow and green schemas differ slightly; partitions inherit the table schema (`AddOrUpdateBehavior = InheritFromTable`) to avoid partition/table schema mismatches as column types drift between months.

Raw columns (yellow): `vendorid`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `ratecodeid`, `store_and_fwd_flag`, `pulocationid`, `dolocationid`, `payment_type`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `airport_fee`. Green uses `lpep_*` timestamps and adds `ehail_fee`, `trip_type`. Partition columns: `service`, `year`, `month` (strings).

**Processed schema** (the contract between `OUTPUT_COLUMNS` in the job and `local.processed_columns` in Terraform — change both together):

| Column | Type | Notes |
|---|---|---|
| `vendorid` | int | |
| `pickup_datetime`, `dropoff_datetime` | timestamp | from `tpep_*` (yellow) or `lpep_*` (green) |
| `passenger_count`, `ratecodeid`, `payment_type` | int | raw files flip between double and bigint; normalized |
| `pulocationid`, `dolocationid` | int | join to `taxi_zones.locationid` |
| `trip_distance`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `total_amount` | double | `airport_fee` is null for green |
| `trip_duration_min` | double | derived |
| `service` (partition) | string | `yellow` / `green` |
| `year`, `month` (partitions) | int | the source file's month |

## 3. Glue ETL job (PySpark)

**Job name:** `<project_name>-clean-taxi-trips`
**Type:** Spark, Glue version 4.0 (Spark 3.3, Python 3.10)
**Worker type:** `G.1X`, `NumberOfWorkers: 5` (tune up for full-year backfills)
**Timeout:** 60 min (`glue_job_timeout_minutes`) · **Max concurrent runs:** 1 (runs share a bookmark)

Job parameters:

```
--job-bookmark-option              job-bookmark-enable
--enable-metrics                   true
--enable-continuous-cloudwatch-log true
--TempDir                          s3://<bucket>/tmp/glue/
--source_database                  nyc_taxi_raw
--source_table                     nyc_taxi
--target_path                      s3://<bucket>/processed/nyc-taxi/
--dq_fail_on_error                 true
```

Transformation logic ([`glue_jobs/clean_taxi_trips.py`](../glue_jobs/clean_taxi_trips.py)). `clean_trips()` is plain PySpark with no `awsglue` imports, so it's unit tested locally in [`tests/`](../tests/):

1. Read the raw table with `create_dynamic_frame.from_catalog(..., transformation_ctx="raw_dyf")`. The `transformation_ctx` is what enables bookmarking. `resolveChoice(choice="cast:double")` settles numeric columns whose type differs between monthly files.
2. If the bookmark finds no new files, commit and exit.
3. Lower-case all column names, merging columns that differ only by case (`airport_fee` vs `Airport_fee` across TLC releases).
4. Keep only `service IN ('yellow','green')`; map `tpep_*`/`lpep_*` timestamps to `pickup_datetime`/`dropoff_datetime`.
5. Drop rows where `trip_distance <= 0`, `fare_amount < 0`, either timestamp is null, `dropoff <= pickup`, or duration > 24 h.
6. Drop rows whose pickup falls outside the source file's year/month (each monthly file has a few trips dated years away; left in, they'd create junk partitions).
7. Derive `trip_duration_min`; cast every column to the processed schema.
8. Deduplicate on (`service`, `vendorid`, `pickup_datetime`, `pulocationid`, `dolocationid`).
9. Run the Data Quality ruleset (§4). If any rule fails and `dq_fail_on_error=true`, raise, so nothing is written.
10. Write with the Spark writer: `repartition(service, year, month)`, `partitionBy(service, year, month)`, Snappy, `mode("overwrite")` with **`spark.sql.sources.partitionOverwriteMode=dynamic`**. The Glue sink (`write_dynamic_frame`) only appends. Spark's default *static* overwrite would delete every partition not in the current batch, which with bookmarks means every earlier month. Dynamic overwrite replaces only the partitions this batch touches, so re-runs are idempotent.

## 4. Glue Data Quality (DQDL)

Evaluated in-job on every batch, before the write, via `EvaluateDataQuality.apply`. Results are published to the Glue Data Quality console and as CloudWatch metrics (`Glue Data Quality` namespace):

```
Rules = [
    ColumnValues "trip_distance" > 0,
    ColumnValues "fare_amount" >= 0,
    IsComplete "pickup_datetime",
    IsComplete "dropoff_datetime",
    ColumnValues "passenger_count" <= 6 with threshold > 0.9,
    Uniqueness "service" "vendorid" "pickup_datetime" "pulocationid" "dolocationid" > 0.99
]
```

- The first four restate the cleaning filters. A failure on those means the transform regressed, not the data.
- `passenger_count` uses a threshold because TLC data has a few percent of null/out-of-range values that are legitimate (e.g. store-and-forward records).
- Failed-rule action: with `dq_fail_on_error = true` (default) the job fails, EventBridge fires the alert, and `processed/` is untouched. Set it to `false` to log and publish only.

## 5. IAM (least privilege)

**Role:** `<project_name>-glue-etl-role`, shared by the crawler and the job (inline policy `<project_name>-glue-etl-policy`):

- `s3:ListBucket` on the bucket.
- `s3:GetObject` on `raw/*` and `scripts/*`.
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `processed/*` and `tmp/*` (delete is required by overwrite and by Spark's commit protocol).
- Glue catalog read/write (tables + partitions) on the catalog, `nyc_taxi_raw` and its tables only. The job doesn't touch the processed catalog, because Terraform owns that table.
- `glue:PublishDataQuality` on `dataQualityRuleset/*`.
- `cloudwatch:PutMetricData` on `*`, limited by condition to the `Glue` and `Glue Data Quality` namespaces (the action has no resource-level permissions).
- `kms:Decrypt`, `kms:GenerateDataKey` on the project CMK.
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` on `/aws-glue/*` in this account.
- No `s3:*` or `glue:*` wildcards; every ARN is scoped to this account.

**KMS key policy** (`aws_kms_key_policy.this`): the account root statement (so IAM policies apply), plus `kms:Decrypt`/`kms:GenerateDataKey*` for `events.amazonaws.com`. Without that grant, EventBridge can't publish to the KMS-encrypted SNS topic, and alerts are dropped silently.

**SNS topic policy:** `SNS:Publish` for `events.amazonaws.com`, restricted with `aws:SourceArn` to this project's two EventBridge rules.

## 6. Athena

- Dedicated **workgroup** `<project_name>-analytics`:
  - Query result location `s3://<bucket>/analytics/athena-results/`, SSE-KMS with the project key, enforced for all queries.
  - "Publish query metrics to CloudWatch" enabled.
  - Per-query bytes-scanned limit (default 5 GB) to prevent accidental full-table scans.
  - Enforced result location rules out CTAS `external_location`, so reusable rollups are **views** (`daily_trip_summary`).
- `trips_processed` uses **partition projection**, defined in Terraform table parameters:

```
projection.enabled          = true
projection.service.type     = enum      projection.service.values = yellow,green
projection.year.type        = integer   projection.year.range     = 2019,2030
projection.month.type       = integer   projection.month.range    = 1,12
storage.location.template   = s3://<bucket>/processed/nyc-taxi/service=${service}/year=${year}/month=${month}/
```

  Athena computes partition locations from the query predicates, so a newly processed month is queryable without a crawler run or `MSCK REPAIR TABLE`. Projected partitions with no data return zero rows.
- Every file in [`sql/`](../sql/) is deployed as a saved query (`aws_athena_named_query`). Partition-pruned example:

```sql
SELECT service, COUNT(*) trips, AVG(fare_amount) avg_fare
FROM nyc_taxi_processed.trips_processed
WHERE year = 2024 AND month = 1
GROUP BY service;
```

## 7. Orchestration, monitoring & governance

- **Glue Workflow** `<project_name>-pipeline`: start trigger (`ON_DEMAND`, or `SCHEDULED` when `pipeline_schedule` is set) → raw crawler → `CONDITIONAL` trigger on crawl `SUCCEEDED` → ETL job. `scripts/run_pipeline.py` starts a run and polls until it finishes. Glue marks a workflow run `COMPLETED` even when an action inside it failed, so the script also checks `FailedActions`.
- **Alerts:** EventBridge rules for **Glue Job State Change** (`FAILED`, `TIMEOUT`, `ERROR`) and **Glue Crawler State Change** (`Failed`) → SNS topic `<project_name>-alerts` (optional email subscription). The job-state event fires as soon as the run ends, with no wait for a metric evaluation period.
- **Duration regressions:** covered by the job `timeout`. A run that exceeds it ends in `TIMEOUT`, which triggers the same alert.
- CloudTrail (org trail or account trail) captures all Glue/Athena/S3 API calls for audit.
- Glue Data Catalog table versions record raw schema changes the crawler makes between months.

## 8. Cost estimate (order of magnitude)

- Glue ETL: `G.1X` = 1 DPU/worker; 5 workers × ~10 min/monthly run ≈ 0.8 DPU-hour ≈ $0.35 per run. Crawler runs are billed per DPU-second with a 10-minute minimum, which comes to a few cents.
- S3 storage: raw ~50 MB/month (yellow), processed ~similar (Parquet+Snappy) — negligible at Standard rates.
- Athena: $5/TB scanned — partitioning + Parquet keeps typical exploratory queries in the MB range.

## 9. Terraform resource inventory

All of the above is implemented in [`infra/main.tf`](../infra/main.tf), calling the vendored `kms_key`, `s3_data_lake_bucket` and `alerting` modules (`infra/modules/`).

| Resource | Terraform address | Notes |
|---|---|---|
| CMK + key policy | `module.kms.aws_kms_key.this`, `aws_kms_key_policy.this` | rotation enabled, aliased `alias/<project_name>-key`; policy grants EventBridge use for SNS |
| S3 bucket (all tiers) | `module.data_lake.aws_s3_bucket.this` | one bucket, prefix-separated tiers; `raw/` lifecycle rule to Glacier IR at 90 days |
| Glue databases | `aws_glue_catalog_database.raw` / `.processed` | `nyc_taxi_raw`, `nyc_taxi_processed` |
| Processed + zone tables | `aws_glue_catalog_table.trips_processed` / `.taxi_zones` | partition projection; OpenCSVSerde |
| Glue IAM role + inline policy | `aws_iam_role.glue_etl` / `aws_iam_role_policy.glue_etl` | §5 |
| Raw crawler | `aws_glue_crawler.raw` | grouping + table-level config, §2 |
| ETL script upload | `aws_s3_object.etl_script` | uploads [`glue_jobs/clean_taxi_trips.py`](../glue_jobs/clean_taxi_trips.py); `etag` = file MD5 so `apply` re-uploads on script changes |
| Glue job | `aws_glue_job.clean_taxi_trips` | bookmarks, timeout, DQ flag, §3 |
| Workflow + triggers | `aws_glue_workflow.pipeline`, `aws_glue_trigger.start` / `.run_etl` | §7 |
| Athena workgroup + saved queries | `aws_athena_workgroup.analytics`, `aws_athena_named_query.this[*]` | one saved query per `sql/*.sql` |
| Alerting | `aws_cloudwatch_event_rule.glue_job_failed` / `.glue_crawler_failed` + targets, `aws_sns_topic_policy.allow_eventbridge`, `module.alerts` | §7 |

**Apply order caveat:** `terraform apply` provisions the pipeline but doesn't run it. That boundary is deliberate: Terraform manages infrastructure, not pipeline execution. Land data with `scripts/load_trip_data.py`, then run `scripts/run_pipeline.py` (or wait for the scheduled trigger).

**Destroy safety:** `module.data_lake` defaults `force_destroy = true` and the workgroup sets `force_destroy = true`, so `terraform destroy` removes everything, including anything you've landed in `raw/`/`processed/`/`analytics/`. Download anything worth keeping first, or set `force_destroy = false` on the module.

## 10. CI/CD

GitHub Actions workflows are in `.github/workflows/`; the README's CI/CD section has the one-time setup.

- **Identity:** GitHub OIDC → IAM roles created by `bootstrap/`, with no stored AWS keys.
  - The plan role (`ReadOnlyAccess` + state bucket) trusts `pull_request` and `refs/heads/main`.
  - The deploy role trusts only `environment:production`, so GitHub environment protection (required reviewers) gates every apply, pipeline run and destroy.
  - The deploy role is scoped to this project's name prefixes (`<project_name>-*`, `nyc_taxi_*`, the data bucket). The one exception is `kms:*`, because `kms:CreateKey` can't be resource-scoped.
- **State:** S3 backend in CI only, with S3-native locking (`use_lockfile`, Terraform ≥ 1.10; CI pins 1.11.4). The backend block is generated as `backend_override.tf` (gitignored), so local development keeps local state.
- **Plan/apply:** `deploy.yml` plans with the read-only role and uploads the binary plan (3-day retention, since plans can contain sensitive values). The apply job applies exactly that plan; if state changed in between, Terraform rejects it as stale. A shared concurrency group (`terraform-production`) serializes deploy and destroy runs.
- **Quality gates** (`ci.yml`, also run as the first stage of `deploy.yml`): ruff, pytest on Python 3.10 + JDK 11 (Glue 4.0 parity), `terraform fmt`/`validate`, tflint (AWS ruleset), and Checkov. Accepted Checkov findings are documented in `.checkov.yaml` and inline.
