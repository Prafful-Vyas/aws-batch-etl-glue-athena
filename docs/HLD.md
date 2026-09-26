# High-Level Design — Batch ETL Pipeline (S3 + Glue + Athena)

## 1. Domain context

NYC TLC trip records capture every yellow/green taxi and for-hire-vehicle (Uber/Lyft-class) trip in New York City: pickup/dropoff timestamps and locations, distance, fare components, passenger count, and payment type. Business questions this pipeline should be able to answer once curated:

- What are peak demand hours/zones by day of week?
- How does average fare/tip vary by borough and month?
- What's the trend in trip volume by vendor/service type over time?

These are the same question shapes (demand forecasting, geo/time aggregation, revenue analysis) asked of any trip- or order-based dataset in logistics, delivery, and ride-share companies.

**Scope:** yellow and green taxi trips, which share one schema family (`tpep_*` vs `lpep_*` timestamps). High-volume FHV (`fhvhv`) files have a different schema (no vendor/fare breakdown, different column names) and are out of scope; adding them would mean a separate raw crawler target and a mapping in the job.

## 2. Architecture overview

```
 NYC TLC CloudFront ──(scripts/load_trip_data.py)──┐
                                                   ▼
                 ┌──────────────────────────────────────────────┐
                 │  Amazon S3 (SSE-KMS)                         │
                 │  raw/  reference/  processed/  analytics/     │
                 └───────┬──────────────────────▲───────────────┘
   Glue Workflow         │                      │
   (on demand or   ┌─────▼──────────┐           │
    monthly cron)  │ Glue Crawler   │ infers schema, registers
        │          │ (raw db)       │ service/year/month partitions
        │          └─────┬──────────┘
        │                │ on SUCCEEDED
        │          ┌─────▼──────────┐           │
        └─────────▶│ Glue ETL Job   │ PySpark: normalize, clean, dedupe,
                   │ G.1X, bookmark │ DQ gate, write Parquet+Snappy ──┘
                   └─────┬──────────┘
                         │ FAILED / TIMEOUT          ┌────────────────────┐
                         └──▶ EventBridge ──▶ SNS ──▶│ email alert        │
                                                     └────────────────────┘
                   ┌────────────────┐
                   │ Glue Catalog   │ trips_processed (Terraform-owned,
                   │ (processed db) │ partition projection), taxi_zones
                   └─────┬──────────┘
                   ┌─────▼──────────┐
                   │ Amazon Athena  │ workgroup: scan cap, KMS results,
                   │                │ saved queries from sql/
                   └────────────────┘
```

## 3. Components

| Component | Purpose |
|---|---|
| S3 `raw/` | Immutable landing zone for source Parquet files as downloaded, partitioned by service/year/month |
| S3 `reference/` | Taxi-zone lookup CSV (LocationID → borough/zone) |
| S3 `processed/` | Cleaned, typed, deduplicated data in Parquet+Snappy, partitioned for query pruning |
| S3 `analytics/` | Athena query result location |
| Glue Data Catalog | Two databases (`nyc_taxi_raw`, `nyc_taxi_processed`) shared across Glue and Athena |
| Glue Crawler | Infers the raw schema (handles drift between months) and registers new raw partitions |
| Glue ETL Job | PySpark cleaning/transformation; job bookmarks so re-runs only process new files; Data Quality gate before write |
| Glue Workflow | Chains crawler → job; on-demand or scheduled |
| `trips_processed` table | Declared in Terraform with partition projection — the job owns the output schema, so no processed-side crawler is needed |
| Athena | Serverless SQL over the processed table; saved queries for the business questions |
| EventBridge + SNS | Alerts on job failure/timeout and crawler failure |

## 4. Data flow

1. A month of trip data is downloaded and uploaded to `raw/nyc-taxi/service=<yellow|green>/year=YYYY/month=MM/` (`scripts/load_trip_data.py`).
2. The Glue workflow starts (`scripts/run_pipeline.py`, or its cron trigger) and runs the raw crawler, which updates the raw table's schema and partitions.
3. When the crawl succeeds, a conditional trigger starts the ETL job. It reads only files that are new since the last **job bookmark**, normalizes yellow/green columns to one schema, drops invalid rows, and deduplicates.
4. The job evaluates the **Data Quality** ruleset on the batch. If any rule fails, the job fails without writing, and the failure alert fires.
5. The job writes to `processed/` partitioned by `service`, `year`, `month`, using **dynamic partition overwrite**: only the partitions in this batch are replaced.
6. Athena queries `trips_processed` straight away. **Partition projection** computes partition locations from the query predicates, so new months need no crawl or `MSCK REPAIR`.

## 5. Key exam concepts exercised

- Glue job bookmarking for incremental batch loads.
- S3 partitioning strategy and its effect on Athena scan cost/performance; partition projection vs. crawler-managed partitions.
- Columnar storage (Parquet) + compression (Snappy) trade-offs vs. row formats.
- Glue Crawler grouping/table-level configuration and schema evolution (column type/name drift across months).
- Glue Data Quality (DQDL) evaluated inside an ETL job.
- Glue Workflows and triggers for orchestration.
- Athena workgroups, query result location, and cost controls (bytes-scanned limits).
- KMS key policies for service principals (EventBridge → encrypted SNS).

## 6. Non-functional considerations

- **Cost:** Glue DPU-hours dominate cost; Athena is pay-per-TB-scanned, so partitioning/columnar format directly controls spend.
- **Idempotency:** bookmarks prevent reprocessing, and dynamic partition overwrite means re-running a month (or re-uploading a corrected file) replaces that month rather than duplicating or wiping others.
- **Data quality:** bad batches are stopped before they reach `processed/`, rather than discovered in Athena.
- **Security:** SSE-KMS on every tier, the SNS topic and Athena results; Glue uses one IAM role scoped to this bucket's prefixes, the raw catalog database and the project key (see LLD).

## 7. Infrastructure as Code (Terraform)

Every resource above — the KMS key and its policy, the bucket and its lifecycle rule, both Glue databases, the crawler, the processed and zone tables, the ETL job, the workflow, the Athena workgroup and saved queries, and the failure-alert wiring — is defined in [`infra/`](../infra/) rather than clicked together in the console, so the whole environment is reproducible and disposable.

```
infra/
├── versions.tf              # Terraform + AWS provider version pins
├── providers.tf              # provider config, default tags
├── variables.tf               # bucket_name, project_name, worker sizing, schedule, etc.
├── main.tf                     # all resources (see docs/LLD.md §9 for the full inventory)
├── outputs.tf                    # bucket name, job/crawler/workflow/workgroup names
├── terraform.tfvars.example       # copy to terraform.tfvars and fill in
└── modules/                        # vendored — kms_key, s3_data_lake_bucket, alerting
```

This project is its own standalone git repository, so the common building blocks (`kms_key`, `s3_data_lake_bucket`, `alerting`) live vendored under `infra/modules/` rather than referenced from a path outside the repo — the module code is identical across all four projects in the portfolio, just copied rather than shared, so each repo stays self-contained and clonable on its own.

**Workflow:**

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set a globally-unique bucket_name
terraform init
terraform apply
cd ..
python scripts/load_trip_data.py --bucket <bucket_name> --months 2024-01
python scripts/run_pipeline.py
# ... query in Athena ...
cd infra && terraform destroy     # tears everything back down when you're done experimenting
```

State is kept **local** to this project's `infra/` folder (no remote backend) — appropriate for a single-developer personal project; add an S3 backend block to `versions.tf` if you want durable/shareable state later. See [`docs/LLD.md` §9](LLD.md#9-terraform-resource-inventory) for the full resource-to-file mapping and destroy-safety notes.
