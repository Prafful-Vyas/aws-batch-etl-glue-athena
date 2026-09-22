# High-Level Design — Batch ETL Pipeline (S3 + Glue + Athena)

## 1. Domain context

NYC TLC trip records capture every yellow/green taxi and for-hire-vehicle (Uber/Lyft-class) trip in New York City: pickup/dropoff timestamps and locations, distance, fare components, passenger count, and payment type. Business questions this pipeline should be able to answer once curated:

- What are peak demand hours/zones by day of week?
- How does average fare/tip vary by borough and month?
- What's the trend in trip volume by vendor/service type over time?

These are the same question shapes (demand forecasting, geo/time aggregation, revenue analysis) asked of any trip- or order-based dataset in logistics, delivery, and ride-share companies.

## 2. Architecture overview

```
                 ┌───────────────────────────┐
 NYC TLC site /  │        Amazon S3          │
 CloudFront ────▶│  raw/  → processed/ → analytics/  │
 (monthly files) └───────────┬───────────────┘
                              │
                    ┌─────────▼─────────┐
                    │  Glue Crawler      │  infers schema on raw/
                    │  (raw db)          │
                    └─────────┬─────────┘
                              │ Glue Data Catalog (raw table)
                    ┌─────────▼─────────┐
                    │  Glue ETL Job      │  PySpark: clean, cast types,
                    │  (Spark, G.1X)     │  drop invalid rows, dedupe,
                    │  bookmark=enabled  │  write partitioned Parquet+Snappy
                    └─────────┬─────────┘
                              │ writes to processed/
                    ┌─────────▼─────────┐
                    │  Glue Crawler      │  infers schema on processed/
                    │  (processed db)    │
                    └─────────┬─────────┘
                              │ Glue Data Catalog (processed table)
                    ┌─────────▼─────────┐
                    │  Amazon Athena     │  ad-hoc SQL, workgroup with
                    │                    │  result location in analytics/
                    └────────────────────┘
```

## 3. Components

| Component | Purpose |
|---|---|
| S3 `raw/` | Immutable landing zone for source Parquet files as downloaded, partitioned by service type/year/month |
| S3 `processed/` | Cleaned, typed, deduplicated data in Parquet+Snappy, partitioned for query pruning |
| S3 `analytics/` | Athena query result location + any pre-aggregated summary tables |
| Glue Data Catalog | Central schema registry; two databases (`nyc_taxi_raw`, `nyc_taxi_processed`) shared across Glue and Athena |
| Glue Crawler | Schema inference/registration; run manually first, then on a schedule as new months land |
| Glue ETL Job | PySpark job doing the actual cleaning/transformation; uses job bookmarking so re-runs only process new files |
| Athena | Serverless SQL over the processed table; no infrastructure to manage |

## 4. Data flow

1. New month of trip data is downloaded and uploaded to `raw/nyc-taxi/service=<yellow|green|fhv>/year=YYYY/month=MM/`.
2. Crawler (scheduled or on-demand) updates the raw table's partitions in the Data Catalog.
3. Glue ETL job runs (manually or on an EventBridge schedule), reads only new partitions via **job bookmarking**, applies transformations, writes to `processed/` partitioned by `year`, `month`, `service`.
4. Second crawler (or `enableUpdateCatalog` in the Glue job itself) registers/updates the processed table.
5. Analysts query via Athena; results land in `analytics/athena-results/`.

## 5. Key exam concepts exercised

- Glue job bookmarking for incremental batch loads.
- S3 partitioning strategy and its effect on Athena scan cost/performance.
- Columnar storage (Parquet) + compression (Snappy) trade-offs vs. row formats.
- Glue Crawler classifiers and schema evolution (new columns appearing in newer months).
- Athena workgroups, query result location, and cost controls (bytes-scanned limits).

## 6. Non-functional considerations

- **Cost:** Glue DPU-hours dominate cost; Athena is pay-per-TB-scanned, so partitioning/columnar format directly controls spend.
- **Idempotency:** Glue bookmarking plus partitioned overwrite-by-partition semantics keep re-runs safe.
- **Security:** SSE-KMS on all three tiers; Glue and Athena access data only through an IAM role scoped to this bucket's prefixes (see LLD).

## 7. Infrastructure as Code (Terraform)

Every resource above — the KMS key, the bucket and its lifecycle rule, both Glue databases, the crawlers, the ETL job, the Athena workgroup, and the failure-alert wiring — is defined in [`infra/`](../infra/) rather than clicked together in the console, so the whole environment is reproducible and disposable.

```
infra/
├── versions.tf              # Terraform + AWS provider version pins
├── providers.tf              # provider config, default tags
├── variables.tf               # bucket_name, project_name, worker sizing, etc.
├── main.tf                     # all resources (see docs/LLD.md §9 for the full inventory)
├── outputs.tf                    # bucket name, job/crawler/workgroup names
├── terraform.tfvars.example       # copy to terraform.tfvars and fill in
└── modules/                        # vendored — kms_key, s3_data_lake_bucket, alerting
```

This project is its own standalone git repository, so the common building blocks (`kms_key`, `s3_data_lake_bucket`, `alerting`) live vendored under `infra/modules/` rather than referenced from a path outside the repo — the module code is identical across all four projects in the portfolio, just copied rather than shared, so each repo stays self-contained and clonable on its own.

**Workflow:**

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set a globally-unique bucket_name
terraform init
terraform plan
terraform apply
# ... download/upload trip data, run the crawler + ETL job, query in Athena ...
terraform destroy        # tears everything back down when you're done experimenting
```

State is kept **local** to this project's `infra/` folder (no remote backend) — appropriate for a single-developer personal project; add an S3 backend block to `versions.tf` if you want durable/shareable state later. See [`docs/LLD.md` §9](LLD.md#9-terraform-resource-inventory) for the full resource-to-file mapping and destroy-safety notes.
