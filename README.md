# Project 1 — Batch ETL Pipeline: S3 + Glue + Athena

**Domain:** Urban transportation / mobility analytics
**Dataset:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — monthly Yellow/Green taxi and For-Hire-Vehicle trip records, published by NYC's Taxi & Limousine Commission. Files are Parquet, millions of rows/month, free, no registration required. Direct download pattern: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`.

## Why this domain

Trip/transactional-event data is the same shape as ride-share, delivery, and logistics data used across the industry (Uber, Lyft, DoorDash, freight). Mobility and logistics analytics is a consistently hiring domain, and this dataset is large enough (tens of millions of rows/year) to force real decisions about partitioning, file sizing, and query cost — not a toy CSV.

## What this project builds

A serverless batch pipeline: raw monthly Parquet → Glue Crawler infers schema → Glue PySpark ETL job cleans and repartitions to a curated Parquet layer → Athena queries the curated layer directly from S3.

See [`docs/HLD.md`](docs/HLD.md) for architecture and [`docs/LLD.md`](docs/LLD.md) for bucket layout, IAM policies, Glue job parameters, DQ rules, and Athena optimization details.

## Deploying with Terraform

All infrastructure (S3 bucket, KMS key, Glue databases/crawlers/job, Athena workgroup, failure alerting) is defined in [`infra/`](infra/). See [`docs/LLD.md` §9](docs/LLD.md#9-terraform-resource-inventory) for the full resource inventory.

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set a globally-unique bucket_name
terraform init
terraform plan
terraform apply
```

Then, once the infrastructure exists:

```bash
# 1. Download a couple of months of source data
curl -o yellow_tripdata_2024-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

# 2. Upload to the raw tier (bucket name is a terraform output)
aws s3 cp yellow_tripdata_2024-01.parquet s3://<bucket_name>/raw/nyc-taxi/service=yellow/year=2024/month=01/

# 3. Run the Glue Crawler, then the Glue ETL job
aws glue start-crawler --name <raw_crawler_name>
aws glue start-job-run --job-name <glue_job_name>

# 4. Query in Athena, using the workgroup Terraform created
```

**Tear down** when you're done experimenting — the bucket defaults to `force_destroy = true` for easy cleanup:

```bash
cd infra
terraform destroy
```

## Folder layout

```
01-batch-etl-glue-athena/            # this folder is its own git repo
├── .gitignore
├── README.md
├── docs/
│   ├── HLD.md
│   └── LLD.md
├── glue_jobs/
│   └── clean_taxi_trips.py
└── infra/                          # Terraform: S3, KMS, Glue, Athena, alerting
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    ├── terraform.tfvars.example
    └── modules/                      # vendored — kms_key, s3_data_lake_bucket, alerting
```
