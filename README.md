# Project 1 — Batch ETL Pipeline: S3 + Glue + Athena

**Domain:** Urban transportation / mobility analytics
**Dataset:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — monthly Yellow/Green taxi trip records, published by NYC's Taxi & Limousine Commission. Files are Parquet, millions of rows/month, free, no registration required. Direct download pattern: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`.

## Why this domain

Trip/transactional-event data is the same shape as ride-share, delivery, and logistics data used across the industry (Uber, Lyft, DoorDash, freight). Mobility and logistics analytics is a consistently hiring domain, and this dataset is large enough (tens of millions of rows/year) to force real decisions about partitioning, file sizing, and query cost — not a toy CSV.

## What this project builds

A serverless batch pipeline:

```
raw Parquet (S3) ──▶ Glue Crawler ──▶ Glue PySpark job ──▶ processed Parquet (S3) ──▶ Athena
                     (schema +         (clean, dedupe,      service/year/month       (partition
                      partitions)       DQ gate, bookmark)   partitions               projection)
```

- **Glue Workflow** chains the raw crawler → ETL job (on demand or on a monthly schedule).
- **Job bookmarks** make each run incremental; **dynamic partition overwrite** makes re-runs idempotent.
- **Glue Data Quality** rules gate every batch — a failing batch is not written and the job fails.
- **Partition projection** on the Terraform-owned `trips_processed` table: new months are queryable with no crawl or `MSCK REPAIR`.
- **EventBridge → SNS (KMS-encrypted)** alerts on job/crawler failure or timeout.
- **Athena workgroup** with a bytes-scanned cap, SSE-KMS results, and saved queries from [`sql/`](sql/).

See [`docs/HLD.md`](docs/HLD.md) for architecture and [`docs/LLD.md`](docs/LLD.md) for bucket layout, IAM, job parameters, DQ rules, and Athena details.

## Prerequisites

- Terraform ≥ 1.5, AWS credentials for the target account
- Python 3.10+ with `pip install -r requirements.txt` (boto3, used by the helper scripts)

## Run it end to end

```bash
# 1. Infrastructure
cd infra
cp terraform.tfvars.example terraform.tfvars   # set a globally-unique bucket_name (+ alert_email)
terraform init
terraform apply
cd ..
# If you set alert_email, confirm the SNS subscription email.

# 2. Land source data: trip files for a few months + the taxi-zone lookup
python scripts/load_trip_data.py --bucket <bucket_name> --service yellow --months 2024-01 2024-02 2024-03
python scripts/load_trip_data.py --bucket <bucket_name> --service green  --months 2024-01

# 3. Run the pipeline (Glue workflow: raw crawler -> ETL job) and wait for it
python scripts/run_pipeline.py

# 4. Query: Athena console -> workgroup "nyc-taxi-batch-etl-analytics" -> Saved queries
```

The saved queries (one per file in [`sql/`](sql/)) cover:

| Query | Question |
|---|---|
| `01_trips_by_service_month` | Trips and avg fare per service — shows partition pruning |
| `02_peak_demand_by_hour` | Peak demand by day of week and hour |
| `03_top_pickup_zones` | Busiest pickup zones (joins the zone lookup) |
| `04_fare_tip_by_borough_month` | Avg fare and card tip % by borough and month |
| `05_volume_trend_by_vendor` | Monthly volume trend by service/vendor |
| `06_create_daily_trip_summary_view` | Creates a `daily_trip_summary` view for dashboards |

**Incremental loads:** upload another month and run `run_pipeline.py` again — the bookmark means only the new file is processed. Re-uploading a month replaces just that month's processed partition.

**Scheduling:** set `pipeline_schedule = "cron(0 6 25 * ? *)"` in `terraform.tfvars` to run monthly (you still need something to land new files — `load_trip_data.py` from cron, or extend with a Lambda).

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest
```

The transform tests run PySpark locally, so they need a JDK (11 or 17) and Python 3.10/3.11 to match PySpark 3.3 (the Glue 4.0 runtime); without pyspark they're skipped.

## CI/CD (GitHub Actions)

| Workflow | Trigger | What it does |
|---|---|---|
| [`ci.yml`](.github/workflows/ci.yml) | PRs, pushes to `main`, called by `deploy` | ruff lint/format, pytest (with JDK), `terraform fmt`/`validate` + tflint on `infra/` and `bootstrap/`, Checkov security scan |
| [`terraform-plan.yml`](.github/workflows/terraform-plan.yml) | PRs touching infra/job/SQL | `terraform plan` with a **read-only** role; posts the plan as a PR comment |
| [`deploy.yml`](.github/workflows/deploy.yml) | push to `main`, manual | CI → plan (read-only role) → **apply that saved plan** in the `production` environment → smoke check |
| [`run-pipeline.yml`](.github/workflows/run-pipeline.yml) | manual | Land chosen TLC months in `raw/` and run the Glue workflow to completion |
| [`destroy.yml`](.github/workflows/destroy.yml) | manual, type `destroy` to confirm | `terraform destroy` |

AWS access uses GitHub **OIDC**, so no long-lived keys are stored in GitHub. CI keeps Terraform state in S3 by generating a `backend_override.tf` at runtime ([`.github/actions/terraform-init`](.github/actions/terraform-init/action.yml)), so local runs keep using local state. Dependabot keeps actions, pip and providers current. Accepted Checkov findings, with reasons, are in [`.checkov.yaml`](.checkov.yaml) and inline `#checkov:skip` comments.

**One-time setup:**

1. Create the CI prerequisites with the [`bootstrap/`](bootstrap/) stack: state bucket, OIDC provider, `*-github-plan` role (ReadOnlyAccess + state) and `*-github-deploy` role (scoped to this project's resource names; assumable only from the `production` environment). It runs locally with local state:
   ```bash
   cd bootstrap
   cp terraform.tfvars.example terraform.tfvars   # repo, bucket names; create_oidc_provider=false if the account already has it
   terraform init && terraform apply
   ```
2. In GitHub, go to **Settings → Environments** and create `production`. Add yourself as a required reviewer so every apply, pipeline run and destroy waits for approval.
3. Under **Settings → Secrets and variables → Actions → Variables**, add the bootstrap outputs: `AWS_REGION`, `AWS_PLAN_ROLE_ARN`, `AWS_DEPLOY_ROLE_ARN`, `TF_STATE_BUCKET`, `DATA_BUCKET_NAME`, plus the optional `ALERT_EMAIL` and `PIPELINE_SCHEDULE`. Until these are set, the AWS jobs are skipped and CI still runs.
4. If you've already applied `infra/` locally, move its state to S3 so CI manages the same resources. Add a temporary `backend_override.tf` with `terraform { backend "s3" {} }`, then run `terraform init -migrate-state -backend-config=bucket=<TF_STATE_BUCKET> -backend-config=key=01-batch-etl-glue-athena/terraform.tfstate -backend-config=region=<region> -backend-config=use_lockfile=true`. (CI uses Terraform 1.11 for S3-native locking.)
5. Workflows run on `main`. If your local branch is `master`, rename it before pushing: `git branch -M main`.

## Tear down

The bucket defaults to `force_destroy = true`, so this deletes all landed and processed data too:

```bash
cd infra
terraform destroy
```

## Folder layout

```
01-batch-etl-glue-athena/            # this folder is its own git repo
├── README.md
├── requirements.txt                 # boto3 for scripts/
├── requirements-dev.txt             # + pyspark, pytest
├── docs/
│   ├── HLD.md
│   └── LLD.md
├── glue_jobs/
│   └── clean_taxi_trips.py          # Glue job; transform logic is plain PySpark (testable)
├── scripts/
│   ├── load_trip_data.py            # download TLC files -> raw/ (+ zone lookup)
│   └── run_pipeline.py              # start the Glue workflow and wait
├── sql/                             # Athena queries, deployed as saved queries
├── tests/                           # pytest: transform + script helpers
├── .github/
│   ├── workflows/                   # ci, terraform-plan, deploy, run-pipeline, destroy
│   ├── actions/terraform-init/      # shared: install TF + init against S3 backend
│   └── dependabot.yml
├── .checkov.yaml  .tflint.hcl  ruff.toml  pytest.ini
├── bootstrap/                       # one-time: TF state bucket + GitHub OIDC roles
└── infra/                           # Terraform: S3, KMS, Glue, Athena, alerting
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    ├── terraform.tfvars.example
    └── modules/                     # vendored — kms_key, s3_data_lake_bucket, alerting
```
