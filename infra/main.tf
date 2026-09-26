module "kms" {
  source      = "./modules/kms_key"
  name        = "${var.project_name}-key"
  description = "CMK for the NYC taxi batch ETL pipeline (S3 + Glue + Athena)."
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  bucket     = module.data_lake.bucket_name

  # The raw crawler creates one table at raw/nyc-taxi/ (TableLevelConfiguration = 3),
  # and Glue names a crawled table after its folder.
  raw_table_name = "nyc_taxi"
  processed_path = "s3://${module.data_lake.bucket_name}/processed/nyc-taxi/"

  # Must match OUTPUT_COLUMNS in glue_jobs/clean_taxi_trips.py.
  processed_columns = [
    { name = "vendorid", type = "int" },
    { name = "pickup_datetime", type = "timestamp" },
    { name = "dropoff_datetime", type = "timestamp" },
    { name = "passenger_count", type = "int" },
    { name = "trip_distance", type = "double" },
    { name = "ratecodeid", type = "int" },
    { name = "pulocationid", type = "int" },
    { name = "dolocationid", type = "int" },
    { name = "payment_type", type = "int" },
    { name = "fare_amount", type = "double" },
    { name = "extra", type = "double" },
    { name = "mta_tax", type = "double" },
    { name = "tip_amount", type = "double" },
    { name = "tolls_amount", type = "double" },
    { name = "improvement_surcharge", type = "double" },
    { name = "congestion_surcharge", type = "double" },
    { name = "airport_fee", type = "double" },
    { name = "total_amount", type = "double" },
    { name = "trip_duration_min", type = "double" },
  ]
}

# The default key policy only trusts IAM principals in this account. EventBridge
# must also be able to use the key, or it cannot publish to the encrypted SNS topic.
data "aws_iam_policy_document" "kms" {
  #checkov:skip=CKV_AWS_109:Key policy - "*" means this key only; root statement delegates to IAM (AWS default)
  #checkov:skip=CKV_AWS_111:Key policy - "*" means this key only
  #checkov:skip=CKV_AWS_356:Key policy - "*" means this key only
  statement {
    sid       = "EnableIAMPolicies"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }
  }

  statement {
    sid       = "AllowEventBridgeForEncryptedSns"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_kms_key_policy" "this" {
  key_id = module.kms.key_id
  policy = data.aws_iam_policy_document.kms.json
}

module "data_lake" {
  source      = "./modules/s3_data_lake_bucket"
  bucket_name = var.bucket_name
  kms_key_arn = module.kms.key_arn

  lifecycle_rules = [
    {
      id              = "raw-to-glacier-ir"
      prefix          = "raw/"
      transition_days = 90
      storage_class   = "GLACIER_IR"
    }
  ]
}

module "alerts" {
  source      = "./modules/alerting"
  name        = "${var.project_name}-alerts"
  kms_key_arn = module.kms.key_arn
  alert_email = var.alert_email
}

# ---------------------------------------------------------------------------
# Glue Data Catalog
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "raw" {
  name = "nyc_taxi_raw"
}

resource "aws_glue_catalog_database" "processed" {
  name = "nyc_taxi_processed"
}

# ---------------------------------------------------------------------------
# IAM role shared by the crawlers and the ETL job
# ---------------------------------------------------------------------------

resource "aws_iam_role" "glue_etl" {
  name = "${var.project_name}-glue-etl-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_etl" {
  name = "${var.project_name}-glue-etl-policy"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [module.data_lake.bucket_arn]
      },
      {
        Sid      = "ReadRawAndScripts"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${module.data_lake.bucket_arn}/raw/*", "${module.data_lake.bucket_arn}/scripts/*"]
      },
      {
        Sid      = "WriteProcessedAndTemp"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${module.data_lake.bucket_arn}/processed/*", "${module.data_lake.bucket_arn}/tmp/*"]
      },
      {
        Sid    = "CrawlRawCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
          "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition", "glue:CreatePartition",
          "glue:BatchCreatePartition", "glue:UpdatePartition", "glue:BatchUpdatePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${local.account_id}:catalog",
          aws_glue_catalog_database.raw.arn,
          "arn:aws:glue:${var.aws_region}:${local.account_id}:table/${aws_glue_catalog_database.raw.name}/*"
        ]
      },
      {
        Sid      = "PublishDataQualityResults"
        Effect   = "Allow"
        Action   = ["glue:PublishDataQuality"]
        Resource = ["arn:aws:glue:${var.aws_region}:${local.account_id}:dataQualityRuleset/*"]
      },
      {
        # PutMetricData has no resource-level permissions; scope by namespace instead.
        Sid       = "JobAndDataQualityMetrics"
        Effect    = "Allow"
        Action    = ["cloudwatch:PutMetricData"]
        Resource  = ["*"]
        Condition = { StringLike = { "cloudwatch:namespace" = ["Glue", "Glue Data Quality"] } }
      },
      {
        Sid      = "KmsForData"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [module.kms.key_arn]
      },
      {
        Sid      = "Logging"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/*"]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Raw crawler: infers the raw schema and registers service/year/month partitions
# ---------------------------------------------------------------------------

resource "aws_glue_crawler" "raw" {
  #checkov:skip=CKV_AWS_195:Bucket default SSE-KMS encrypts all S3 output; logs/bookmarks hold no trip data
  name          = "${var.project_name}-raw-crawler"
  database_name = aws_glue_catalog_database.raw.name
  role          = aws_iam_role.glue_etl.arn

  s3_target {
    path = "s3://${local.bucket}/raw/nyc-taxi/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      # Yellow and green files differ slightly (tpep_* vs lpep_*); force a single
      # table at raw/nyc-taxi/ (bucket = level 1) instead of one per service.
      TableGroupingPolicy     = "CombineCompatibleSchemas"
      TableLevelConfiguration = 3
    }
    CrawlerOutput = {
      # Partitions inherit the table schema, so a month with a drifted column
      # type doesn't cause HIVE_PARTITION_SCHEMA_MISMATCH later.
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

# ---------------------------------------------------------------------------
# Processed + reference tables, owned by Terraform (no processed crawler).
# The ETL job writes a fixed schema, so the table is declared up front and uses
# partition projection: new months are queryable without MSCK REPAIR / crawls.
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_table" "trips_processed" {
  name          = "trips_processed"
  database_name = aws_glue_catalog_database.processed.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                    = "TRUE"
    classification              = "parquet"
    "parquet.compression"       = "SNAPPY"
    "projection.enabled"        = "true"
    "projection.service.type"   = "enum"
    "projection.service.values" = "yellow,green"
    "projection.year.type"      = "integer"
    "projection.year.range"     = var.projection_year_range
    "projection.month.type"     = "integer"
    "projection.month.range"    = "1,12"
    "storage.location.template" = "${local.processed_path}service=$${service}/year=$${year}/month=$${month}/"
  }

  partition_keys {
    name = "service"
    type = "string"
  }
  partition_keys {
    name = "year"
    type = "int"
  }
  partition_keys {
    name = "month"
    type = "int"
  }

  storage_descriptor {
    location      = local.processed_path
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.processed_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# TLC zone lookup (LocationID -> Borough/Zone), uploaded by scripts/load_trip_data.py.
resource "aws_glue_catalog_table" "taxi_zones" {
  name          = "taxi_zones"
  database_name = aws_glue_catalog_database.processed.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                 = "TRUE"
    classification           = "csv"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${local.bucket}/reference/taxi_zones/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
      parameters = {
        separatorChar = ","
        quoteChar     = "\""
      }
    }

    # OpenCSVSerde reads every field as a string; cast in queries.
    columns {
      name = "locationid"
      type = "string"
    }
    columns {
      name = "borough"
      type = "string"
    }
    columns {
      name = "zone"
      type = "string"
    }
    columns {
      name = "service_zone"
      type = "string"
    }
  }
}

# ---------------------------------------------------------------------------
# ETL job
# ---------------------------------------------------------------------------

resource "aws_s3_object" "etl_script" {
  bucket = local.bucket
  key    = "scripts/clean_taxi_trips.py"
  source = "${path.module}/../glue_jobs/clean_taxi_trips.py"
  etag   = filemd5("${path.module}/../glue_jobs/clean_taxi_trips.py")
}

resource "aws_glue_job" "clean_taxi_trips" {
  #checkov:skip=CKV_AWS_195:Bucket default SSE-KMS encrypts all S3 output; logs/bookmarks hold no trip data
  name              = "${var.project_name}-clean-taxi-trips"
  role_arn          = aws_iam_role.glue_etl.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  # A run that exceeds this ends in TIMEOUT, which the EventBridge rule below alerts on.
  timeout = var.glue_job_timeout_minutes

  execution_property {
    max_concurrent_runs = 1 # concurrent runs would race on the same bookmark
  }

  command {
    name            = "glueetl"
    script_location = "s3://${local.bucket}/${aws_s3_object.etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${local.bucket}/tmp/glue/"
    "--source_database"                  = aws_glue_catalog_database.raw.name
    "--source_table"                     = local.raw_table_name
    "--target_path"                      = local.processed_path
    "--dq_fail_on_error"                 = tostring(var.dq_fail_on_error)
  }
}

# ---------------------------------------------------------------------------
# Orchestration: workflow = raw crawler, then the ETL job once the crawl succeeds
# ---------------------------------------------------------------------------

resource "aws_glue_workflow" "pipeline" {
  name        = "${var.project_name}-pipeline"
  description = "Crawl raw/ then run the clean-taxi-trips ETL job."
}

resource "aws_glue_trigger" "start" {
  name              = "${var.project_name}-start"
  workflow_name     = aws_glue_workflow.pipeline.name
  type              = var.pipeline_schedule == "" ? "ON_DEMAND" : "SCHEDULED"
  schedule          = var.pipeline_schedule == "" ? null : var.pipeline_schedule
  start_on_creation = var.pipeline_schedule == "" ? null : true

  actions {
    crawler_name = aws_glue_crawler.raw.name
  }
}

resource "aws_glue_trigger" "run_etl" {
  name              = "${var.project_name}-run-etl"
  workflow_name     = aws_glue_workflow.pipeline.name
  type              = "CONDITIONAL"
  start_on_creation = true # conditional triggers do nothing until activated

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.raw.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.clean_taxi_trips.name
  }
}

# ---------------------------------------------------------------------------
# Athena
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "analytics" {
  name          = "${var.project_name}-analytics"
  force_destroy = true # lets `terraform destroy` remove it even with saved queries/history

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${local.bucket}/analytics/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = module.kms.key_arn
      }
    }
  }
}

# Every file in sql/ becomes a saved query in the workgroup.
resource "aws_athena_named_query" "this" {
  for_each = fileset("${path.module}/../sql", "*.sql")

  name      = trimsuffix(each.value, ".sql")
  workgroup = aws_athena_workgroup.analytics.id
  database  = aws_glue_catalog_database.processed.name
  query     = file("${path.module}/../sql/${each.value}")
}

# ---------------------------------------------------------------------------
# Failure alerting: Glue job / crawler state change -> EventBridge -> SNS
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "glue_job_failed" {
  name = "${var.project_name}-glue-job-failed"

  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.clean_taxi_trips.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

resource "aws_cloudwatch_event_rule" "glue_crawler_failed" {
  name = "${var.project_name}-glue-crawler-failed"

  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Crawler State Change"]
    detail = {
      crawlerName = [aws_glue_crawler.raw.name]
      state       = ["Failed"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_job_failed_sns" {
  rule = aws_cloudwatch_event_rule.glue_job_failed.name
  arn  = module.alerts.topic_arn
}

resource "aws_cloudwatch_event_target" "glue_crawler_failed_sns" {
  rule = aws_cloudwatch_event_rule.glue_crawler_failed.name
  arn  = module.alerts.topic_arn
}

data "aws_iam_policy_document" "sns_eventbridge" {
  statement {
    sid    = "AllowEventBridgePublish"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions   = ["SNS:Publish"]
    resources = [module.alerts.topic_arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [
        aws_cloudwatch_event_rule.glue_job_failed.arn,
        aws_cloudwatch_event_rule.glue_crawler_failed.arn,
      ]
    }
  }
}

resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn    = module.alerts.topic_arn
  policy = data.aws_iam_policy_document.sns_eventbridge.json
}
