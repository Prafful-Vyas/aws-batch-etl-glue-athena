module "kms" {
  source      = "./modules/kms_key"
  name        = "${var.project_name}-key"
  description = "CMK for the NYC taxi batch ETL pipeline (S3 + Glue + Athena)."
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
        Sid      = "ReadRaw"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [module.data_lake.bucket_arn, "${module.data_lake.bucket_arn}/raw/*"]
      },
      {
        Sid      = "WriteProcessedAndTemp"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [module.data_lake.bucket_arn, "${module.data_lake.bucket_arn}/processed/*", "${module.data_lake.bucket_arn}/tmp/*", "${module.data_lake.bucket_arn}/scripts/*"]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions",
          "glue:BatchCreatePartition", "glue:CreateTable", "glue:UpdateTable"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:*:catalog",
          aws_glue_catalog_database.raw.arn,
          aws_glue_catalog_database.processed.arn,
          "arn:aws:glue:${var.aws_region}:*:table/${aws_glue_catalog_database.raw.name}/*",
          "arn:aws:glue:${var.aws_region}:*:table/${aws_glue_catalog_database.processed.name}/*"
        ]
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
        Resource = ["arn:aws:logs:${var.aws_region}:*:log-group:/aws-glue/*"]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Crawlers
# ---------------------------------------------------------------------------

resource "aws_glue_crawler" "raw" {
  name          = "${var.project_name}-raw-crawler"
  database_name = aws_glue_catalog_database.raw.name
  role          = aws_iam_role.glue_etl.arn

  s3_target {
    path = "s3://${module.data_lake.bucket_name}/raw/nyc-taxi/"
  }
}

resource "aws_glue_crawler" "processed" {
  name          = "${var.project_name}-processed-crawler"
  database_name = aws_glue_catalog_database.processed.name
  role          = aws_iam_role.glue_etl.arn

  s3_target {
    path = "s3://${module.data_lake.bucket_name}/processed/nyc-taxi/"
  }
}

# ---------------------------------------------------------------------------
# ETL job
# ---------------------------------------------------------------------------

resource "aws_s3_object" "etl_script" {
  bucket = module.data_lake.bucket_name
  key    = "scripts/clean_taxi_trips.py"
  source = "${path.module}/../glue_jobs/clean_taxi_trips.py"
  etag   = filemd5("${path.module}/../glue_jobs/clean_taxi_trips.py")
}

resource "aws_glue_job" "clean_taxi_trips" {
  name              = "${var.project_name}-clean-taxi-trips"
  role_arn          = aws_iam_role.glue_etl.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  command {
    name            = "glueetl"
    script_location = "s3://${module.data_lake.bucket_name}/${aws_s3_object.etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${module.data_lake.bucket_name}/tmp/glue/"
    "--source_database"                  = aws_glue_catalog_database.raw.name
    "--source_table"                     = "raw"
    "--target_path"                      = "s3://${module.data_lake.bucket_name}/processed/nyc-taxi/"
  }
}

# ---------------------------------------------------------------------------
# Athena
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "analytics" {
  name = "${var.project_name}-analytics"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${module.data_lake.bucket_name}/analytics/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = module.kms.key_arn
      }
    }
  }
}

# ---------------------------------------------------------------------------
# Failure alerting: Glue job state change -> EventBridge -> SNS
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

resource "aws_cloudwatch_event_target" "glue_job_failed_sns" {
  rule = aws_cloudwatch_event_rule.glue_job_failed.name
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
  }
}

resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn    = module.alerts.topic_arn
  policy = data.aws_iam_policy_document.sns_eventbridge.json
}
