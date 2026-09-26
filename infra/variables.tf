variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short name used to prefix/tag all resources."
  type        = string
  default     = "nyc-taxi-batch-etl"
}

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for the raw/processed/analytics tiers. Must be changed from the example before applying."
  type        = string
}

variable "glue_worker_type" {
  description = "Glue worker type for the ETL job."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers for the ETL job."
  type        = number
  default     = 5
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query bytes-scanned limit for the Athena workgroup (cost guardrail)."
  type        = number
  default     = 5000000000 # 5 GB
}

variable "alert_email" {
  description = "Optional email to subscribe to the failure-alert SNS topic."
  type        = string
  default     = ""
}

variable "glue_job_timeout_minutes" {
  description = "Glue job timeout. A run that exceeds it ends in TIMEOUT and triggers the failure alert."
  type        = number
  default     = 60
}

variable "dq_fail_on_error" {
  description = "Fail the ETL job (and skip the write) when any Data Quality rule fails. false = log and publish results only."
  type        = bool
  default     = true
}

variable "pipeline_schedule" {
  description = "Glue cron expression for the crawler -> ETL workflow, e.g. \"cron(0 6 25 * ? *)\" (TLC publishes monthly with a ~2 month lag). Empty = on-demand only."
  type        = string
  default     = ""
}

variable "projection_year_range" {
  description = "Partition projection range for the processed table's year partition, as \"min,max\"."
  type        = string
  default     = "2019,2030"
}
