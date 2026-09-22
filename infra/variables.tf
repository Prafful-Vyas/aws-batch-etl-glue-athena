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
