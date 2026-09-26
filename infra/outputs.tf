output "bucket_name" {
  value = module.data_lake.bucket_name
}

output "glue_job_name" {
  value = aws_glue_job.clean_taxi_trips.name
}

output "raw_crawler_name" {
  value = aws_glue_crawler.raw.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.analytics.name
}

output "sns_topic_arn" {
  value = module.alerts.topic_arn
}

output "kms_key_arn" {
  value = module.kms.key_arn
}

output "aws_region" {
  value = var.aws_region
}

output "glue_workflow_name" {
  value = aws_glue_workflow.pipeline.name
}

output "processed_table" {
  value = "${aws_glue_catalog_database.processed.name}.${aws_glue_catalog_table.trips_processed.name}"
}
