output "bucket_name" {
  value = module.data_lake.bucket_name
}

output "glue_job_name" {
  value = aws_glue_job.clean_taxi_trips.name
}

output "raw_crawler_name" {
  value = aws_glue_crawler.raw.name
}

output "processed_crawler_name" {
  value = aws_glue_crawler.processed.name
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
