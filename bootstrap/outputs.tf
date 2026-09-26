# Copy these into the GitHub repository's Actions variables (Settings -> Secrets and variables -> Actions -> Variables).

output "AWS_PLAN_ROLE_ARN" {
  value = aws_iam_role.plan.arn
}

output "AWS_DEPLOY_ROLE_ARN" {
  value = aws_iam_role.deploy.arn
}

output "TF_STATE_BUCKET" {
  value = aws_s3_bucket.tf_state.bucket
}

output "AWS_REGION" {
  value = var.aws_region
}

output "DATA_BUCKET_NAME" {
  value = var.data_bucket_name
}
