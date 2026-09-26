variable "aws_region" {
  description = "Region of the infra/ stack (used to scope the deploy role)."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Must match project_name in infra/ — the deploy role is scoped to resources with this prefix."
  type        = string
  default     = "nyc-taxi-batch-etl"
}

variable "data_bucket_name" {
  description = "Must match bucket_name in infra/ (the data lake bucket the deploy role may manage)."
  type        = string
}

variable "state_bucket_name" {
  description = "Globally-unique name for the Terraform state bucket (can be shared by all portfolio projects)."
  type        = string
}

variable "state_key_prefix" {
  description = "Prefix within the state bucket that this project's CI may read/write."
  type        = string
  default     = "01-batch-etl-glue-athena"
}

variable "github_repository" {
  description = "GitHub repository allowed to assume the roles, as \"owner/name\"."
  type        = string
}

variable "deploy_branch" {
  description = "Branch whose pushes may run terraform plan (deploys additionally require the environment below)."
  type        = string
  default     = "main"
}

variable "deploy_environment" {
  description = "GitHub environment whose jobs may assume the deploy role. Add required reviewers to it in GitHub."
  type        = string
  default     = "production"
}

variable "create_oidc_provider" {
  description = "Create the GitHub OIDC provider. Set false if the account already has token.actions.githubusercontent.com."
  type        = bool
  default     = true
}
