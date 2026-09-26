# One-time, run-locally stack for CI/CD prerequisites:
#   - S3 bucket for Terraform remote state (shared by CI runs; locking via S3 lockfile)
#   - GitHub Actions OIDC identity provider (optional; one per AWS account)
#   - "plan" role: read-only, assumable from pull requests and the main branch
#   - "deploy" role: can manage this project's resources, assumable only from the
#     GitHub "production" environment (so environment protection rules gate it)
#
# State for this stack stays local (bootstrap/terraform.tfstate, gitignored).

data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  oidc_host   = "token.actions.githubusercontent.com"
  oidc_arn    = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:aws:iam::${local.account_id}:oidc-provider/${local.oidc_host}"
  repo_prefix = "repo:${var.github_repository}"
}

# ---------------------------------------------------------------------------
# Terraform state bucket
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "tf_state" {
  #checkov:skip=CKV_AWS_145:SSE-S3 on purpose, so CI roles need no KMS grants to read state
  bucket = var.state_bucket_name
}

resource "aws_s3_bucket_lifecycle_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days           = 90
      newer_noncurrent_versions = 10 # always keep the last 10 versions for rollback
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket                  = aws_s3_bucket.tf_state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "tf_state_tls_only" {
  bucket = aws_s3_bucket.tf_state.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.tf_state.arn, "${aws_s3_bucket.tf_state.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# ---------------------------------------------------------------------------
# GitHub OIDC
# ---------------------------------------------------------------------------

resource "aws_iam_openid_connect_provider" "github" {
  count           = var.create_oidc_provider ? 1 : 0
  url             = "https://${local.oidc_host}"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1", "1c58a3a8518e8759bf075b76b750d4f2df264fcd"]
}

data "aws_iam_policy_document" "state_access" {
  statement {
    sid       = "ListStateBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.tf_state.arn]
  }
  statement {
    # Put/Delete are needed by plan too: S3-native locking writes a .tflock object.
    sid       = "ReadWriteProjectState"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.tf_state.arn}/${var.state_key_prefix}/*"]
  }
}

# --- plan role ---------------------------------------------------------------

data "aws_iam_policy_document" "plan_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "${local.oidc_host}:sub"
      values = [
        "${local.repo_prefix}:pull_request",
        "${local.repo_prefix}:ref:refs/heads/${var.deploy_branch}",
      ]
    }
  }
}

resource "aws_iam_role" "plan" {
  name                 = "${var.project_name}-github-plan"
  assume_role_policy   = data.aws_iam_policy_document.plan_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "plan_readonly" {
  role       = aws_iam_role.plan.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_iam_role_policy" "plan_state" {
  name   = "terraform-state"
  role   = aws_iam_role.plan.id
  policy = data.aws_iam_policy_document.state_access.json
}

# --- deploy role -------------------------------------------------------------

data "aws_iam_policy_document" "deploy_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:sub"
      values   = ["${local.repo_prefix}:environment:${var.deploy_environment}"]
    }
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "${var.project_name}-github-deploy"
  assume_role_policy   = data.aws_iam_policy_document.deploy_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "deploy_state" {
  name   = "terraform-state"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.state_access.json
}

# Scoped to the names infra/ creates (all prefixed with project_name, plus the
# data bucket and the fixed Glue database names).
data "aws_iam_policy_document" "deploy" {
  #checkov:skip=CKV_AWS_109:kms:* on "*" - CreateKey is not resource-scopable (see Kms statement)
  #checkov:skip=CKV_AWS_111:kms:* on "*" - CreateKey is not resource-scopable (see Kms statement)
  #checkov:skip=CKV_AWS_356:kms:* on "*" - CreateKey is not resource-scopable (see Kms statement)
  statement {
    sid     = "DataBucket"
    actions = ["s3:*"]
    resources = [
      "arn:aws:s3:::${var.data_bucket_name}",
      "arn:aws:s3:::${var.data_bucket_name}/*",
    ]
  }

  statement {
    # kms:CreateKey has no resource ARN to scope to, and the key ID is generated,
    # so KMS is the one service granted account-wide. Tighten with aws:ResourceTag
    # conditions (Project = project_name) if this role is shared.
    sid       = "Kms"
    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid     = "Glue"
    actions = ["glue:*"]
    resources = [
      "arn:aws:glue:${var.aws_region}:${local.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:database/nyc_taxi_*",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:table/nyc_taxi_*/*",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:userDefinedFunction/nyc_taxi_*/*",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:crawler/${var.project_name}-*",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:job/${var.project_name}-*",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:workflow/${var.project_name}-*",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:trigger/${var.project_name}-*",
    ]
  }

  statement {
    sid       = "Athena"
    actions   = ["athena:*"]
    resources = ["arn:aws:athena:${var.aws_region}:${local.account_id}:workgroup/${var.project_name}-*"]
  }

  statement {
    sid       = "Sns"
    actions   = ["sns:*"]
    resources = ["arn:aws:sns:${var.aws_region}:${local.account_id}:${var.project_name}-*"]
  }

  statement {
    sid       = "EventBridge"
    actions   = ["events:*"]
    resources = ["arn:aws:events:${var.aws_region}:${local.account_id}:rule/${var.project_name}-*"]
  }

  statement {
    sid = "IamProjectRoles"
    actions = [
      "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:UpdateRole", "iam:TagRole", "iam:UntagRole",
      "iam:UpdateAssumeRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole", "iam:PutRolePolicy", "iam:GetRolePolicy", "iam:DeleteRolePolicy",
    ]
    # Excludes this stack's own *-github-* roles via the Glue-role naming below.
    resources = ["arn:aws:iam::${local.account_id}:role/${var.project_name}-glue-*"]
  }

  statement {
    sid       = "PassGlueRole"
    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::${local.account_id}:role/${var.project_name}-glue-*"]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "deploy-${var.project_name}"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}
