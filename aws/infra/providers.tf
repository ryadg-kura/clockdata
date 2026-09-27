provider "aws" {
  region = var.aws_region

  # Credentials come from the standard AWS chain (AWS_PROFILE, SSO, env vars):
  # nothing is hardcoded here. offline_plan=true lets CI run `terraform plan`
  # with dummy credentials and no AWS account at all.
  skip_credentials_validation = var.offline_plan
  skip_requesting_account_id  = var.offline_plan
  skip_metadata_api_check     = var.offline_plan

  default_tags {
    tags = local.tags
  }
}
