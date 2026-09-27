locals {
  name = "${var.project}-${var.environment}"

  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  lambda_runtime      = "python3.13"
  lambda_architecture = "arm64" # Graviton: ~20% cheaper per GB-second than x86

  # AWS publishes this layer from account 336392948345 in every region.
  pyarrow_layer_arn = coalesce(
    var.pyarrow_layer_arn,
    "arn:aws:lambda:${var.aws_region}:336392948345:layer:AWSSDKPandas-Python313-Arm64:${var.pandas_layer_version}",
  )

  metric_namespace = "ClockData"

  # On-demand price of one provisioned shard-hour (AWS Price List API, 2026-09).
  shard_hour_usd = {
    "us-east-1" = 0.015
    "eu-west-1" = 0.017
    "eu-west-3" = 0.0179
  }

  # Table schemas are shared with the Lambda code (single source of truth).
  silver_columns = jsondecode(file("${path.module}/../lambdas/processor/schema_silver.json"))
  gold_columns   = jsondecode(file("${path.module}/../lambdas/aggregator/schema_gold.json"))
}

# Random suffix: S3 bucket names are global across all AWS accounts.
resource "random_id" "suffix" {
  byte_length = 3
}
