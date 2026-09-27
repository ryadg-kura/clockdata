output "region" {
  description = "Region everything was deployed to."
  value       = var.aws_region
}

output "stream_name" {
  description = "Kinesis stream the simulator writes to."
  value       = aws_kinesis_stream.events.name
}

output "data_lake_bucket" {
  description = "S3 bucket holding bronze/, silver/, gold/ and quarantine/."
  value       = aws_s3_bucket.data_lake.id
}

output "athena_results_bucket" {
  description = "S3 bucket for Athena query results."
  value       = aws_s3_bucket.athena_results.id
}

output "glue_database" {
  description = "Glue database to select in Athena."
  value       = aws_glue_catalog_database.lake.name
}

output "athena_workgroup" {
  description = "Athena workgroup (enforces the result location and scan limit)."
  value       = aws_athena_workgroup.lake.name
}

output "processor_function" {
  description = "Stream processor Lambda."
  value       = aws_lambda_function.processor.function_name
}

output "aggregator_function" {
  description = "Silver -> Gold aggregator Lambda."
  value       = aws_lambda_function.aggregator.function_name
}

output "dlq_url" {
  description = "Dead-letter queue for failed batches."
  value       = aws_sqs_queue.processor_dlq.url
}

output "alerts_topic_arn" {
  description = "SNS topic receiving alarm notifications."
  value       = aws_sns_topic.alerts.arn
}

output "dashboard_url" {
  description = "CloudWatch dashboard."
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards/dashboard/${aws_cloudwatch_dashboard.pipeline.dashboard_name}"
}

output "athena_console_url" {
  description = "Athena query editor."
  value       = "https://${var.aws_region}.console.aws.amazon.com/athena/home?region=${var.aws_region}#/query-editor"
}

output "estimated_idle_cost" {
  description = "What this stack costs per hour even with no traffic (Kinesis shard)."
  value = (
    contains(keys(local.shard_hour_usd), var.aws_region)
    ? format("~$%.4f/hour (~$%.2f/day, ~$%.0f/month) for the Kinesis shard in %s. Run `make destroy` when done.",
      local.shard_hour_usd[var.aws_region], local.shard_hour_usd[var.aws_region] * 24,
    local.shard_hour_usd[var.aws_region] * 730, var.aws_region)
    : "See https://aws.amazon.com/kinesis/data-streams/pricing/ for ${var.aws_region}. Run `make destroy` when done."
  )
}
