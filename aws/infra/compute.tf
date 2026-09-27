# --------------------------------------------------------------------------
# Lambda functions. Code is zipped by Terraform itself (no build step);
# pyarrow comes from the AWS-managed "AWS SDK for pandas" layer.
# --------------------------------------------------------------------------

data "archive_file" "processor" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/processor"
  output_path = "${path.module}/.build/processor.zip"
  excludes    = ["__pycache__/**", "**/*.pyc"]
}

data "archive_file" "aggregator" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/aggregator"
  output_path = "${path.module}/.build/aggregator.zip"
  excludes    = ["__pycache__/**", "**/*.pyc"]
}

# Log groups are created by Terraform (not implicitly by Lambda) so they get a
# retention period and are deleted by `make destroy`.
resource "aws_cloudwatch_log_group" "processor" {
  name              = "/aws/lambda/${local.name}-processor"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "aggregator" {
  name              = "/aws/lambda/${local.name}-aggregator"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "processor" {
  function_name    = "${local.name}-processor"
  description      = "Kinesis -> validate/enrich -> S3 bronze (JSON) / silver (Parquet) / quarantine"
  role             = aws_iam_role.processor.arn
  runtime          = local.lambda_runtime
  architectures    = [local.lambda_architecture]
  handler          = "handler.handler"
  filename         = data.archive_file.processor.output_path
  source_code_hash = data.archive_file.processor.output_base64sha256
  layers           = [local.pyarrow_layer_arn]
  memory_size      = 512 # pyarrow import is faster with more (proportional) CPU
  timeout          = 60

  # No reserved concurrency on purpose: new AWS accounts often have a total
  # concurrency quota of 10, and reserving any of it makes the deploy fail.

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.processor.name
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET  = aws_s3_bucket.data_lake.id
      HR_LOW_THRESHOLD  = tostring(var.hr_low_threshold)
      HR_HIGH_THRESHOLD = tostring(var.hr_high_threshold)
      METRIC_NAMESPACE  = local.metric_namespace
      PIPELINE_NAME     = local.name
    }
  }

  depends_on = [aws_iam_role_policy.processor]
}

resource "aws_lambda_event_source_mapping" "kinesis_to_processor" {
  function_name                      = aws_lambda_function.processor.arn
  event_source_arn                   = aws_kinesis_stream.events.arn
  starting_position                  = "TRIM_HORIZON" # do not lose events sent right after deploy
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.batching_window_seconds
  parallelization_factor             = 1

  # Poison-batch handling: retry, split the batch in two to isolate the bad
  # record, then give up and send the batch metadata to the DLQ so the shard
  # keeps moving.
  maximum_retry_attempts         = 3
  bisect_batch_on_function_error = true
  maximum_record_age_in_seconds  = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.processor_dlq.arn
    }
  }

  depends_on = [aws_iam_role_policy.processor]
}

resource "aws_lambda_function" "aggregator" {
  function_name    = "${local.name}-aggregator"
  description      = "S3 silver -> per device per hour metrics -> S3 gold (Parquet)"
  role             = aws_iam_role.aggregator.arn
  runtime          = local.lambda_runtime
  architectures    = [local.lambda_architecture]
  handler          = "handler.handler"
  filename         = data.archive_file.aggregator.output_path
  source_code_hash = data.archive_file.aggregator.output_base64sha256
  layers           = [local.pyarrow_layer_arn]
  memory_size      = 512
  timeout          = 120

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.aggregator.name
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = aws_s3_bucket.data_lake.id
    }
  }

  depends_on = [aws_iam_role_policy.aggregator]
}

# Re-aggregates the current and previous hour (idempotent overwrite).
resource "aws_cloudwatch_event_rule" "aggregation" {
  name                = "${local.name}-aggregation"
  description         = "Refresh gold hourly metrics"
  schedule_expression = var.aggregation_schedule
}

resource "aws_cloudwatch_event_target" "aggregation" {
  rule  = aws_cloudwatch_event_rule.aggregation.name
  arn   = aws_lambda_function.aggregator.arn
  input = jsonencode({ hours_back = 2 })
}

resource "aws_lambda_permission" "aggregation_schedule" {
  statement_id  = "AllowEventBridgeSchedule"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aggregator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.aggregation.arn
}
