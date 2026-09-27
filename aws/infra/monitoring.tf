# --------------------------------------------------------------------------
# Alarms (the free tier covers 10) -> SNS topic
# --------------------------------------------------------------------------

locals {
  pipeline_dimension = { Pipeline = local.name }
  alarm_actions      = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "abnormal_heart_rate" {
  alarm_name          = "${local.name}-abnormal-heart-rate"
  alarm_description   = "At least one event with heart rate < ${var.hr_low_threshold} or > ${var.hr_high_threshold} BPM in the last minute. Details: Logs Insights, filter type = 'ABNORMAL_HEART_RATE'."
  namespace           = local.metric_namespace
  metric_name         = "AbnormalHeartRate"
  dimensions          = local.pipeline_dimension
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "processor_errors" {
  alarm_name          = "${local.name}-processor-errors"
  alarm_description   = "The stream processor Lambda is failing (batches are retried, then sent to the DLQ)."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.processor.function_name }
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${local.name}-processor-lagging"
  alarm_description   = "The processor is more than 60 s behind the head of the stream."
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  dimensions          = { FunctionName = aws_lambda_function.processor.function_name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 60000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${local.name}-dlq-not-empty"
  alarm_description   = "Batches landed in the dead-letter queue: data needs to be replayed from Kinesis."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.processor_dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "aggregator_errors" {
  alarm_name          = "${local.name}-aggregator-errors"
  alarm_description   = "The Silver -> Gold aggregation failed."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.aggregator.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

# --------------------------------------------------------------------------
# Dashboard (the free tier covers 3 dashboards of up to 50 metrics)
# --------------------------------------------------------------------------

locals {
  processor_fn  = aws_lambda_function.processor.function_name
  aggregator_fn = aws_lambda_function.aggregator.function_name
  stream        = aws_kinesis_stream.events.name

  dashboard_widgets = [
    {
      type = "text", x = 0, y = 0, width = 24, height = 2
      properties = {
        markdown = "## ClockData pipeline (${local.name})\nWatches → Kinesis `${local.stream}` → Lambda `${local.processor_fn}` → S3 bronze/silver → Lambda `${local.aggregator_fn}` → S3 gold → Athena. **Run `make destroy` after the demo.**"
      }
    },
    {
      type = "metric", x = 0, y = 2, width = 8, height = 6
      properties = {
        title  = "Throughput: records into Kinesis / min"
        region = var.aws_region, view = "timeSeries", stat = "Sum", period = 60
        metrics = [
          ["AWS/Kinesis", "IncomingRecords", "StreamName", local.stream, { label = "Incoming records" }],
          [".", "GetRecords.Records", ".", ".", { label = "Read by Lambda" }],
        ]
      }
    },
    {
      type = "metric", x = 8, y = 2, width = 8, height = 6
      properties = {
        title  = "Events: valid / rejected / abnormal HR"
        region = var.aws_region, view = "timeSeries", stat = "Sum", period = 60
        metrics = [
          [local.metric_namespace, "ValidEvents", "Pipeline", local.name, { color = "#2ca02c" }],
          [".", "InvalidEvents", ".", ".", { color = "#ff7f0e" }],
          [".", "AbnormalHeartRate", ".", ".", { color = "#d62728" }],
        ]
      }
    },
    {
      type = "alarm", x = 16, y = 2, width = 8, height = 6
      properties = {
        title = "Alarms"
        alarms = [
          aws_cloudwatch_metric_alarm.abnormal_heart_rate.arn,
          aws_cloudwatch_metric_alarm.processor_errors.arn,
          aws_cloudwatch_metric_alarm.iterator_age.arn,
          aws_cloudwatch_metric_alarm.dlq_not_empty.arn,
          aws_cloudwatch_metric_alarm.aggregator_errors.arn,
        ]
      }
    },
    {
      type = "metric", x = 0, y = 8, width = 8, height = 6
      properties = {
        title  = "Latency: event → processed (ms)"
        region = var.aws_region, view = "timeSeries", period = 60
        metrics = [
          [local.metric_namespace, "EndToEndLatency", "Pipeline", local.name, { stat = "p50", label = "p50" }],
          ["...", { stat = "p99", label = "p99" }],
          ["AWS/Lambda", "IteratorAge", "FunctionName", local.processor_fn, { stat = "Maximum", label = "Iterator age (max)" }],
        ]
      }
    },
    {
      type = "metric", x = 8, y = 8, width = 8, height = 6
      properties = {
        title  = "Lambda: invocations / errors / throttles"
        region = var.aws_region, view = "timeSeries", stat = "Sum", period = 60
        metrics = [
          ["AWS/Lambda", "Invocations", "FunctionName", local.processor_fn, { label = "processor invocations" }],
          [".", "Errors", ".", ".", { label = "processor errors", color = "#d62728" }],
          [".", "Throttles", ".", ".", { label = "processor throttles" }],
          [".", "Errors", ".", local.aggregator_fn, { label = "aggregator errors" }],
        ]
      }
    },
    {
      type = "metric", x = 16, y = 8, width = 8, height = 6
      properties = {
        title  = "Lambda duration (ms) & DLQ depth"
        region = var.aws_region, view = "timeSeries", period = 60
        metrics = [
          ["AWS/Lambda", "Duration", "FunctionName", local.processor_fn, { stat = "Average", label = "processor avg" }],
          ["...", { stat = "Maximum", label = "processor max" }],
          ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.processor_dlq.name, { stat = "Maximum", label = "DLQ messages", yAxis = "right" }],
        ]
      }
    },
  ]
}

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = local.name
  dashboard_body = jsonencode({ widgets = local.dashboard_widgets })
}
