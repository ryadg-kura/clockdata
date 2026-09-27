# --------------------------------------------------------------------------
# Ingestion: Kinesis Data Streams, 1 provisioned shard
# COST: a provisioned shard is billed every hour it exists, even with zero
# traffic (eu-west-3: $0.0179/shard-hour ~ $0.43/day ~ $13/month).
# Run `make destroy` after each demo.
# --------------------------------------------------------------------------

resource "aws_kinesis_stream" "events" {
  name             = "${local.name}-events"
  retention_period = var.kinesis_retention_hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
  shard_count = 1 # 1 MB/s or 1,000 records/s in, 2 MB/s out

  # Server-side encryption with the AWS-managed key (no key to create or pay).
  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  # Shard-level (enhanced) metrics are billed as custom metrics, so they are
  # left disabled; stream-level metrics are free.
}

# --------------------------------------------------------------------------
# Dead-letter queue: metadata of batches the processor could not handle after
# all retries (shard id + sequence number range, to replay from Kinesis).
# --------------------------------------------------------------------------

resource "aws_sqs_queue" "processor_dlq" {
  name                      = "${local.name}-processor-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum
  sqs_managed_sse_enabled   = true
}

# --------------------------------------------------------------------------
# Alert fan-out: CloudWatch alarms -> SNS -> (optional) e-mail
# --------------------------------------------------------------------------

# The default topic policy already lets same-account CloudWatch alarms publish.
resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "alert_email" {
  count = var.alert_email == "" ? 0 : 1

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
