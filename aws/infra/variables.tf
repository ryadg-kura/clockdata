variable "aws_region" {
  description = "AWS region to deploy into. eu-west-3 = Paris."
  type        = string
  default     = "eu-west-3"
}

variable "project" {
  description = "Project name, used as a prefix for every resource."
  type        = string
  default     = "clockdata"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.project))
    error_message = "project must be lowercase letters, digits and dashes (max 21 chars)."
  }
}

variable "environment" {
  description = "Environment name (dev, demo, ...)."
  type        = string
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z0-9]{1,10}$", var.environment))
    error_message = "environment must be lowercase letters/digits (max 10 chars)."
  }
}

variable "alert_email" {
  description = "Optional e-mail subscribed to alarms (you must click the confirmation link AWS sends). Empty = no subscription."
  type        = string
  default     = ""
}

variable "hr_low_threshold" {
  description = "Heart rate (BPM) under which an event is flagged abnormal (bradycardia)."
  type        = number
  default     = 40
}

variable "hr_high_threshold" {
  description = "Heart rate (BPM) above which an event is flagged abnormal (tachycardia)."
  type        = number
  default     = 180

  validation {
    condition     = var.hr_high_threshold > 100
    error_message = "hr_high_threshold must be above 100 BPM."
  }
}

variable "kinesis_retention_hours" {
  description = "Kinesis retention. 24h is the included minimum; anything above costs extra."
  type        = number
  default     = 24
}

variable "data_retention_days" {
  description = "S3 lifecycle: objects of the data lake are deleted after this many days (cost safety)."
  type        = number
  default     = 30
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the Lambda functions."
  type        = number
  default     = 7
}

variable "batch_size" {
  description = "Max Kinesis records per Lambda invocation."
  type        = number
  default     = 100
}

variable "batching_window_seconds" {
  description = "How long Lambda waits to fill a batch. Higher = fewer invocations, more latency."
  type        = number
  default     = 5
}

variable "aggregation_schedule" {
  description = "EventBridge schedule of the Silver -> Gold aggregator."
  type        = string
  default     = "rate(15 minutes)"
}

variable "athena_bytes_scanned_cutoff" {
  description = "Athena refuses queries scanning more than this (bytes). 1 GB = at most $0.005 per query."
  type        = number
  default     = 1073741824
}

variable "pandas_layer_version" {
  description = "Version of the AWS-managed 'AWS SDK for pandas' layer (provides pyarrow). See https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html"
  type        = number
  default     = 16
}

variable "pyarrow_layer_arn" {
  description = "Override the full ARN of the layer providing pyarrow (leave empty to use the AWS SDK for pandas layer)."
  type        = string
  default     = ""
}

variable "offline_plan" {
  description = "CI only: skip AWS credential checks so `terraform plan` runs without an AWS account. Never apply with this set."
  type        = bool
  default     = false
}
