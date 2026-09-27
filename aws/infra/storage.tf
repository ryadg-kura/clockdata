# --------------------------------------------------------------------------
# S3 data lake: one bucket, three medallion prefixes
#   bronze/events/     raw NDJSON envelopes (every record received)
#   silver/events/     validated + enriched Parquet
#   gold/device_hourly aggregated Parquet (per device, per hour)
#   quarantine/events  rejected records + reasons
# --------------------------------------------------------------------------

resource "aws_s3_bucket" "data_lake" {
  bucket = "${local.name}-datalake-${random_id.suffix.hex}"

  # Lets `terraform destroy` delete the bucket even when it still contains
  # objects. Without it, `make destroy` would stop on a non-empty bucket.
  force_destroy = true
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${local.name}-athena-results-${random_id.suffix.hex}"
  force_destroy = true
}

locals {
  buckets = {
    data_lake      = aws_s3_bucket.data_lake.id
    athena_results = aws_s3_bucket.athena_results.id
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = local.buckets

  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = local.buckets

  bucket = each.value
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets

  bucket = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # SSE-S3: free, no KMS request charges
    }
  }
}

# Deny any request not using TLS.
data "aws_iam_policy_document" "tls_only" {
  for_each = {
    data_lake      = aws_s3_bucket.data_lake.arn
    athena_results = aws_s3_bucket.athena_results.arn
  }

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      each.value,
      "${each.value}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = local.buckets

  bucket = each.value
  policy = data.aws_iam_policy_document.tls_only[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.this]
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "expire-demo-data"
    status = "Enabled"
    filter {}
    expiration {
      days = var.data_retention_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
