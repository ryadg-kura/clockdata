# --------------------------------------------------------------------------
# Glue Data Catalog + Athena
# Partition projection computes partitions (dt, hour) from the S3 layout, so
# there is no Glue crawler to run or pay for, and new hours are queryable
# immediately.
# --------------------------------------------------------------------------

resource "aws_glue_catalog_database" "lake" {
  name        = replace("${local.name}_lake", "-", "_")
  description = "ClockData data lake (bronze / silver / gold)"
}

locals {
  s3_lake = "s3://${aws_s3_bucket.data_lake.id}"

  partition_projection = {
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.range"         = "2025-01-01,NOW"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "projection.hour.type"        = "integer"
    "projection.hour.range"       = "0,23"
    "projection.hour.digits"      = "2"
  }

  json_envelope_columns = [
    { name = "payload", type = "string", comment = "Raw record as received" },
    { name = "partition_key", type = "string", comment = "Kinesis partition key" },
    { name = "sequence_number", type = "string", comment = "Kinesis sequence number" },
    { name = "shard_id", type = "string", comment = "Kinesis shard" },
    { name = "arrival_time", type = "string", comment = "Kinesis arrival time, ISO 8601 UTC" },
  ]

  tables = {
    bronze_events = {
      description = "Bronze: every record received, raw payload + Kinesis metadata (NDJSON)"
      prefix      = "bronze/events"
      format      = "json"
      columns     = local.json_envelope_columns
    }
    quarantine_events = {
      description = "Quarantine: records rejected by validation, with reasons (NDJSON)"
      prefix      = "quarantine/events"
      format      = "json"
      columns     = concat(local.json_envelope_columns, [{ name = "errors", type = "array<string>", comment = "Validation errors" }])
    }
    silver_events = {
      description = "Silver: validated, enriched events (Parquet), partitioned by event hour"
      prefix      = "silver/events"
      format      = "parquet"
      columns     = local.silver_columns
    }
    gold_device_hourly = {
      description = "Gold: per device per hour metrics (Parquet)"
      prefix      = "gold/device_hourly"
      format      = "parquet"
      columns     = local.gold_columns
    }
  }

  formats = {
    json = {
      input  = "org.apache.hadoop.mapred.TextInputFormat"
      output = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
      serde  = "org.openx.data.jsonserde.JsonSerDe"
      params = { "ignore.malformed.json" = "true" }
    }
    parquet = {
      input  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
      output = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
      serde  = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      params = {}
    }
  }
}

resource "aws_glue_catalog_table" "tables" {
  for_each = local.tables

  name          = each.key
  database_name = aws_glue_catalog_database.lake.name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(local.partition_projection, {
    "EXTERNAL"                  = "TRUE"
    "classification"            = each.value.format
    "storage.location.template" = "${local.s3_lake}/${each.value.prefix}/dt=$${dt}/hour=$${hour}/"
  })

  partition_keys {
    name    = "dt"
    type    = "string"
    comment = "UTC date, yyyy-MM-dd"
  }
  partition_keys {
    name    = "hour"
    type    = "string"
    comment = "UTC hour, 00-23"
  }

  storage_descriptor {
    location      = "${local.s3_lake}/${each.value.prefix}/"
    input_format  = local.formats[each.value.format].input
    output_format = local.formats[each.value.format].output

    ser_de_info {
      serialization_library = local.formats[each.value.format].serde
      parameters            = local.formats[each.value.format].params
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }
}

resource "aws_athena_workgroup" "lake" {
  name          = "${local.name}-workgroup"
  description   = "ClockData queries, with a per-query scan limit"
  force_destroy = true # delete saved queries/history on `make destroy`

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# The sample queries from ../queries show up in the Athena console
# ("Saved queries") so you can run them without the CLI.
resource "aws_athena_named_query" "samples" {
  for_each = fileset("${path.module}/../queries", "*.sql")

  name      = trimsuffix(each.value, ".sql")
  workgroup = aws_athena_workgroup.lake.id
  database  = aws_glue_catalog_database.lake.name
  query     = file("${path.module}/../queries/${each.value}")
}
