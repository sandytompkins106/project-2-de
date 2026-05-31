# ── S3 Source Connectors ──────────────────────────────────────────────────────
# Three managed S3 Source connectors, one per GitHub resource type.
# Each polls its S3 prefix for new JSONL files (written by the batch pipeline)
# and publishes every JSON line as a Kafka message to the matching raw topic.
#
# S3 path layout written by data_integration/pipeline.py:
#   s3://<bucket>/raw/github/<resource>/year=YYYY/month=MM/day=DD/part-00001.jsonl
#
# Each message value is the full JSON record:
#   { "_resource": "...", "_run_id": "...", "_extracted_at": "...", "payload": {...} }
# Flink SQL extracts fields from "payload" via JSON_VALUE().
# ─────────────────────────────────────────────────────────────────────────────

locals {
  s3_sources = {
    repos         = { prefix = "raw/github/repositories", topic = "github.repos.raw" }
    pull_requests = { prefix = "raw/github/pull_requests", topic = "github.pull_requests.raw" }
    issues        = { prefix = "raw/github/issues",        topic = "github.issues.raw" }
  }
}

resource "confluent_connector" "s3_source" {
  for_each = local.s3_sources

  environment   { id = confluent_environment.main.id }
  kafka_cluster { id = confluent_kafka_cluster.main.id }

  config_nonsensitive = {
    "connector.class"          = "S3Source"
    "name"                     = "github-s3-source-${each.key}"
    "tasks.max"                = "1"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.s3_source.id

    # S3 source
    "aws.access.key.id"        = var.aws_access_key_id
    "s3.bucket.name"           = var.s3_bucket
    "s3.region"                = var.s3_region
    "topics.dir"               = each.value.prefix
    "s3.recursive.descend"     = "true"

    # Routing — GENERIC mode requires topic.regex.list: "<topic>:<filename-regex>"
    # Each connector monitors one prefix so .* matches all files under it.
    "topic.regex.list"         = "${each.value.topic}:.*"

    # Data format — one JSON object per JSONL line becomes one Kafka message.
    # Plain JSON (no Schema Registry) keeps the topic as `val BYTES` in Flink,
    # which is accessed via JSON_VALUE(CAST(val AS STRING), '$.path').
    "input.data.format"        = "JSON"
    "output.data.format"       = "JSON"
    "mode"                     = "GENERIC"
    "record.batch.max.size"    = "500"
  }

  config_sensitive = {
    "aws.secret.access.key" = var.aws_secret_access_key
  }

  depends_on = [
    confluent_kafka_topic.topics,
    confluent_kafka_acl.s3_source_write,
  ]
}

# ── ClickHouse Sink ───────────────────────────────────────────────────────────
# ClickHouse ingestion is handled via ClickPipes (configured manually in the
# ClickHouse Cloud UI). No Terraform-managed connector needed.
# ─────────────────────────────────────────────────────────────────────────────
