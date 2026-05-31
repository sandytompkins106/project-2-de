# ── Topics ────────────────────────────────────────────────────────────────────
# Three raw ingest topics (one per GitHub resource type) and two processed
# output topics that Flink writes into.
locals {
  raw_topics = [
    "github.repos.raw",
    "github.pull_requests.raw",
    "github.issues.raw",
  ]
  processed_topics = [
    "github.trending_repos",
    "github.developer_activity",
  ]
  all_topics = concat(local.raw_topics, local.processed_topics)
}

resource "confluent_kafka_topic" "topics" {
  for_each = toset(local.all_topics)

  topic_name       = each.value
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  config = {
    "retention.ms"   = "604800000" # 7 days
    "cleanup.policy" = "delete"
  }

  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }
  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }
}

# ── Service Accounts ──────────────────────────────────────────────────────────
resource "confluent_service_account" "cluster_admin" {
  display_name = "github-cluster-admin"
  description  = "Terraform cluster admin — manages topics and ACLs, not used at runtime"
}

resource "confluent_service_account" "s3_source" {
  display_name = "github-s3-source"
  description  = "S3 Source connector — reads JSONL files from S3 and writes to the three raw Kafka topics"
}

resource "confluent_service_account" "flink" {
  display_name = "github-flink"
  description  = "Confluent Flink compute pool — reads raw topics, writes processed topics"
}

resource "confluent_service_account" "clickhouse_sink" {
  display_name = "github-clickhouse-sink"
  description  = "HTTP Sink connector — reads processed topics and POSTs to ClickHouse"
}

# ── Role Bindings ─────────────────────────────────────────────────────────────
resource "confluent_role_binding" "cluster_admin" {
  principal   = "User:${confluent_service_account.cluster_admin.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

resource "confluent_role_binding" "flink_developer" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_role_binding" "flink_env_admin" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

# ── API Keys ──────────────────────────────────────────────────────────────────
resource "confluent_api_key" "cluster_admin" {
  display_name = "github-cluster-admin-key"
  owner {
    id          = confluent_service_account.cluster_admin.id
    api_version = confluent_service_account.cluster_admin.api_version
    kind        = confluent_service_account.cluster_admin.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind
    environment {
      id = confluent_environment.main.id
    }
  }
  depends_on = [confluent_role_binding.cluster_admin]
}

resource "confluent_api_key" "s3_source" {
  display_name = "github-s3-source-key"
  owner {
    id          = confluent_service_account.s3_source.id
    api_version = confluent_service_account.s3_source.api_version
    kind        = confluent_service_account.s3_source.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind
    environment {
      id = confluent_environment.main.id
    }
  }
}

resource "confluent_api_key" "flink" {
  display_name = "github-flink-key"
  owner {
    id          = confluent_service_account.flink.id
    api_version = confluent_service_account.flink.api_version
    kind        = confluent_service_account.flink.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind
    environment {
      id = confluent_environment.main.id
    }
  }
  depends_on = [confluent_flink_compute_pool.main]
}

resource "confluent_api_key" "clickhouse_sink" {
  display_name = "github-clickhouse-sink-key"
  owner {
    id          = confluent_service_account.clickhouse_sink.id
    api_version = confluent_service_account.clickhouse_sink.api_version
    kind        = confluent_service_account.clickhouse_sink.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind
    environment {
      id = confluent_environment.main.id
    }
  }
}

# ── ACLs: S3 Source connector (write raw records to ingest topics) ────────────
resource "confluent_kafka_acl" "s3_source_write" {
  for_each = toset(local.raw_topics)

  kafka_cluster { id = confluent_kafka_cluster.main.id }
  resource_type = "TOPIC"
  resource_name = each.value
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.s3_source.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }
}

# ── ACLs: ClickHouse sink — Dead Letter Queue (DLQ) access ─────────────────────
# The HTTP Sink connector auto-creates a DLQ topic named dlq-<connector-id>.
# Grant CREATE + WRITE on the "dlq-" prefix so both sink connectors can use it.
resource "confluent_kafka_acl" "sink_dlq_create" {
  kafka_cluster { id = confluent_kafka_cluster.main.id }
  resource_type = "TOPIC"
  resource_name = "dlq-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.clickhouse_sink.id}"
  host          = "*"
  operation     = "CREATE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }
}

resource "confluent_kafka_acl" "sink_dlq_write" {
  kafka_cluster { id = confluent_kafka_cluster.main.id }
  resource_type = "TOPIC"
  resource_name = "dlq-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.clickhouse_sink.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }
}
resource "confluent_kafka_acl" "sink_read" {
  for_each = toset(local.processed_topics)

  kafka_cluster { id = confluent_kafka_cluster.main.id }
  resource_type = "TOPIC"
  resource_name = each.value
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.clickhouse_sink.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }
}

resource "confluent_kafka_acl" "sink_consumer_group" {
  for_each = toset(["READ", "DESCRIBE", "DELETE"])

  kafka_cluster { id = confluent_kafka_cluster.main.id }
  resource_type = "GROUP"
  resource_name = "connect-lcc-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.clickhouse_sink.id}"
  host          = "*"
  operation     = each.value
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }
}

resource "confluent_kafka_acl" "clickpipes_consumer_group" {
  for_each = toset(["READ", "DESCRIBE"])

  kafka_cluster { id = confluent_kafka_cluster.main.id }
  resource_type = "GROUP"
  resource_name = "clickpipes-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.clickhouse_sink.id}"
  host          = "*"
  operation     = each.value
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }
}
