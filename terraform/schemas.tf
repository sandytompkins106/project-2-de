# ── Schema Registry ───────────────────────────────────────────────────────────
# Registering Avro schemas for the two Flink output topics lets Confluent Flink
# see typed columns (repo_id BIGINT, full_name STRING, …) instead of the
# default [key: BYTES, val: BYTES] representation for schema-less topics.
# ─────────────────────────────────────────────────────────────────────────────

data "confluent_schema_registry_cluster" "main" {
  environment {
    id = confluent_environment.main.id
  }
}

resource "confluent_api_key" "schema_registry" {
  display_name = "github-schema-registry-key"
  description  = "API key for Schema Registry — used by Terraform to register Avro schemas."

  owner {
    id          = confluent_service_account.flink.id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.main.id
    api_version = "srcm/v2"
    kind        = "Cluster"

    environment {
      id = confluent_environment.main.id
    }
  }
}

# ── trending_repos Avro schema ────────────────────────────────────────────────

resource "confluent_schema" "trending_repos" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.main.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.main.rest_endpoint

  subject_name = "github.trending_repos-value"
  format       = "AVRO"

  schema = jsonencode({
    type      = "record"
    name      = "TrendingRepo"
    namespace = "io.github.analytics"
    fields = [
      { name = "repo_id",           type = "long" },
      { name = "full_name",         type = "string" },
      { name = "owner_login",       type = ["null", "string"], default = null },
      { name = "language",          type = ["null", "string"], default = null },
      { name = "stargazers_count",  type = "long" },
      { name = "forks_count",       type = "long" },
      { name = "open_issues_count", type = "long" },
      { name = "star_tier",         type = "string" },
      {
        name = "event_time"
        type = {
          type        = "long"
          logicalType = "timestamp-millis"
        }
      }
    ]
  })

  credentials {
    key    = confluent_api_key.schema_registry.id
    secret = confluent_api_key.schema_registry.secret
  }
}

# ── developer_activity Avro schema ────────────────────────────────────────────

resource "confluent_schema" "developer_activity" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.main.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.main.rest_endpoint

  subject_name = "github.developer_activity-value"
  format       = "AVRO"

  schema = jsonencode({
    type      = "record"
    name      = "DeveloperActivity"
    namespace = "io.github.analytics"
    fields = [
      { name = "owner_login",      type = "string" },
      { name = "owner_type",       type = ["null", "string"], default = null },
      { name = "repos_count",      type = "long" },
      { name = "total_stars",      type = "long" },
      { name = "total_forks",      type = "long" },
      { name = "primary_language", type = ["null", "string"], default = null },
      {
        name = "event_time"
        type = {
          type        = "long"
          logicalType = "timestamp-millis"
        }
      }
    ]
  })

  credentials {
    key    = confluent_api_key.schema_registry.id
    secret = confluent_api_key.schema_registry.secret
  }
}
