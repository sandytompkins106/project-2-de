# ── Flink Compute Pool ────────────────────────────────────────────────────────
# max_cfu=5 is the minimum for a dev pool; scale up for production.

# Flink API keys must be scoped to a Flink Region (kind="Region"), not a
# compute pool. The region wraps all compute pools in the same cloud+region.
data "confluent_flink_region" "main" {
  cloud  = "AWS"
  region = var.confluent_region
}

resource "confluent_flink_compute_pool" "main" {
  display_name = "github-analytics-flink"
  cloud        = "AWS"
  region       = var.confluent_region
  max_cfu      = 5

  environment {
    id = confluent_environment.main.id
  }
}

# ── Flink SQL Statements ──────────────────────────────────────────────────────
# Each statement is stored as a plain .sql file and read via file().
# Confluent Cloud Flink runs these as persistent streaming jobs until stopped.
#
# Sink table schemas are pre-registered in Schema Registry (schemas.tf) so
# Flink sees typed columns instead of [key: BYTES, val: BYTES] on output topics.

resource "confluent_flink_statement" "repos_parsed_view" {
  statement_name = "github-repos-parsed-view"
  statement      = file("${path.module}/flink_statements/repos_parsed_view.sql")
  rest_endpoint  = data.confluent_flink_region.main.rest_endpoint

  organization { id = data.confluent_organization.main.id }
  environment  { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal    { id = confluent_service_account.flink.id }

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_kafka_topic.topics,
    confluent_role_binding.flink_developer,
    confluent_role_binding.flink_env_admin,
  ]
}

resource "confluent_flink_statement" "trending_repos" {
  statement_name = "github-trending-repos"
  statement      = file("${path.module}/flink_statements/trending_repos.sql")
  rest_endpoint  = data.confluent_flink_region.main.rest_endpoint
  stopped        = false

  organization { id = data.confluent_organization.main.id }
  environment  { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal    { id = confluent_service_account.flink.id }

  # Tell Flink which catalog (environment) and database (cluster) to use.
  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_kafka_topic.topics,
    confluent_role_binding.flink_developer,
    confluent_role_binding.flink_env_admin,
    confluent_schema.trending_repos,
  ]
}

resource "confluent_flink_statement" "developer_activity" {
  statement_name = "github-developer-activity"
  statement      = file("${path.module}/flink_statements/developer_activity.sql")
  rest_endpoint  = data.confluent_flink_region.main.rest_endpoint
  stopped        = false

  organization { id = data.confluent_organization.main.id }
  environment  { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal    { id = confluent_service_account.flink.id }

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_kafka_topic.topics,
    confluent_role_binding.flink_developer,
    confluent_role_binding.flink_env_admin,
    confluent_flink_statement.repos_parsed_view,
    confluent_schema.developer_activity,
  ]
}

