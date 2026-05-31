output "kafka_bootstrap_endpoint" {
  description = "Kafka bootstrap server — set as KAFKA_BOOTSTRAP_SERVERS in .env."
  value       = confluent_kafka_cluster.main.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  description = "Kafka REST endpoint — used by the Confluent CLI and topic management."
  value       = confluent_kafka_cluster.main.rest_endpoint
}

output "environment_id" {
  description = "Confluent environment ID."
  value       = confluent_environment.main.id
}

output "kafka_cluster_id" {
  description = "Confluent Kafka cluster ID."
  value       = confluent_kafka_cluster.main.id
}

output "flink_compute_pool_id" {
  description = "Flink compute pool ID."
  value       = confluent_flink_compute_pool.main.id
}

output "s3_source_service_account_id" {
  description = "Service account ID used by the S3 Source Connector."
  value       = confluent_service_account.s3_source.id
}

output "clickhouse_sink_kafka_api_key" {
  description = "Kafka API key for the clickhouse_sink service account — use as ClickPipes broker API key."
  value       = confluent_api_key.clickhouse_sink.id
  sensitive   = false
}

output "clickhouse_sink_kafka_api_secret" {
  description = "Kafka API secret for the clickhouse_sink service account."
  value       = confluent_api_key.clickhouse_sink.secret
  sensitive   = true
}
