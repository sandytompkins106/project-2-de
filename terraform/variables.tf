# ── Confluent Cloud ───────────────────────────────────────────────────────────

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud-level key, not a Kafka API key). Create at confluent.cloud → API keys."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret paired with confluent_cloud_api_key."
  type        = string
  sensitive   = true
}

variable "confluent_region" {
  description = "AWS region for the Kafka cluster and Flink compute pool. Must match the S3 bucket region."
  type        = string
  default     = "eu-central-1"
}

# ── ClickHouse Cloud ──────────────────────────────────────────────────────────

variable "clickhouse_username" {
  description = "ClickHouse username."
  type        = string
  default     = "default"
}

variable "clickhouse_password" {
  description = "ClickHouse password."
  type        = string
  sensitive   = true
}

variable "clickhouse_http_url" {
  description = "Full HTTPS URL used by the Confluent HTTP Sink connector (e.g. https://<host>:8443)."
  type        = string
}

# ── AWS / S3 (read by Confluent S3 Source Connector) ─────────────────────────
# The S3 Source Connector reads the same bucket that the batch pipeline writes
# to (project-2-github). These are the same AWS credentials already in .env.

variable "aws_access_key_id" {
  description = "AWS access key ID used by the S3 Source Connector to read JSONL files."
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key paired with aws_access_key_id."
  type        = string
  sensitive   = true
}

variable "s3_bucket" {
  description = "S3 bucket name that the batch pipeline writes to. S3 Source Connector polls this bucket."
  type        = string
  default     = "project-2-github"
}

variable "s3_region" {
  description = "AWS region of the S3 bucket. Must match confluent_region."
  type        = string
  default     = "eu-central-1"
}
