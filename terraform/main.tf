terraform {
  required_version = ">= 1.5"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.11"
    }
  }
}

# ── Confluent Cloud provider ──────────────────────────────────────────────────
# Credentials come from terraform.tfvars (never commit that file).
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}


# ── Confluent Environment ─────────────────────────────────────────────────────
resource "confluent_environment" "main" {
  display_name = "github-analytics"

  # Stream Governance ESSENTIALS includes Schema Registry — required for Flink.
  stream_governance {
    package = "ESSENTIALS"
  }
}

# ── Kafka Cluster ─────────────────────────────────────────────────────────────
# Basic single-zone cluster — sufficient for a dev/bootcamp workload.
resource "confluent_kafka_cluster" "main" {
  display_name = "github-analytics"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.confluent_region

  basic {}

  environment {
    id = confluent_environment.main.id
  }
}

# ── Data: look up the Confluent org id (needed by Flink statements) ───────────
data "confluent_organization" "main" {}
