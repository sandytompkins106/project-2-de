![alt text](images/readme_images/github_image.png)

# GitHub Analytics Pipeline

## Objective

This project builds two complementary analytics pipelines over the GitHub Search API:

1. **Batch pipeline** — extracts trending Python repositories, pull requests, and issues daily, loads them into Snowflake via Airbyte, and transforms them with dbt for dashboarding in Preset. Orchestrated by Dagster+ with CI/CD via GitHub Actions.
2. **Streaming pipeline** — re-uses the same S3 data and streams it through Confluent Cloud (Kafka + Flink SQL) into ClickHouse Cloud for real-time analytical queries. Infrastructure managed entirely by Terraform.

## Consumers

Data analysts and engineers who want to track GitHub ecosystem trends — repository growth, developer activity, community health scores, and PR/issue velocity across popular Python projects.

## Questions

> - Which Python repositories are trending by stars, forks, and activity?
> - Who are the most active developers (by PR and issue contributions)?
> - What is the community health of a repository (open issues ratio, merge rate, engagement)?
> - How do repository metrics evolve over time?

## Source datasets

Data is sourced from the [GitHub Search API](https://docs.github.com/en/rest/search).

| Source name    | Source type | Description                                      |
| -------------- | ----------- | ------------------------------------------------ |
| repositories   | REST API    | Python repos with ≥5 stars, filtered by date     |
| pull_requests  | REST API    | Pull requests linked to trending repos           |
| issues         | REST API    | Issues linked to trending repos                  |

## Solution architecture

### Batch pipeline

```
GitHub Search API
      │
      ▼
data_integration (Python extraction)
      │  writes JSONL partitioned by date
      ▼
AWS S3  (raw/github/{resource}/year=YYYY/month=MM/day=DD/part-00001.jsonl)
      │
      ▼
Airbyte Cloud  (S3 → Snowflake, github-s3 → Snowflake connection)
      │
      ▼
Snowflake  GITHUB.RAW.{repositories, pull_requests, issues}
      │
      ▼
dbt  (staging views → mart tables)
      │
      ▼
Snowflake  GITHUB.STAGING.*  /  GITHUB.MARTS.*
      │
      ▼
Preset  (dashboards)
```

All assets are orchestrated by **Dagster+** (serverless cloud) with daily partitions. CI/CD is handled by GitHub Actions — pull requests trigger lint + test checks, merges to `main` trigger a production deployment.

- **Extraction pattern**: Partitioned by date, deterministic S3 keys (backfill-safe / idempotent)
- **Load pattern**: Full refresh per partition via Airbyte
- **Transform pattern**: Staging layer (views) → Marts layer (tables) using dbt with `dbt_utils` and `dbt_expectations`

### Streaming pipeline

```
AWS S3  (raw/github/repos/...)
      │
      ▼
Confluent S3 Source Connector  (GENERIC mode, JSON, 3 connectors)
      │  github.repos.raw / github.prs.raw / github.issues.raw
      ▼
Confluent Cloud Kafka  (github-analytics cluster, AWS eu-central-1)
      │
      ▼
Flink SQL  (Confluent Cloud compute pool)
      │  persistent VIEW parses raw JSON bytes
      │  INSERT INTO github.trending_repos   (star_tier classification)
      │  INSERT INTO github.developer_activity  (row-by-row, aggregated at query time)
      ▼
Kafka topics (AVRO — schemas registered in Schema Registry)
      │  github.trending_repos / github.developer_activity
      ▼
ClickPipes  (ClickHouse Cloud managed connector)
      │  AvroConfluent format, schema auto-fetched from SR
      ▼
ClickHouse Cloud  (github_streaming database)
      │  trending_repos / developer_activity
      │  ReplacingMergeTree — idempotent on re-delivery
      ▼
ClickHouse SQL queries
```

Confluent Cloud infrastructure (environment, cluster, topics, service accounts, ACLs, Schema Registry schemas, Flink compute pool, Flink SQL statements) is managed entirely by **Terraform**. ClickHouse tables are created once via `terraform/clickhouse_setup.sql`. ClickPipes is configured manually in the ClickHouse Cloud UI.

## Project structure

```
project-2-de/
├── data_folders/
│   ├── data_integration/       # GitHub API extraction (config, client, pipeline, S3 writer)
│   ├── data_orchestration/     # Dagster assets, definitions, and dbt project
│   │   └── github_analytics/   # dbt project (models/staging + models/marts)
│   └── pyproject.toml          # Package definition (hatchling build)
├── terraform/                  # Streaming pipeline infrastructure (Confluent + ClickHouse)
│   ├── main.tf                 # Provider config, Confluent environment, Kafka cluster
│   ├── variables.tf            # Input variables (credentials, region, S3 config)
│   ├── outputs.tf              # Useful IDs and API keys after apply
│   ├── topics.tf               # Kafka topics, service accounts, API keys, ACLs
│   ├── connectors.tf           # S3 Source connectors (repos, PRs, issues)
│   ├── schemas.tf              # Schema Registry API key + AVRO schemas for output topics
│   ├── flink.tf                # Flink compute pool + 3 SQL statements
│   ├── flink_statements/
│   │   ├── repos_parsed_view.sql      # Persistent VIEW parsing raw JSON bytes
│   │   ├── trending_repos.sql         # INSERT with star_tier classification
│   │   └── developer_activity.sql     # INSERT row-by-row from parsed view
│   ├── clickhouse_setup.sql    # Run once manually — creates DB and tables
│   ├── terraform.tfvars.example
│   └── .terraform.lock.hcl
├── tests/
│   ├── unit/                   # Fully mocked unit tests (no credentials required)
│   └── gx/                     # Great Expectations data quality tests
├── .github/workflows/
│   ├── ci.yml                  # PR checks: black, ruff, unit tests, GX tests
│   ├── deploy.yml              # Production deployment on push to main
│   └── branch_deployments.yml  # Branch preview deployments on PRs
├── dagster_cloud.yaml          # Dagster+ code location config
└── pyproject.toml              # Root package (dev dependencies)
```

## Getting started

### Prerequisites

- Snowflake account
- AWS account with an S3 bucket
- Airbyte Cloud account
- Dagster+ account
- Preset account
- GitHub personal access token (with `public_repo` scope)
- conda or Python 3.11+

### 1. Clone the repo

```bash
git clone https://github.com/<your-org>/project-2-de.git
cd project-2-de
```

### 2. Create and activate the conda environment

```bash
conda create -n github_project python=3.11
conda activate github_project
```

### 3. Install dependencies

```bash
pip install -e "data_folders/[dev]"
pip install -r requirements-dev.txt
```

### 4. Configure environment variables

Create `data_folders/.env` (never commit this file):

```env
GITHUB_TOKEN=ghp_...
GITHUB_API_URL=https://api.github.com

AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-southeast-2
S3_BUCKET=your-bucket-name

AIRBYTE_WORKSPACE_ID=...
AIRBYTE_CLIENT_ID=...
AIRBYTE_CLIENT_SECRET=...
AIRBYTE_CONNECTION_ID=...

SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=AIRBYTE_ROLE
SNOWFLAKE_DATABASE=GITHUB
SNOWFLAKE_WAREHOUSE=AIRBYTE_WH
```

## Running locally

### Start Dagster dev server

```bash
cd data_folders
dagster dev
```

Then open http://localhost:3000, go to **Assets → Global Asset Lineage**, and click **Materialize all**.

### Run dbt manually

```bash
cd data_folders/data_orchestration/github_analytics
dbt deps
dbt run
dbt test
```

### Run tests

```bash
# Unit tests (no credentials needed)
pytest tests/unit/ -v

# Great Expectations data quality tests
pytest tests/gx/ -v
```

### Lint and format

```bash
black .                  # auto-format
black --check .          # check only (used in CI)
ruff check .             # style and import linting
```

## Airbyte setup

1. Log in to [Airbyte Cloud](https://cloud.airbyte.com)
2. Create a **Source**: Amazon S3 — point to your bucket, prefix `raw/github/`, file format JSONL
3. Create a **Destination**: Snowflake — database `GITHUB`, schema `RAW`
4. Create a **Connection** named `github-s3 → Snowflake`, set schedule to **Manual** (Dagster controls when syncs run)
5. Under **Settings → Schema**, enable streams: `repositories`, `pull_requests`, `issues`

To reset and re-ingest from scratch: **Settings → Clear your data**, then trigger a manual sync after backfilling S3.

## Snowflake setup

Run the following in a Snowflake worksheet to verify data after a sync:

```sql
-- Raw layer
SELECT COUNT(*) FROM GITHUB.RAW.REPOSITORIES;
SELECT COUNT(*) FROM GITHUB.RAW.PULL_REQUESTS;
SELECT COUNT(*) FROM GITHUB.RAW.ISSUES;

-- Marts layer (after dbt run)
SELECT COUNT(*) FROM GITHUB.MARTS.DIM_REPOS;
SELECT COUNT(*) FROM GITHUB.MARTS.FACT_ISSUES;
SELECT COUNT(*) FROM GITHUB.MARTS.FACT_PULL_REQUESTS;
```

## Dagster+ cloud deployment

Deployments are automated via GitHub Actions:

| Trigger             | Workflow                  | Action                            |
| ------------------- | ------------------------- | --------------------------------- |
| Push to `main`      | `deploy.yml`              | Deploy to production              |
| Open/update PR      | `branch_deployments.yml`  | Deploy to branch preview env      |
| Open/update PR      | `ci.yml`                  | Run black, ruff, unit + GX tests  |

Add the following secrets to your GitHub repository (**Settings → Secrets**):

- `DAGSTER_CLOUD_API_TOKEN`
- All env vars from the `.env` file above (prefixed as needed)

The Dagster+ deployment is configured in `dagster_cloud.yaml` — build directory is `data_folders/`, code location is `data_orchestration`.

## dbt models

| Layer   | Materialization | Models                                                                                 |
| ------- | --------------- | -------------------------------------------------------------------------------------- |
| Staging | View            | `stg_github__repositories`, `stg_github__pull_requests`, `stg_github__issues`         |
| Marts   | Table           | `dim_repos`, `dim_developers`, `dim_date`, `fact_repo_snapshots`, `fact_pull_requests`, `fact_issues`, `mart_trending_repos`, `mart_developer_activity`, `mart_community_health` |

dbt packages used: `dbt_utils`, `dbt_expectations`

## Preset dashboard

1. Log in to [Preset](https://preset.io)
2. Create a workspace and connect to your Snowflake database (`GITHUB.MARTS`)
3. Build charts from the mart tables (trending repos, developer activity, community health)
4. Add charts to a dashboard

---

## Streaming pipeline setup (Terraform + Confluent + ClickHouse)

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.5
- Confluent Cloud account with a Cloud API key (Organisation level)
- ClickHouse Cloud account (Mini tier, AWS eu-central-1)
- AWS credentials with read access to the S3 bucket

### 1. Configure variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your credentials (never commit this file):

```hcl
confluent_cloud_api_key    = "..."
confluent_cloud_api_secret = "..."
confluent_region           = "eu-central-1"

aws_access_key_id          = "..."
aws_secret_access_key      = "..."
s3_bucket                  = "project-2-github"
s3_region                  = "eu-central-1"

clickhouse_http_url        = "https://<host>:8443"
clickhouse_username        = "default"
clickhouse_password        = "..."
```

### 2. Deploy Confluent infrastructure

```bash
terraform init
terraform apply
```

This creates:
- Confluent environment `github-analytics` with Stream Governance ESSENTIALS (Schema Registry)
- Basic Kafka cluster `github-analytics` (AWS eu-central-1, SINGLE_ZONE)
- 5 Kafka topics: `github.repos.raw`, `github.prs.raw`, `github.issues.raw`, `github.trending_repos`, `github.developer_activity`
- 4 service accounts: `cluster_admin`, `s3_source`, `flink`, `clickhouse_sink`
- All ACLs scoped to minimum required permissions
- AVRO schemas for `github.trending_repos-value` and `github.developer_activity-value` in Schema Registry
- 3 S3 Source connectors (repos, PRs, issues)
- Flink compute pool + 3 SQL statements (VIEW + 2 INSERTs)

After apply, note the outputs:

```bash
terraform output clickhouse_sink_kafka_api_key
terraform output clickhouse_sink_kafka_api_secret
```

These are needed for ClickPipes setup in step 4.

### 3. Create ClickHouse tables

Run `terraform/clickhouse_setup.sql` once against your ClickHouse Cloud service via the SQL console or curl:

```bash
curl -u default:<password> \
  "https://<host>:8443/?database=default" \
  --data-binary @clickhouse_setup.sql
```

This creates the `github_streaming` database and two `ReplacingMergeTree` tables:
- `github_streaming.trending_repos` — deduplicated on `repo_id`
- `github_streaming.developer_activity` — deduplicated on `owner_login`

> **Why not Terraform?** The ClickHouse Terraform provider v3.x is a Cloud management API — it provisions services, not tables. Table creation via SQL is standard ClickHouse practice.

### 4. Configure ClickPipes (manual — ClickHouse Cloud UI)

ClickPipes is the managed Kafka connector inside ClickHouse Cloud. Configure one ClickPipe per output topic:

1. In ClickHouse Cloud, go to **Integrations → ClickPipes → New ClickPipe**
2. Select **Confluent Cloud** as the source
3. Set broker: `<bootstrap-endpoint>` (from `terraform output bootstrap_endpoint`)
4. Auth: **SASL/PLAIN** — use the `clickhouse_sink` API key/secret from step 2
5. Format: **AvroConfluent**
6. Schema Registry URL: from `terraform output schema_registry_endpoint`
7. SR credentials: use the `flink` SA SR API key (visible in Terraform state or Confluent Cloud UI)
8. Select topic `github.trending_repos` → map to `github_streaming.trending_repos`
9. Set offset: **from beginning**
10. Repeat for `github.developer_activity` → `github_streaming.developer_activity`

> **Consumer group ACL note:** ClickPipes uses a consumer group prefixed with `clickpipes-`. The Terraform ACL in `topics.tf` (`confluent_kafka_acl.clickpipes_consumer_group`) grants `clickhouse_sink` READ/DESCRIBE on `clickpipes-*` groups. This is required — without it ClickPipes fails with `GROUP_AUTHORIZATION_FAILED`.

### 5. Verify data in ClickHouse

```sql
-- Check row counts
SELECT COUNT(*) FROM github_streaming.trending_repos;
SELECT COUNT(*) FROM github_streaming.developer_activity;

-- Trending repos by star tier
SELECT star_tier, COUNT(*) AS repos, AVG(stargazers_count) AS avg_stars
FROM github_streaming.trending_repos FINAL
GROUP BY star_tier
ORDER BY avg_stars DESC;

-- Developer activity summary
SELECT owner_login, SUM(repos_count) AS repos, SUM(total_stars) AS stars
FROM github_streaming.developer_activity FINAL
GROUP BY owner_login
ORDER BY stars DESC
LIMIT 10;
```

### Pause / resume Flink statements

To pause (stop billing for CFUs without destroying state):

```hcl
# In terraform/flink.tf, set stopped = true on both INSERT statements
resource "confluent_flink_statement" "trending_repos" {
  stopped = true
  ...
}
```

```bash
terraform apply \
  -target=confluent_flink_statement.trending_repos \
  -target=confluent_flink_statement.developer_activity
```

To resume: set `stopped = false` and re-apply.

### Terraform file reference

| File | Purpose |
|------|---------|
| `main.tf` | Provider config, Confluent environment (with Stream Governance ESSENTIALS), Kafka cluster |
| `variables.tf` | All input variables — credentials, region, S3 config |
| `outputs.tf` | Bootstrap endpoint, cluster/environment/Flink IDs, `clickhouse_sink` API key for ClickPipes |
| `topics.tf` | 5 Kafka topics, 4 service accounts, API keys, all ACLs |
| `connectors.tf` | 3 S3 Source connectors (GENERIC mode, JSON format) |
| `schemas.tf` | Schema Registry lookup, SR API key, AVRO schemas for output topics |
| `flink.tf` | Flink compute pool, `repos_parsed_view`, `trending_repos` INSERT, `developer_activity` INSERT |

### Flink SQL statement reference

| Statement | Description |
|-----------|-------------|
| `repos_parsed_view.sql` | Persistent VIEW over `github.repos.raw` — parses raw JSON bytes into typed columns using `JSON_VALUE(CAST(val AS STRING), '$.payload.*')` |
| `trending_repos.sql` | Row-by-row INSERT into `github.trending_repos` — classifies each repo into `star_tier` (`high/medium/low/minimal`) |
| `developer_activity.sql` | Row-by-row INSERT into `github.developer_activity` — passes owner fields through; aggregation done at query time in ClickHouse |

> **Why row-by-row instead of windowed aggregation?** A TUMBLE window was attempted but watermarks stalled — all S3 data replays in one burst and then the source goes idle. With no new events arriving, the watermark never advances and the window never closes. Row-by-row INSERT sidesteps this for batch-replayed data.

---

## Phase 1 Quick Start

Testing direct GitHub API extraction validation before Airbyte ingestion.


1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set environment variables:

- Copy `data_integration/.env.example` values into your environment.
- Ensure `GITHUB_TOKEN` is set.

3. Run extraction validation (local output):

```bash
python -m data_integration.src.main --resources repositories,pull_requests,issues --max-pages 2
```