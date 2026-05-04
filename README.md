# GitHub Analytics Pipeline

## Objective

The objective of this project is to build an automated ELT pipeline that extracts trending Python repositories, pull requests, and issues from the GitHub Search API, loads them into a Snowflake data warehouse, and transforms them into analytical datasets for end-user reporting. The pipeline runs on a daily partition schedule and is deployed to Dagster+ serverless cloud with CI/CD via GitHub Actions.

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

## Project structure

```
project-2-de/
├── data_folders/
│   ├── data_integration/       # GitHub API extraction (config, client, pipeline, S3 writer)
│   ├── data_orchestration/     # Dagster assets, definitions, and dbt project
│   │   └── github_analytics/   # dbt project (models/staging + models/marts)
│   └── pyproject.toml          # Package definition (hatchling build)
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

Capstone project for building a managed-cloud batch data engineering pipeline focused on GitHub analytics.

## Current Status

Phase 1 implementation is in progress and includes direct GitHub API extraction validation before Airbyte ingestion.

Implemented module:

- data_integration/src (GitHub extraction local validation)

## Phase 1 Quick Start

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