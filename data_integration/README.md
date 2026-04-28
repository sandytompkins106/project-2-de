# Data Integration (Phase 1)

This module validates live GitHub API extraction before Airbyte ingestion.

## What it does

1. Pulls live data from GitHub API resources:
   - repositories
   - pull_requests
   - issues
2. Handles pagination with configurable max pages.
3. Captures rate-limit metadata in manifests.
4. Writes raw output locally as JSONL.

## Setup

1. Create and activate a Python environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set environment variables (copy from `.env.example`).

## Run

Local output only:

```bash
python -m data_integration.src.main --resources repositories,pull_requests,issues --max-pages 2
```

## Output layout

Local:

- data_integration/output/raw/github/{resource}/year=YYYY/month=MM/day=DD/run_id=<uuid>/part-00001.jsonl
- data_integration/output/raw/github/{resource}/year=YYYY/month=MM/day=DD/run_id=<uuid>/manifest.json
- data_integration/output/latest_run_summary.json

## Notes

- Use an authenticated GitHub token to avoid strict unauthenticated limits.
- Search endpoints can return up to 1000 results per query window.
- Keep `max-pages` small for initial validation runs.
