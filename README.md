# project-2-de

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