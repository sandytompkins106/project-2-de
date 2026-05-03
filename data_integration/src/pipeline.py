from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger

from .github_client import GitHubClient
from .s3_writer import upload_jsonl


def _local_output_prefix(base_dir: str, resource: str, run_id: str, now_utc: datetime) -> Path:
    """Build the local directory path for a resource's output files.

    Partitions output using a Hive-style layout:
    ``<base_dir>/raw/github/<resource>/year=YYYY/month=MM/day=DD/run_id=<uuid>/``
    """
    return (
        Path(base_dir)
        / "raw"
        / "github"
        / resource
        / f"year={now_utc:%Y}"
        / f"month={now_utc:%m}"
        / f"day={now_utc:%d}"
        / f"run_id={run_id}"
    )


def _write_local_jsonl(records: list[dict[str, Any]], out_path: Path) -> None:
    """Write a list of records to a JSONL file, creating parent directories as needed."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, default=str))
            handle.write("\n")


def run_phase1_extraction(
    github_token: str,
    github_api_url: str,
    resources: list[str],
    date: str,
    per_page: int,
    max_pages: int,
    output_dir: str,
    s3_bucket: str = "",
    s3_prefix: str = "raw/github",
    aws_region: str = "ap-southeast-2",
) -> dict[str, Any]:
    """Extract GitHub repositories, pull requests, and issues for a single date.

    For each resource, fetches data from the GitHub Search API, enriches each
    record with run metadata, writes a local JSONL file, and optionally uploads
    to S3.  A JSON manifest summarising the run is written alongside the data.

    Args:
        github_token: GitHub personal access token.
        github_api_url: Base URL for the GitHub API.
        resources: List of resource types to extract (``repositories``,
            ``pull_requests``, ``issues``).
        date: ISO date string (``YYYY-MM-DD``) for the extraction window.
        per_page: Number of results per API page.
        max_pages: Maximum pages to fetch per resource.
        output_dir: Local base directory for JSONL output.
        s3_bucket: S3 bucket name; upload is skipped when empty.
        s3_prefix: S3 key prefix for uploaded files.
        aws_region: AWS region for the S3 client.

    Returns:
        A summary dict keyed by resource name containing record counts, file
        paths, and rate-limit metadata.
    """
    run_id = str(uuid.uuid4())
    now_utc = datetime.now(timezone.utc)
    logger.info("Starting extraction | date={} run_id={}", date, run_id)

    client = GitHubClient(
        base_url=github_api_url,
        token=github_token,
        per_page=per_page,
    )

    results: dict[str, Any] = {
        "run_id": run_id,
        "started_at_utc": now_utc.isoformat(),
        "resources": {},
    }

    for resource in resources:
        logger.info("Fetching {} for {}", resource, date)
        response = client.fetch_resource(resource=resource, date=date, max_pages=max_pages)
        logger.info("{}: {} items across {} pages", resource, len(response.items), response.pages_fetched)

        enriched: list[dict[str, Any]] = []
        extracted_at = datetime.now(timezone.utc).isoformat()
        for item in response.items:
            enriched.append(
                {
                    "_resource": resource,
                    "_run_id": run_id,
                    "_extracted_at": extracted_at,
                    "payload": item,
                }
            )

        local_prefix = _local_output_prefix(output_dir, resource, run_id, now_utc)
        data_file = local_prefix / "part-00001.jsonl"
        manifest_file = local_prefix / "manifest.json"
        _write_local_jsonl(enriched, data_file)

        # S3 upload (optional — skipped when s3_bucket is empty)
        s3_data_uri: str | None = None
        if s3_bucket:
            s3_key = (
                f"{s3_prefix}/{resource}"
                f"/year={now_utc:%Y}/month={now_utc:%m}/day={now_utc:%d}"
                f"/run_id={run_id}/part-00001.jsonl"
            )
            jsonl_content = "\n".join(json.dumps(r, default=str) for r in enriched)
            s3_data_uri = upload_jsonl(jsonl_content, s3_bucket, s3_key, aws_region)
            logger.info("Uploaded {} → {}", resource, s3_data_uri)

        manifest: dict[str, Any] = {
            "resource": resource,
            "run_id": run_id,
            "date": date,
            "records": len(enriched),
            "pages_fetched": response.pages_fetched,
            "request_count": response.request_count,
            "rate_limit_remaining": response.rate_limit_remaining,
            "rate_limit_reset_utc": response.rate_limit_reset_utc,
            "data_file": str(data_file).replace("\\", "/"),
            "s3_data_uri": s3_data_uri,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        manifest_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        results["resources"][resource] = {
            "records": len(enriched),
            "pages_fetched": response.pages_fetched,
            "request_count": response.request_count,
            "rate_limit_remaining": response.rate_limit_remaining,
            "rate_limit_reset_utc": response.rate_limit_reset_utc,
            "local_data_file": str(data_file).replace("\\", "/"),
            "local_manifest_file": str(manifest_file).replace("\\", "/"),
            "s3_data_uri": s3_data_uri,
        }

    results["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    logger.info("Extraction complete | run_id={}", run_id)
    return results
