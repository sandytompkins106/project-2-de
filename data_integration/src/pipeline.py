from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .github_client import GitHubClient
from .s3_writer import upload_jsonl


def _local_output_prefix(base_dir: str, resource: str, run_id: str, now_utc: datetime) -> Path:
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
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, default=str))
            handle.write("\n")


def run_phase1_extraction(
    github_token: str,
    github_api_url: str,
    resources: list[str],
    since: str,
    per_page: int,
    max_pages: int,
    output_dir: str,
    s3_bucket: str = "",
    s3_prefix: str = "raw/github",
    aws_region: str = "ap-southeast-2",
) -> dict[str, Any]:
    run_id = str(uuid.uuid4())
    now_utc = datetime.now(timezone.utc)

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
        response = client.fetch_resource(resource=resource, since=since, max_pages=max_pages)

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
            print(f"  Uploaded to {s3_data_uri}")

        manifest: dict[str, Any] = {
            "resource": resource,
            "run_id": run_id,
            "since": since,
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
    return results
