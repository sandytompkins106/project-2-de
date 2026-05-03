"""Unit tests for the Phase 1 extraction pipeline."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import patch

from src.github_client import ResourceResult
from src.pipeline import run_phase1_extraction


def _make_result(resource: str, items: list) -> ResourceResult:
    return ResourceResult(
        resource=resource,
        items=items,
        pages_fetched=1,
        request_count=1,
        rate_limit_remaining=50,
        rate_limit_reset_utc="2026-04-25T06:00:00+00:00",
    )


_SAMPLE: dict = {
    "repositories": [{"id": 1, "full_name": "user/repo", "stargazers_count": 10}],
    "pull_requests": [{"id": 100, "title": "Fix bug"}],
    "issues": [{"id": 200, "title": "Bug report"}],
}


def _side_effect(resource: str, date: str, max_pages: int = 2) -> ResourceResult:
    return _make_result(resource, _SAMPLE[resource])


class TestRunPhase1Extraction:
    def test_creates_local_jsonl_file(self, tmp_path: Path) -> None:
        with patch("src.pipeline.GitHubClient") as mock_cls:
            mock_cls.return_value.fetch_resource.side_effect = _side_effect
            result = run_phase1_extraction(
                github_token="token",
                github_api_url="https://api.github.com",
                resources=["repositories"],
                date="2026-04-25",
                per_page=100,
                max_pages=2,
                output_dir=str(tmp_path),
            )
        data_file = Path(result["resources"]["repositories"]["local_data_file"])
        assert data_file.exists()

    def test_jsonl_contains_enrichment_fields(self, tmp_path: Path) -> None:
        with patch("src.pipeline.GitHubClient") as mock_cls:
            mock_cls.return_value.fetch_resource.side_effect = _side_effect
            result = run_phase1_extraction(
                github_token="token",
                github_api_url="https://api.github.com",
                resources=["repositories"],
                date="2026-04-25",
                per_page=100,
                max_pages=2,
                output_dir=str(tmp_path),
            )
        data_file = Path(result["resources"]["repositories"]["local_data_file"])
        record = json.loads(data_file.read_text().splitlines()[0])
        assert record["_resource"] == "repositories"
        assert "_run_id" in record
        assert "_extracted_at" in record
        assert record["payload"]["id"] == 1

    def test_s3_skipped_when_no_bucket(self, tmp_path: Path) -> None:
        with patch("src.pipeline.GitHubClient") as mock_cls:
            mock_cls.return_value.fetch_resource.side_effect = _side_effect
            result = run_phase1_extraction(
                github_token="token",
                github_api_url="https://api.github.com",
                resources=["repositories"],
                date="2026-04-25",
                per_page=100,
                max_pages=2,
                output_dir=str(tmp_path),
                s3_bucket="",
            )
        assert result["resources"]["repositories"]["s3_data_uri"] is None

    def test_all_three_resources_extracted(self, tmp_path: Path) -> None:
        with patch("src.pipeline.GitHubClient") as mock_cls:
            mock_cls.return_value.fetch_resource.side_effect = _side_effect
            result = run_phase1_extraction(
                github_token="token",
                github_api_url="https://api.github.com",
                resources=["repositories", "pull_requests", "issues"],
                date="2026-04-25",
                per_page=100,
                max_pages=2,
                output_dir=str(tmp_path),
            )
        assert set(result["resources"].keys()) == {"repositories", "pull_requests", "issues"}

    def test_run_id_is_uuid(self, tmp_path: Path) -> None:
        with patch("src.pipeline.GitHubClient") as mock_cls:
            mock_cls.return_value.fetch_resource.side_effect = _side_effect
            result = run_phase1_extraction(
                github_token="token",
                github_api_url="https://api.github.com",
                resources=["repositories"],
                date="2026-04-25",
                per_page=100,
                max_pages=2,
                output_dir=str(tmp_path),
            )
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            result["run_id"],
        )

    def test_manifest_written_alongside_data(self, tmp_path: Path) -> None:
        with patch("src.pipeline.GitHubClient") as mock_cls:
            mock_cls.return_value.fetch_resource.side_effect = _side_effect
            result = run_phase1_extraction(
                github_token="token",
                github_api_url="https://api.github.com",
                resources=["repositories"],
                date="2026-04-25",
                per_page=100,
                max_pages=2,
                output_dir=str(tmp_path),
            )
        manifest_file = Path(result["resources"]["repositories"]["local_manifest_file"])
        assert manifest_file.exists()
        manifest = json.loads(manifest_file.read_text())
        assert manifest["resource"] == "repositories"
        assert manifest["date"] == "2026-04-25"
