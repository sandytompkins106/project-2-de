"""Pytest fixtures for Great Expectations source data tests."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

_FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def _load_payloads(filename: str) -> pd.DataFrame:
    """Load a JSONL fixture file and extract the payload field into a DataFrame."""
    records = []
    with open(_FIXTURES_DIR / filename) as fh:
        for line in fh:
            line = line.strip()
            if line:
                rec = json.loads(line)
                records.append(rec.get("payload", rec))
    return pd.DataFrame(records)


@pytest.fixture(scope="session")
def repos_df() -> pd.DataFrame:
    return _load_payloads("sample_repositories.jsonl")


@pytest.fixture(scope="session")
def pull_requests_df() -> pd.DataFrame:
    return _load_payloads("sample_pull_requests.jsonl")


@pytest.fixture(scope="session")
def issues_df() -> pd.DataFrame:
    return _load_payloads("sample_issues.jsonl")
