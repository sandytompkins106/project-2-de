from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Immutable runtime configuration loaded from environment variables."""

    github_token: str
    github_api_url: str
    per_page: int
    max_pages: int
    since: str
    output_dir: str
    # S3 settings — all optional; leave S3_BUCKET empty to disable S3 upload
    s3_bucket: str
    s3_prefix: str
    aws_region: str


def _default_since() -> str:
    """Return today's UTC date as ``YYYY-MM-DD``, used as the fallback extraction date."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_settings() -> Settings:
    """Build a :class:`Settings` instance from environment variables.

    All values are read from the process environment (populated by
    ``load_dotenv()`` at module import time).  Missing optional variables
    fall back to sensible defaults.
    """
    return Settings(
        github_token=os.getenv("GITHUB_TOKEN", "").strip(),
        github_api_url=os.getenv("GITHUB_API_URL", "https://api.github.com").rstrip("/"),
        per_page=int(os.getenv("GITHUB_PER_PAGE", "100")),
        max_pages=int(os.getenv("GITHUB_MAX_PAGES", "2")),
        since=os.getenv("GITHUB_SINCE", _default_since()),
        output_dir=os.getenv("OUTPUT_DIR", "data_integration/output"),
        s3_bucket=os.getenv("S3_BUCKET", "").strip(),
        s3_prefix=os.getenv("S3_PREFIX", "raw/github").strip().strip("/"),
        aws_region=os.getenv("AWS_REGION", "ap-southeast-2").strip(),
    )
