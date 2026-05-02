from __future__ import annotations

import argparse
import json
import os

from .config import get_settings
from .pipeline import run_phase1_extraction


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 1 GitHub extraction validation (local output)."
    )
    parser.add_argument(
        "--resources",
        default="repositories,pull_requests,issues",
        help="Comma-separated resources: repositories,pull_requests,issues",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Single date (UTC) in YYYY-MM-DD format to extract. Defaults to env GITHUB_SINCE or today.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum pages per resource.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = get_settings()

    resources = [r.strip() for r in args.resources.split(",") if r.strip()]
    date = args.date or settings.since
    max_pages = args.max_pages if args.max_pages is not None else settings.max_pages

    if not settings.github_token:
        raise SystemExit("GITHUB_TOKEN is required for reliable GitHub API extraction.")

    summary = run_phase1_extraction(
        github_token=settings.github_token,
        github_api_url=settings.github_api_url,
        resources=resources,
        date=date,
        per_page=settings.per_page,
        max_pages=max_pages,
        output_dir=settings.output_dir,
        s3_bucket=settings.s3_bucket,
        s3_prefix=settings.s3_prefix,
        aws_region=settings.aws_region,
    )

    os.makedirs(settings.output_dir, exist_ok=True)
    summary_path = os.path.join(settings.output_dir, "latest_run_summary.json")
    with open(summary_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    print(json.dumps(summary, indent=2))
    print(f"\nRun summary written to: {summary_path}")


if __name__ == "__main__":
    main()
