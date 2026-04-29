from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


class GitHubAPIError(RuntimeError):
    """Raised when GitHub API calls fail after retries."""


@dataclass
class ResourceResult:
    resource: str
    items: list[dict[str, Any]]
    pages_fetched: int
    request_count: int
    rate_limit_remaining: int | None
    rate_limit_reset_utc: str | None


class GitHubClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        per_page: int = 100,
        timeout_seconds: int = 30,
        max_retries: int = 5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.per_page = per_page
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "project-2-de-github-extractor",
            }
        )
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def _request(self, path: str, params: dict[str, Any]) -> requests.Response:
        url = f"{self.base_url}{path}"
        backoff_seconds = 1.0

        for attempt in range(1, self.max_retries + 1):
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            if response.status_code < 400:
                return response

            # 429 and 5xx are retriable; rate-limit exhausted is also retriable.
            retriable = response.status_code in {429, 500, 502, 503, 504}
            exhausted = (
                response.status_code == 403
                and response.headers.get("X-RateLimit-Remaining") == "0"
            )
            if retriable or exhausted:
                reset_epoch = response.headers.get("X-RateLimit-Reset")
                if exhausted and reset_epoch and reset_epoch.isdigit():
                    sleep_for = max(0, int(reset_epoch) - int(time.time())) + 1
                else:
                    sleep_for = backoff_seconds

                if attempt == self.max_retries:
                    break

                time.sleep(sleep_for)
                backoff_seconds = min(backoff_seconds * 2, 32)
                continue

            raise GitHubAPIError(
                f"GitHub API error {response.status_code}: {response.text[:500]}"
            )

        raise GitHubAPIError(
            f"GitHub API failed after {self.max_retries} retries for path={path}"
        )

    def _extract_rate_limit(self, response: requests.Response) -> tuple[int | None, str | None]:
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_epoch = response.headers.get("X-RateLimit-Reset")

        remaining_int = int(remaining) if remaining and remaining.isdigit() else None
        if reset_epoch and reset_epoch.isdigit():
            reset_utc = datetime.fromtimestamp(int(reset_epoch), tz=timezone.utc).isoformat()
        else:
            reset_utc = None

        return remaining_int, reset_utc

    def fetch_resource(self, resource: str, since: str, max_pages: int = 2) -> ResourceResult:
        if resource == "repositories":
            path = "/search/repositories"
            query = f"created:>={since}"
        elif resource == "pull_requests":
            path = "/search/issues"
            query = f"type:pr created:>={since}"
        elif resource == "issues":
            path = "/search/issues"
            query = f"type:issue created:>={since}"
        else:
            raise ValueError(
                "Unsupported resource. Choose repositories, pull_requests, or issues."
            )

        all_items: list[dict[str, Any]] = []
        request_count = 0
        pages_fetched = 0
        remaining: int | None = None
        reset_utc: str | None = None

        for page in range(1, max_pages + 1):
            params = {
                "q": query,
                "sort": "updated",
                "order": "desc",
                "per_page": self.per_page,
                "page": page,
            }
            response = self._request(path, params)
            request_count += 1
            pages_fetched += 1
            remaining, reset_utc = self._extract_rate_limit(response)

            body = response.json()
            items = body.get("items", [])
            if not items:
                break
            all_items.extend(items)

        # Deduplicate by id — GitHub Search API can return the same item on
        # multiple pages when results shift between page fetches (sort=updated).
        seen: set[int] = set()
        deduped: list[dict[str, Any]] = []
        for item in all_items:
            item_id = item.get("id")
            if item_id not in seen:
                seen.add(item_id)
                deduped.append(item)
        all_items = deduped

        return ResourceResult(
            resource=resource,
            items=all_items,
            pages_fetched=pages_fetched,
            request_count=request_count,
            rate_limit_remaining=remaining,
            rate_limit_reset_utc=reset_utc,
        )
