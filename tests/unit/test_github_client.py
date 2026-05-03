"""Unit tests for GitHubClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from src.github_client import GitHubAPIError, GitHubClient, ResourceResult


@pytest.fixture
def client() -> GitHubClient:
    return GitHubClient(
        base_url="https://api.github.com",
        token="test_token",
        per_page=10,
    )


def _make_response(
    items: list,
    status: int = 200,
    remaining: str = "50",
    reset: str = "9999999999",
) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status
    mock.headers = {
        "X-RateLimit-Remaining": remaining,
        "X-RateLimit-Reset": reset,
    }
    mock.json.return_value = {"items": items, "total_count": len(items)}
    mock.text = "error body"
    return mock


class TestFetchResource:
    def test_repositories_returns_items(self, client: GitHubClient) -> None:
        item = {"id": 1, "full_name": "user/repo", "stargazers_count": 10}
        with patch.object(client.session, "get", return_value=_make_response([item])):
            result = client.fetch_resource("repositories", "2026-04-25", max_pages=1)
        assert isinstance(result, ResourceResult)
        assert len(result.items) == 1
        assert result.items[0]["id"] == 1

    def test_pull_requests_resource(self, client: GitHubClient) -> None:
        item = {"id": 100, "title": "Fix bug"}
        with patch.object(client.session, "get", return_value=_make_response([item])):
            result = client.fetch_resource("pull_requests", "2026-04-25", max_pages=1)
        assert result.resource == "pull_requests"
        assert len(result.items) == 1

    def test_issues_resource(self, client: GitHubClient) -> None:
        item = {"id": 200, "title": "Bug report"}
        with patch.object(client.session, "get", return_value=_make_response([item])):
            result = client.fetch_resource("issues", "2026-04-25", max_pages=1)
        assert result.resource == "issues"

    def test_unsupported_resource_raises(self, client: GitHubClient) -> None:
        with pytest.raises(ValueError, match="Unsupported resource"):
            client.fetch_resource("comments", "2026-04-25")

    def test_deduplicates_by_id(self, client: GitHubClient) -> None:
        items = [{"id": 1, "name": "a"}, {"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        responses = [_make_response(items), _make_response([])]
        with patch.object(client.session, "get", side_effect=responses):
            result = client.fetch_resource("repositories", "2026-04-25", max_pages=2)
        assert len(result.items) == 2

    def test_stops_on_empty_page(self, client: GitHubClient) -> None:
        responses = [_make_response([{"id": 1}]), _make_response([])]
        with patch.object(client.session, "get", side_effect=responses):
            result = client.fetch_resource("repositories", "2026-04-25", max_pages=5)
        assert result.pages_fetched == 2

    def test_rate_limit_metadata_parsed(self, client: GitHubClient) -> None:
        item = [{"id": 1}]
        with patch.object(
            client.session,
            "get",
            return_value=_make_response(item, remaining="42", reset="1999999999"),
        ):
            result = client.fetch_resource("repositories", "2026-04-25", max_pages=1)
        assert result.rate_limit_remaining == 42
        assert result.rate_limit_reset_utc is not None

    def test_non_retriable_error_raises_immediately(self, client: GitHubClient) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 422
        mock_resp.headers = {}
        mock_resp.text = "Validation error"
        with patch.object(client.session, "get", return_value=mock_resp):
            with pytest.raises(GitHubAPIError):
                client.fetch_resource("repositories", "2026-04-25", max_pages=1)

    def test_request_count_tracked(self, client: GitHubClient) -> None:
        responses = [_make_response([{"id": i}]) for i in range(1, 3)] + [_make_response([])]
        with patch.object(client.session, "get", side_effect=responses):
            result = client.fetch_resource("repositories", "2026-04-25", max_pages=5)
        assert result.request_count == 3
