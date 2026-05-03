"""Great Expectations validation tests for raw GitHub source data.

Each test loads a fixture JSONL file (mirroring what Phase 1 extraction
writes to S3) and validates it against a set of GX expectations using an
ephemeral in-memory DataContext.

Why GX here?
  - Validates the *source contract* — shape and semantics of GitHub API data
    before it ever reaches Airbyte or Snowflake.
  - Catches upstream API changes (e.g. field renames, unexpected nulls) early.
  - Complements dbt-expectations tests which run post-ingestion in Snowflake.
"""

from __future__ import annotations

import great_expectations as gx
import pandas as pd


def _validate(df: pd.DataFrame, suite_name: str, expectations: list) -> None:
    """Spin up an ephemeral GX context, apply expectations, and assert success."""
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(suite_name)
    asset = datasource.add_dataframe_asset("data")
    batch_def = asset.add_batch_definition_whole_dataframe("batch")
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for exp in expectations:
        suite.add_expectation(exp)
    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{suite_name}_vd", data=batch_def, suite=suite)
    )
    result = vd.run(batch_parameters={"dataframe": df})
    assert result.success, f"GX validation failed [{suite_name}]:\n{result}"


class TestRawRepositories:
    def test_required_fields_exist_and_non_null(self, repos_df: pd.DataFrame) -> None:
        _validate(
            repos_df,
            "repos_required_fields",
            [
                gx.expectations.ExpectColumnToExist(column="id"),
                gx.expectations.ExpectColumnToExist(column="full_name"),
                gx.expectations.ExpectColumnToExist(column="stargazers_count"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="full_name"),
            ],
        )

    def test_stargazers_count_respects_api_filter(self, repos_df: pd.DataFrame) -> None:
        """All returned repos must have >= 5 stars (our search filter)."""
        _validate(
            repos_df,
            "repos_stars_filter",
            [
                gx.expectations.ExpectColumnValuesToBeBetween(column="stargazers_count", min_value=5),
            ],
        )

    def test_language_is_python(self, repos_df: pd.DataFrame) -> None:
        """All repos must be Python (our language filter)."""
        _validate(
            repos_df,
            "repos_language",
            [
                gx.expectations.ExpectColumnValuesToBeInSet(column="language", value_set=["Python"]),
            ],
        )

    def test_at_least_one_row(self, repos_df: pd.DataFrame) -> None:
        _validate(
            repos_df,
            "repos_row_count",
            [gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)],
        )


class TestRawPullRequests:
    def test_required_fields_non_null(self, pull_requests_df: pd.DataFrame) -> None:
        _validate(
            pull_requests_df,
            "prs_required_fields",
            [
                gx.expectations.ExpectColumnToExist(column="id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="title"),
            ],
        )

    def test_state_is_open_or_closed(self, pull_requests_df: pd.DataFrame) -> None:
        _validate(
            pull_requests_df,
            "prs_state",
            [
                gx.expectations.ExpectColumnValuesToBeInSet(column="state", value_set=["open", "closed"]),
            ],
        )


class TestRawIssues:
    def test_required_fields_non_null(self, issues_df: pd.DataFrame) -> None:
        _validate(
            issues_df,
            "issues_required_fields",
            [
                gx.expectations.ExpectColumnToExist(column="id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="title"),
            ],
        )

    def test_state_is_open_or_closed(self, issues_df: pd.DataFrame) -> None:
        _validate(
            issues_df,
            "issues_state",
            [
                gx.expectations.ExpectColumnValuesToBeInSet(column="state", value_set=["open", "closed"]),
            ],
        )

    def test_comments_count_non_negative(self, issues_df: pd.DataFrame) -> None:
        _validate(
            issues_df,
            "issues_comments",
            [
                gx.expectations.ExpectColumnValuesToBeBetween(column="comments", min_value=0),
            ],
        )
