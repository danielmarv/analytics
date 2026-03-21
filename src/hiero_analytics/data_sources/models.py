"""
Typed data models representing normalized GitHub records.

These dataclasses define the structured records produced by the GitHub
ingestion layer. They provide a consistent schema for repositories,
issues, and merged pull request difficulty metrics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class RepositoryRecord:
    """Metadata describing a GitHub repository."""
    full_name: str
    name: str
    owner: str
    created_at: datetime | None = None
    stargazers: int | None = None
    forks: int | None = None


@dataclass(frozen=True)
class IssueRecord:
    """A normalized GitHub issue record."""
    repo: str
    number: int
    title: str
    state: str
    created_at: datetime
    closed_at: datetime | None
    labels: list[str]


@dataclass(frozen=True)
class PullRequestDifficultyRecord:
    """Metadata linking a merged pull request to the issues it closes."""
    repo: str
    pr_number: int
    pr_created_at: datetime
    pr_merged_at: datetime
    pr_additions: int
    pr_deletions: int
    pr_changed_files: int
    issue_number: int
    issue_labels: list[str]


@dataclass(frozen=True)
class ContributorActivityRecord:
    """A normalized contributor activity event used for pipeline inference."""

    repo: str
    actor: str
    occurred_at: datetime
    activity_type: str
    target_type: str
    target_number: int
    target_author: str | None = None
    detail: str | None = None
