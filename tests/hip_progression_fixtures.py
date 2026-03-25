"""Synthetic fixtures for HIP progression tests."""

from __future__ import annotations

from datetime import UTC, datetime

from hiero_analytics.domain.hip_progression_models import (
    HipArtifact,
    HipCandidate,
    build_changed_file,
)


def make_pull_request_artifact(
    *,
    number: int = 101,
    title: str = "Implement HIP-1234 support",
    body: str = "This PR adds HIP-1234 support to the SDK.",
    comments_text: str = "",
    commit_messages_text: str = "feat: support HIP-1234",
    author_association: str = "MEMBER",
    merged: bool = True,
    changed_files=None,
    additions: int = 120,
    deletions: int = 15,
) -> HipArtifact:
    """Build a synthetic pull request artifact with realistic implementation signals."""
    if changed_files is None:
        changed_files = [
            build_changed_file("src/client/hip1234.ts", additions=80, deletions=5, status="added"),
            build_changed_file("tests/unit/hip1234.test.ts", additions=25, deletions=0, status="added"),
            build_changed_file("tests/integration/hip1234.e2e.ts", additions=15, deletions=0, status="added"),
        ]
    return HipArtifact(
        repo="hiero-ledger/hiero-sdk-js",
        artifact_type="pull_request",
        number=number,
        title=title,
        body=body,
        comments_text=comments_text,
        commit_messages_text=commit_messages_text,
        author_login="alice",
        author_association=author_association,
        state="closed" if merged else "open",
        merged=merged,
        created_at=datetime(2025, 1, 5, tzinfo=UTC),
        updated_at=datetime(2025, 1, 8, tzinfo=UTC),
        closed_at=datetime(2025, 1, 8, tzinfo=UTC) if merged else None,
        changed_files=changed_files,
        additions=additions,
        deletions=deletions,
        labels=["hip"],
        url=f"https://github.com/hiero-ledger/hiero-sdk-js/pull/{number}",
    )


def make_issue_artifact(
    *,
    number: int = 77,
    title: str = "Track HIP-1234 support",
    body: str = "We should discuss HIP-1234 support for a future release.",
    comments_text: str = "",
    author_association: str = "NONE",
) -> HipArtifact:
    """Build a synthetic issue artifact with text-only HIP evidence."""
    return HipArtifact(
        repo="hiero-ledger/hiero-sdk-js",
        artifact_type="issue",
        number=number,
        title=title,
        body=body,
        comments_text=comments_text,
        commit_messages_text="",
        author_login="bob",
        author_association=author_association,
        state="open",
        merged=False,
        created_at=datetime(2025, 1, 2, tzinfo=UTC),
        updated_at=datetime(2025, 1, 6, tzinfo=UTC),
        closed_at=None,
        changed_files=[],
        additions=0,
        deletions=0,
        labels=["discussion"],
        url=f"https://github.com/hiero-ledger/hiero-sdk-js/issues/{number}",
    )


def make_candidate(
    artifact: HipArtifact,
    *,
    hip_id: str = "HIP-1234",
    matched_sources: list[str] | None = None,
    negative_context_flags: list[str] | None = None,
) -> HipCandidate:
    """Build a synthetic HIP candidate tied to a test artifact."""
    matched_sources = matched_sources or ["title", "body"]
    return HipCandidate(
        artifact=artifact,
        hip_id=hip_id,
        extraction_source=", ".join(matched_sources),
        text_match_reason="; ".join(
            f"explicit HIP mention in {source.replace('_', ' ')}"
            for source in matched_sources
        ),
        negative_context_flags=list(negative_context_flags or []),
        matched_sources=list(matched_sources),
    )
