"""Synthetic fixtures for HIP progression tests."""

from __future__ import annotations

from datetime import UTC, datetime

from hiero_analytics.domain.hip_progression_models import (
    ArtifactComment,
    ArtifactCommit,
    HipArtifact,
    HipCandidate,
    HipCatalogEntry,
    build_changed_file,
)


def make_catalog_entries(*hip_ids: str) -> list[HipCatalogEntry]:
    """Build a small official HIP catalog snapshot for tests."""
    return [
        HipCatalogEntry(
            hip_id=hip_id,
            number=int(hip_id.split("-", maxsplit=1)[1]),
            title=f"Test {hip_id}",
            status="Approved",
            hip_type="Standards Track",
            category="Core",
            url=f"https://hips.hedera.com/hip/{hip_id.lower()}",
        )
        for hip_id in hip_ids
    ]


def make_pull_request_artifact(
    *,
    repo: str = "hiero-ledger/hiero-sdk-js",
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
    linked_artifact_numbers: list[int] | None = None,
) -> HipArtifact:
    """Build a synthetic pull request artifact with realistic implementation signals."""
    if changed_files is None:
        changed_files = [
            build_changed_file("src/client/hip1234.ts", additions=80, deletions=5, status="added"),
            build_changed_file("tests/unit/hip1234.test.ts", additions=25, deletions=0, status="added"),
            build_changed_file("tests/integration/hip1234.e2e.ts", additions=15, deletions=0, status="added"),
        ]
    comments = [
        ArtifactComment(
            body=comments_text,
            source_kind="review_comment",
            author_login="reviewer",
            author_association="MEMBER",
        )
    ] if comments_text else []
    commits = [ArtifactCommit(message=commit_messages_text)] if commit_messages_text else []
    return HipArtifact(
        repo=repo,
        artifact_type="pull_request",
        number=number,
        title=title,
        body=body,
        comments_text=comments_text,
        commit_messages_text=commit_messages_text,
        comments=comments,
        commits=commits,
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
        linked_artifact_numbers=list(linked_artifact_numbers or []),
        url=f"https://github.com/{repo}/pull/{number}",
    )


def make_issue_artifact(
    *,
    repo: str = "hiero-ledger/hiero-sdk-js",
    number: int = 77,
    title: str = "Track HIP-1234 support",
    body: str = "We should discuss HIP-1234 support for a future release.",
    comments_text: str = "",
    author_association: str = "NONE",
    state: str = "open",
    linked_artifact_numbers: list[int] | None = None,
) -> HipArtifact:
    """Build a synthetic issue artifact with text-only HIP evidence."""
    comments = [
        ArtifactComment(
            body=comments_text,
            source_kind="issue_comment",
            author_login="bob",
            author_association=author_association,
        )
    ] if comments_text else []
    return HipArtifact(
        repo=repo,
        artifact_type="issue",
        number=number,
        title=title,
        body=body,
        comments_text=comments_text,
        commit_messages_text="",
        comments=comments,
        commits=[],
        author_login="bob",
        author_association=author_association,
        state=state,
        merged=False,
        created_at=datetime(2025, 1, 2, tzinfo=UTC),
        updated_at=datetime(2025, 1, 6, tzinfo=UTC),
        closed_at=datetime(2025, 1, 8, tzinfo=UTC) if state == "closed" else None,
        changed_files=[],
        additions=0,
        deletions=0,
        labels=["discussion"],
        linked_artifact_numbers=list(linked_artifact_numbers or []),
        url=f"https://github.com/{repo}/issues/{number}",
    )


def make_candidate(
    artifact: HipArtifact,
    *,
    hip_id: str = "HIP-1234",
    source: str = "title_or_body",
    is_propagated: bool = False,
) -> HipCandidate:
    """Build a synthetic HIP candidate tied to a test artifact."""
    return HipCandidate(
        artifact=artifact,
        hip_id=hip_id,
        source=source,
        is_propagated=is_propagated,
    )
