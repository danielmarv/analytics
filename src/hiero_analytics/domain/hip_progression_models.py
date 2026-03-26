"""Domain models and shared heuristics for HIP progression analysis."""

from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal

ArtifactType = Literal["pull_request", "issue"]
ChangedFileStatus = Literal["added", "modified", "removed", "renamed"]
AuthorScope = Literal["all", "maintainers", "committers"]
ConfidenceLevel = Literal["low", "medium", "high"]
RepoHipStatus = Literal["not_started", "in_progress", "completed", "unknown", "conflicting"]

MAINTAINER_AUTHOR_ASSOCIATIONS = {"OWNER", "MEMBER"}
COMMITTER_AUTHOR_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}

SOURCE_DIR_HINTS = {"src", "lib", "app", "package", "packages", "sdk", "client", "clients"}
TEST_DIR_HINTS = {"test", "tests", "__tests__", "spec", "specs"}
INTEGRATION_DIR_HINTS = {"integration", "integrations", "e2e", "end-to-end", "acceptance"}
NON_SOURCE_DIR_HINTS = {
    ".github",
    "docs",
    "documentation",
    "examples",
    "example",
    "samples",
    "sample",
    "scripts",
    "script",
    "fixtures",
    "fixture",
    "dist",
    "build",
    "coverage",
}
CODE_FILE_EXTENSIONS = {
    ".c",
    ".cc",
    ".cpp",
    ".cs",
    ".go",
    ".h",
    ".hpp",
    ".java",
    ".js",
    ".jsx",
    ".kt",
    ".mjs",
    ".py",
    ".rb",
    ".rs",
    ".swift",
    ".ts",
    ".tsx",
}
ARTIFACT_REFERENCE_PATTERNS = (
    re.compile(r"(?:^|[^\w])#(\d+)\b"),
    re.compile(r"github\.com/[^/\s]+/[^/\s]+/(?:issues|pull)/(\d+)\b", re.IGNORECASE),
    re.compile(r"\b[\w.-]+/[\w.-]+#(\d+)\b"),
)


def _path_parts(path: str) -> tuple[str, ...]:
    normalized = path.replace("\\", "/")
    return tuple(part.lower() for part in PurePosixPath(normalized).parts if part not in {"", "."})


def _is_code_file(path: str) -> bool:
    lowered = path.lower()
    suffix = PurePosixPath(lowered).suffix
    return suffix in CODE_FILE_EXTENSIONS


def classify_changed_file_path(path: str) -> tuple[bool, bool, bool]:
    """Infer whether a file path looks like source, test, or integration test code."""
    lowered = path.lower().replace("\\", "/")
    parts = _path_parts(lowered)
    file_name = parts[-1] if parts else lowered

    is_integration_test = any(part in INTEGRATION_DIR_HINTS for part in parts) or any(
        marker in file_name for marker in (".integration.", ".e2e.", ".acceptance.")
    )

    is_test = is_integration_test or any(part in TEST_DIR_HINTS for part in parts) or any(
        marker in file_name for marker in ("test_", "_test.", ".test.", ".spec.")
    )

    if not _is_code_file(path):
        return False, is_test, is_integration_test

    has_source_hint = any(part in SOURCE_DIR_HINTS for part in parts)
    blocked_by_non_source_dir = any(part in NON_SOURCE_DIR_HINTS for part in parts[:2])
    is_src = not is_test and (has_source_hint or not blocked_by_non_source_dir)

    return is_src, is_test, is_integration_test


def is_maintainer_like_author(author_association: str | None) -> bool:
    """Return True when the GitHub author association suggests maintainer-level access."""
    return (author_association or "").upper() in MAINTAINER_AUTHOR_ASSOCIATIONS


def is_committer_like_author(author_association: str | None) -> bool:
    """Return True when the GitHub author association suggests commit access."""
    return (author_association or "").upper() in COMMITTER_AUTHOR_ASSOCIATIONS


def author_matches_scope(
    author_association: str | None,
    author_scope: AuthorScope,
) -> bool:
    """Return whether an artifact should be kept for the chosen author scope."""
    if author_scope == "all":
        return True
    if author_scope == "maintainers":
        return is_maintainer_like_author(author_association)
    if author_scope == "committers":
        return is_committer_like_author(author_association)
    return True


def extract_artifact_reference_numbers(*texts: str) -> list[int]:
    """Extract referenced local GitHub issue or pull-request numbers from free-form text."""
    seen: OrderedDict[int, None] = OrderedDict()
    for text in texts:
        if not text:
            continue
        for pattern in ARTIFACT_REFERENCE_PATTERNS:
            for match in pattern.finditer(text):
                seen[int(match.group(1))] = None
    return list(seen)


@dataclass(slots=True)
class ChangedFile:
    """A normalized file touched by a GitHub pull request."""

    path: str
    additions: int
    deletions: int
    status: ChangedFileStatus
    is_src: bool
    is_test: bool
    is_integration_test: bool


def build_changed_file(
    path: str,
    *,
    additions: int,
    deletions: int,
    status: ChangedFileStatus,
) -> ChangedFile:
    """Create a changed-file record with centralized path classification."""
    is_src, is_test, is_integration_test = classify_changed_file_path(path)
    return ChangedFile(
        path=path,
        additions=additions,
        deletions=deletions,
        status=status,
        is_src=is_src,
        is_test=is_test,
        is_integration_test=is_integration_test,
    )


@dataclass(slots=True)
class HipArtifact:
    """A normalized issue or pull request used as HIP evidence input."""

    repo: str
    artifact_type: ArtifactType
    number: int
    title: str
    body: str = ""
    comments_text: str = ""
    commit_messages_text: str = ""
    author_login: str = ""
    author_association: str = "NONE"
    state: str = "open"
    merged: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None
    closed_at: datetime | None = None
    changed_files: list[ChangedFile] = field(default_factory=list)
    additions: int = 0
    deletions: int = 0
    labels: list[str] = field(default_factory=list)
    url: str = ""


@dataclass(slots=True)
class HipCandidate:
    """A candidate HIP reference extracted from one artifact."""

    artifact: HipArtifact
    hip_id: str
    extraction_source: str
    text_match_reason: str
    negative_context_flags: list[str] = field(default_factory=list)
    matched_sources: list[str] = field(default_factory=list)


@dataclass(slots=True)
class HipFeatureVector:
    """Engineered features for scoring one artifact-to-HIP candidate relationship."""

    repo: str
    artifact_type: ArtifactType
    artifact_number: int
    hip_id: str
    extraction_source: str
    text_match_reason: str
    explicit_hip_mention: bool
    hip_in_title: bool
    hip_in_body: bool
    hip_in_comments: bool
    hip_in_commit_messages: bool
    negative_phrase_unblock: bool
    negative_phrase_blocked: bool
    negative_phrase_follow_up: bool
    negative_phrase_prep: bool
    negative_phrase_refactor_only: bool
    negative_phrase_cleanup_only: bool
    has_feat_keyword: bool
    has_implement_keyword: bool
    has_support_keyword: bool
    src_files_changed_count: int
    test_files_changed_count: int
    integration_test_files_changed_count: int
    new_src_files_count: int
    new_test_files_count: int
    total_additions: int
    total_deletions: int
    merged: bool
    author_is_maintainer_like: bool
    author_is_committer_like: bool
    negative_context_flags: list[str] = field(default_factory=list)
    implementation_score_inputs: list[str] = field(default_factory=list)


@dataclass(slots=True)
class HipEvidence:
    """Scored artifact-level evidence for one HIP in one repository."""

    repo: str
    hip_id: str
    artifact_type: ArtifactType
    artifact_number: int
    hip_candidate_score: float
    implementation_score: float
    completion_score: float
    confidence_level: ConfidenceLevel
    reasons: list[str] = field(default_factory=list)


@dataclass(slots=True)
class HipRepoStatus:
    """Aggregated repository-level status for one HIP."""

    repo: str
    hip_id: str
    status: RepoHipStatus
    confidence_level: ConfidenceLevel
    supporting_artifact_numbers: list[int] = field(default_factory=list)
    rationale: list[str] = field(default_factory=list)
    last_evidence_at: datetime | None = None
