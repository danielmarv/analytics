"""Domain models and shared helpers for HIP progression analysis."""

from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal

ArtifactType = Literal["pull_request", "issue"]
ArtifactSegmentKind = Literal[
    "title",
    "body",
    "issue_comment",
    "review_comment",
    "commit_message",
    "changed_file",
    "linked_artifact",
]
ChangedFileStatus = Literal["added", "modified", "removed", "renamed"]
AuthorScope = Literal["all", "maintainers", "committers"]
ConfidenceLevel = Literal["low", "high"]
DevelopmentStatus = Literal["not_raised", "raised", "in_progress", "completed"]

MAINTAINER_AUTHOR_ASSOCIATIONS = {"OWNER", "MEMBER"}
COMMITTER_AUTHOR_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}

SOURCE_DIR_HINTS = {"src", "lib", "app", "package", "packages", "sdk", "client", "clients"}
TEST_DIR_HINTS = {"test", "tests", "__tests__", "spec", "specs"}
INTEGRATION_DIR_HINTS = {"integration", "integrations", "e2e", "end-to-end", "acceptance"}
DOC_DIR_HINTS = {
    ".github", "docs", "documentation", "examples", "example",
    "samples", "sample", "scripts", "script", "fixtures", "fixture",
}
NON_SOURCE_DIR_HINTS = DOC_DIR_HINTS | {"dist", "build", "coverage"}
CODE_FILE_EXTENSIONS = {
    ".c", ".cc", ".cpp", ".cs", ".go", ".h", ".hpp", ".java",
    ".js", ".jsx", ".kt", ".mjs", ".py", ".rb", ".rs", ".swift", ".ts", ".tsx",
}
ARTIFACT_REFERENCE_PATTERNS = (
    re.compile(r"(?:^|[^\w])#(\d+)\b"),
    re.compile(r"github\.com/[^/\s]+/[^/\s]+/(?:issues|pull)/(\d+)\b", re.IGNORECASE),
    re.compile(r"\b[\w.-]+/[\w.-]+#(\d+)\b"),
)
HIP_ID_PATTERN = re.compile(r"\bhip[-\s]?(\d+)\b", re.IGNORECASE)
SUBSTANTIAL_DELTA_MIN = 500


def _path_parts(path: str) -> tuple[str, ...]:
    normalized = path.replace("\\", "/")
    return tuple(part.lower() for part in PurePosixPath(normalized).parts if part not in {"", "."})


def _is_code_file(path: str) -> bool:
    suffix = PurePosixPath(path.lower()).suffix
    return suffix in CODE_FILE_EXTENSIONS


def is_docs_file_path(path: str) -> bool:
    """Return True when a changed file looks documentation-oriented."""
    parts = _path_parts(path)
    file_name = parts[-1] if parts else path.lower()
    suffix = PurePosixPath(file_name).suffix
    if suffix in {".md", ".mdx", ".rst", ".txt"}:
        return True
    return any(part in DOC_DIR_HINTS for part in parts[:3])


def is_changelog_file_path(path: str) -> bool:
    """Return True when a changed file looks like a changelog or release note."""
    file_name = PurePosixPath(path.lower().replace("\\", "/")).name
    return file_name in {"changelog.md", "changes.md", "release-notes.md", "release_notes.md"}


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


def author_matches_scope(author_association: str | None, author_scope: AuthorScope) -> bool:
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


def normalize_hip_id(raw_value: str) -> str:
    """Normalize a HIP identifier to the canonical ``HIP-1234`` form."""
    raw_text = str(raw_value or "").strip()
    match = HIP_ID_PATTERN.search(raw_text)
    if not match and raw_text.isdigit():
        return f"HIP-{int(raw_text)}"
    if not match:
        raise ValueError(f"Unable to normalize HIP identifier from {raw_value!r}")
    return f"HIP-{int(match.group(1))}"


def hip_sort_key(hip_id: str) -> tuple[int, str]:
    """Return a stable numeric-first sort key for a HIP identifier."""
    try:
        normalized = normalize_hip_id(hip_id)
    except ValueError:
        return (10**9, hip_id)
    return (int(normalized.split("-", maxsplit=1)[1]), normalized)


def flatten_text(values: list[str]) -> str:
    """Join non-empty text values into one deterministic block."""
    return "\n\n".join(value for value in values if value)


@dataclass(slots=True)
class ArtifactComment:
    """Structured issue or pull-request comment text."""

    body: str
    source_kind: Literal["issue_comment", "review_comment"] = "issue_comment"
    author_login: str = ""
    author_association: str = "NONE"
    created_at: datetime | None = None
    url: str = ""
    is_bot: bool = False


@dataclass(slots=True)
class ArtifactCommit:
    """Structured pull-request commit metadata."""

    message: str
    sha: str = ""
    authored_at: datetime | None = None


@dataclass(slots=True)
class ArtifactTextSegment:
    """One searchable text segment from an artifact."""

    source_kind: ArtifactSegmentKind
    source_id: str
    text: str
    author_login: str = ""
    author_association: str = "NONE"
    created_at: datetime | None = None
    is_bot: bool = False


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
    is_docs: bool
    is_changelog: bool


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
        is_docs=is_docs_file_path(path),
        is_changelog=is_changelog_file_path(path),
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
    comments: list[ArtifactComment] = field(default_factory=list)
    commits: list[ArtifactCommit] = field(default_factory=list)
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
    linked_artifact_numbers: list[int] = field(default_factory=list)
    url: str = ""

    def activity_timestamp(self) -> datetime | None:
        """Return the best timestamp for artifact ordering."""
        return self.updated_at or self.closed_at or self.created_at

    def has_code_changes(self) -> bool:
        """Return True when the artifact touches source files."""
        return any(f.is_src for f in self.changed_files)

    def has_test_changes(self) -> bool:
        """Return True when the artifact touches test files."""
        return any(f.is_test for f in self.changed_files)

    def has_substantial_delta(self) -> bool:
        """Return True when the artifact has a large code change (500+ lines)."""
        return (self.additions + self.deletions) >= SUBSTANTIAL_DELTA_MIN

    def text_segments(self) -> list[ArtifactTextSegment]:
        """Return structured text segments for candidate extraction and evidence linking."""
        segments: list[ArtifactTextSegment] = []
        if self.title:
            segments.append(ArtifactTextSegment(source_kind="title", source_id="title", text=self.title))
        if self.body:
            segments.append(ArtifactTextSegment(source_kind="body", source_id="body", text=self.body))

        return segments


@dataclass(slots=True)
class HipCatalogEntry:
    """One official HIP from the upstream catalog."""

    hip_id: str
    number: int
    title: str
    status: str
    hip_type: str
    category: str
    created: str = ""
    updated: str = ""
    discussions_to: str = ""
    requested_by: str = ""
    url: str = ""


@dataclass(slots=True)
class HipCandidate:
    """A candidate HIP association extracted from one artifact."""

    artifact: HipArtifact
    hip_id: str
    source: str
    is_propagated: bool = False


@dataclass(slots=True)
class ArtifactHipAssessment:
    """Artifact-level HIP classification: status + confidence."""

    repo: str
    hip_id: str
    artifact_type: ArtifactType
    artifact_number: int
    status: DevelopmentStatus
    confidence: ConfidenceLevel
    has_code: bool = False
    has_tests: bool = False
    merged: bool = False
    is_committer: bool = False


@dataclass(slots=True)
class HipRepoStatus:
    """Aggregated repository-level status for one HIP."""

    repo: str
    hip_id: str
    status: DevelopmentStatus
    confidence: ConfidenceLevel
    supporting_artifact_numbers: list[int] = field(default_factory=list)
    top_artifacts: list[str] = field(default_factory=list)
    last_evidence_at: datetime | None = None


# Backward-compatible aliases used by the data loader and evaluation code.
HipEvidence = ArtifactHipAssessment


@dataclass(frozen=True, slots=True)
class RepositoryTargetConfig:
    """Configuration for running HIP progression on one repository."""

    owner: str
    repo: str
    include_issues: bool = True
    include_prs: bool = True
    author_scope: AuthorScope = "all"

    @property
    def full_name(self) -> str:
        """Return the GitHub ``owner/repo`` identifier."""
        return f"{self.owner}/{self.repo}"
