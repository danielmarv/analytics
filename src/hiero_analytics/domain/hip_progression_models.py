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
ConfidenceLevel = Literal["low", "medium", "high"]
EvidencePolarity = Literal["positive", "negative", "neutral"]
EvidenceTier = Literal["tier_1", "tier_2", "tier_3", "tier_4", "tier_5"]
RepoHipStatus = Literal["not_started", "in_progress", "completed", "unknown", "conflicting"]

MAINTAINER_AUTHOR_ASSOCIATIONS = {"OWNER", "MEMBER"}
COMMITTER_AUTHOR_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}

SOURCE_DIR_HINTS = {"src", "lib", "app", "package", "packages", "sdk", "client", "clients"}
TEST_DIR_HINTS = {"test", "tests", "__tests__", "spec", "specs"}
INTEGRATION_DIR_HINTS = {"integration", "integrations", "e2e", "end-to-end", "acceptance"}
DOC_DIR_HINTS = {
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
}
CHANGELOG_FILE_HINTS = {
    "changelog.md",
    "changes.md",
    "release-notes.md",
    "release_notes.md",
    "release-notes.txt",
}
NON_SOURCE_DIR_HINTS = DOC_DIR_HINTS | {"dist", "build", "coverage"}
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
HIP_ID_PATTERN = re.compile(r"\bhip[-\s]?(\d+)\b", re.IGNORECASE)


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
    return file_name in CHANGELOG_FILE_HINTS


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

    def text_segments(self) -> list[ArtifactTextSegment]:
        """Return structured text segments for candidate extraction and evidence linking."""
        segments: list[ArtifactTextSegment] = []
        if self.title:
            segments.append(ArtifactTextSegment(source_kind="title", source_id="title", text=self.title))
        if self.body:
            segments.append(ArtifactTextSegment(source_kind="body", source_id="body", text=self.body))

        if self.comments:
            for index, comment in enumerate(self.comments, start=1):
                if not comment.body:
                    continue
                segments.append(
                    ArtifactTextSegment(
                        source_kind=comment.source_kind,
                        source_id=f"{comment.source_kind}:{index}",
                        text=comment.body,
                        author_login=comment.author_login,
                        author_association=comment.author_association,
                        created_at=comment.created_at,
                        is_bot=comment.is_bot,
                    )
                )
        elif self.comments_text:
            segments.append(
                ArtifactTextSegment(
                    source_kind="issue_comment" if self.artifact_type == "issue" else "review_comment",
                    source_id="comments",
                    text=self.comments_text,
                )
            )

        if self.commits:
            for index, commit in enumerate(self.commits, start=1):
                if not commit.message:
                    continue
                segments.append(
                    ArtifactTextSegment(
                        source_kind="commit_message",
                        source_id=f"commit:{index}",
                        text=commit.message,
                        created_at=commit.authored_at,
                    )
                )
        elif self.commit_messages_text:
            segments.append(
                ArtifactTextSegment(
                    source_kind="commit_message",
                    source_id="commit_messages",
                    text=self.commit_messages_text,
                )
            )

        for changed_file in self.changed_files:
            segments.append(
                ArtifactTextSegment(
                    source_kind="changed_file",
                    source_id=changed_file.path,
                    text=changed_file.path,
                )
            )
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
class HipMention:
    """One candidate HIP mention or file hint observed inside an artifact."""

    hip_id: str
    source_kind: ArtifactSegmentKind
    source_id: str
    matched_text: str
    phrase_context: str
    is_explicit_match: bool
    is_semantic_match: bool
    is_negative_context: bool
    negative_context_flags: list[str] = field(default_factory=list)
    linked_artifact_numbers: list[int] = field(default_factory=list)
    is_bot: bool = False


@dataclass(slots=True)
class HipCandidate:
    """A candidate HIP association extracted from one artifact."""

    artifact: HipArtifact
    hip_id: str
    extraction_source: str
    text_match_reason: str
    mentions: list[HipMention] = field(default_factory=list)
    negative_context_flags: list[str] = field(default_factory=list)
    matched_sources: list[str] = field(default_factory=list)
    linked_artifact_numbers: list[int] = field(default_factory=list)
    propagated_from_artifacts: list[int] = field(default_factory=list)


@dataclass(slots=True)
class ArtifactEvidence:
    """One explainable evidence item for an artifact-to-HIP association."""

    hip_id: str
    artifact_type: ArtifactType
    artifact_number: int
    source_artifact: str
    evidence_type: str
    evidence_tier: EvidenceTier
    source_kind: ArtifactSegmentKind | str
    short_rationale: str
    polarity: EvidencePolarity
    confidence_contribution: float
    top_reasons: list[str] = field(default_factory=list)
    uncertainty_reasons: list[str] = field(default_factory=list)
    fingerprint: str = ""


@dataclass(slots=True)
class HipFeatureVector:
    """Engineered features summarizing one artifact-to-HIP association."""

    repo: str
    hip_id: str
    artifact_type: ArtifactType
    artifact_number: int
    evidence_count: int
    positive_evidence_count: int
    negative_evidence_count: int
    tier_1_count: int
    tier_2_count: int
    tier_3_count: int
    tier_4_count: int
    tier_5_count: int
    direct_mention_count: int
    semantic_phrase_count: int
    propagated_mention_count: int
    bot_mention_count: int
    src_files_changed_count: int
    test_files_changed_count: int
    integration_test_files_changed_count: int
    docs_files_changed_count: int
    changelog_files_changed_count: int
    new_src_files_count: int
    new_test_files_count: int
    merged: bool
    linked_artifact_numbers: list[int] = field(default_factory=list)
    has_direct_reference: bool = False
    has_code_evidence: bool = False
    has_test_evidence: bool = False
    has_docs_only_change: bool = False
    has_changelog_update: bool = False
    has_negative_blocked: bool = False
    has_negative_follow_up: bool = False
    has_negative_prep: bool = False
    has_negative_refactor_only: bool = False
    has_negative_cleanup_only: bool = False
    has_negative_reverted: bool = False
    evidence_records: list[ArtifactEvidence] = field(default_factory=list)
    top_evidence_types: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ConfidenceBreakdown:
    """Explainable confidence payload attached to artifact and repo assessments."""

    confidence_score: float
    confidence_level: ConfidenceLevel
    top_reasons: list[str] = field(default_factory=list)
    uncertainty_reasons: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ArtifactHipAssessment:
    """Final artifact-level inference for one HIP."""

    repo: str
    hip_id: str
    artifact_type: ArtifactType
    artifact_number: int
    status: RepoHipStatus
    progress_stage: str
    confidence_score: float
    confidence_level: ConfidenceLevel
    evidence_count: int
    positive_evidence_count: int
    negative_evidence_count: int
    evidence_records: list[ArtifactEvidence] = field(default_factory=list)
    top_reasons: list[str] = field(default_factory=list)
    uncertainty_reasons: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RepoHipAssessment:
    """Aggregated repository-level status for one HIP."""

    repo: str
    hip_id: str
    status: RepoHipStatus
    rag_label: str
    confidence_score: float
    confidence_level: ConfidenceLevel
    evidence_count: int
    top_artifacts: list[str] = field(default_factory=list)
    supporting_artifact_numbers: list[int] = field(default_factory=list)
    top_reasons: list[str] = field(default_factory=list)
    uncertainty_reasons: list[str] = field(default_factory=list)
    reviewer_notes: str = ""
    rationale: list[str] = field(default_factory=list)
    last_evidence_at: datetime | None = None


HipEvidence = ArtifactHipAssessment
HipRepoStatus = RepoHipAssessment


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


@dataclass(slots=True)
class ArtifactBenchmarkExpectation:
    """Expected artifact-level status in the benchmark dataset."""

    repo: str
    artifact_type: ArtifactType
    artifact_number: int
    hip_id: str
    expected_status: RepoHipStatus
    rationale: str = ""


@dataclass(slots=True)
class RepoBenchmarkExpectation:
    """Expected repo-level status in the benchmark dataset."""

    repo: str
    hip_id: str
    expected_status: RepoHipStatus
    rationale: str = ""
