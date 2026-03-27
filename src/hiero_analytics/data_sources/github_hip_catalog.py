"""Load the official HIP catalog from the upstream Hiero HIP repository."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import requests
import yaml

from hiero_analytics.config.hip_progression import (
    OFFICIAL_HIP_BASE_URL,
    OFFICIAL_HIP_DIRECTORY,
    OFFICIAL_HIP_REPOSITORY,
)
from hiero_analytics.data_sources.cache import load_records_cache, save_records_cache
from hiero_analytics.data_sources.github_client import GitHubClient, github_headers
from hiero_analytics.domain.hip_progression_models import HipCatalogEntry, normalize_hip_id


def _front_matter_and_body(markdown: str) -> tuple[dict[str, Any], str]:
    """Split markdown into YAML front matter and body."""
    if not markdown.startswith("---\n"):
        return {}, markdown
    _, remainder = markdown.split("---\n", maxsplit=1)
    front_matter_text, _, body = remainder.partition("\n---\n")
    front_matter = yaml.safe_load(front_matter_text) or {}
    if not isinstance(front_matter, dict):
        return {}, body
    return front_matter, body


def _catalog_path_label(download_url: str) -> str:
    """Return a user-friendly path label for a raw HIP markdown URL."""
    return download_url.rsplit("/", maxsplit=1)[-1]


def _download_text(url: str, *, client: GitHubClient | None = None) -> str:
    """Fetch text content from GitHub with the standard headers."""
    if client is not None:
        response = client.session.get(url, timeout=20)
    else:
        response = requests.get(url, timeout=20, headers=github_headers())
    response.raise_for_status()
    return response.text


def _entry_from_front_matter(front_matter: dict[str, Any], *, path_label: str) -> HipCatalogEntry:
    """Build a catalog entry from parsed markdown front matter."""
    hip_id = normalize_hip_id(str(front_matter.get("hip", path_label)))
    number = int(hip_id.split("-", maxsplit=1)[1])
    return HipCatalogEntry(
        hip_id=hip_id,
        number=number,
        title=str(front_matter.get("title") or "").strip(),
        status=str(front_matter.get("status") or "").strip(),
        hip_type=str(front_matter.get("type") or "").strip(),
        category=str(front_matter.get("category") or "").strip(),
        created=str(front_matter.get("created") or "").strip(),
        updated=str(front_matter.get("updated") or "").strip(),
        discussions_to=str(front_matter.get("discussions-to") or "").strip(),
        requested_by=str(front_matter.get("requested-by") or "").strip(),
        url=f"{OFFICIAL_HIP_BASE_URL}/{path_label}",
    )


def _catalog_listing(client: GitHubClient) -> list[dict[str, Any]]:
    """Return the official HIP markdown file listing."""
    owner, repo = OFFICIAL_HIP_REPOSITORY.split("/", maxsplit=1)
    response = client.get(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{OFFICIAL_HIP_DIRECTORY}"
    )
    if not isinstance(response, list):
        return []
    return [item for item in response if isinstance(item, dict) and str(item.get("name", "")).endswith(".md")]


def fetch_official_hip_catalog(
    client: GitHubClient,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[HipCatalogEntry]:
    """Fetch and cache the official HIP catalog."""
    cache_parameters = {
        "source": OFFICIAL_HIP_REPOSITORY,
        "directory": OFFICIAL_HIP_DIRECTORY,
    }
    cached = load_records_cache(
        "official_hip_catalog",
        OFFICIAL_HIP_REPOSITORY.replace("/", "_"),
        cache_parameters,
        HipCatalogEntry,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    entries: list[HipCatalogEntry] = []
    for item in _catalog_listing(client):
        download_url = str(item.get("download_url") or "")
        if not download_url:
            continue
        front_matter, _body = _front_matter_and_body(_download_text(download_url, client=client))
        if not front_matter:
            continue
        entries.append(
            _entry_from_front_matter(front_matter, path_label=_catalog_path_label(download_url))
        )

    entries.sort(key=lambda entry: entry.number)
    save_records_cache(
        "official_hip_catalog",
        OFFICIAL_HIP_REPOSITORY.replace("/", "_"),
        cache_parameters,
        HipCatalogEntry,
        entries,
        use_cache=use_cache,
    )
    return entries


def load_catalog_snapshot(path: Path) -> list[HipCatalogEntry]:
    """Load a benchmark catalog snapshot from disk."""
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Catalog snapshot at {path} must be a list.")
    return [
        HipCatalogEntry(
            hip_id=normalize_hip_id(str(item["hip_id"])),
            number=int(item.get("number") or normalize_hip_id(str(item["hip_id"])).split("-", maxsplit=1)[1]),
            title=str(item.get("title") or ""),
            status=str(item.get("status") or ""),
            hip_type=str(item.get("hip_type") or ""),
            category=str(item.get("category") or ""),
            created=str(item.get("created") or ""),
            updated=str(item.get("updated") or ""),
            discussions_to=str(item.get("discussions_to") or ""),
            requested_by=str(item.get("requested_by") or ""),
            url=str(item.get("url") or ""),
        )
        for item in payload
        if isinstance(item, dict) and item.get("hip_id")
    ]
