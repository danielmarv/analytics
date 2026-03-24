"""Helpers for mapping governance config teams to repo-scoped contributor roles."""

from __future__ import annotations

import os
import re
from collections import defaultdict
from typing import Any

import requests
import yaml

from hiero_analytics.config.github import HTTP_TIMEOUT_SECONDS

GOVERNANCE_CONFIG_URL = os.getenv(
    "GOVERNANCE_CONFIG_URL",
    "https://raw.githubusercontent.com/hiero-ledger/governance/main/config.yaml",
)

ROLE_PRIORITY = {
    "general_user": 0,
    "triage": 1,
    "committer": 2,
    "maintainer": 3,
}


def _normalize_username(user: str) -> str:
    """Normalize GitHub logins for case-insensitive matching."""
    return user.strip().lower()


def _tokenize_name(value: str) -> tuple[str, ...]:
    """Split a governance name into normalized alphanumeric tokens."""
    return tuple(token for token in re.split(r"[^a-z0-9]+", value.lower()) if token)


def _best_matching_repo_for_team(
    team_name: str,
    repo_name_tokens: dict[str, tuple[str, ...]],
) -> str | None:
    """Return the most specific repository name prefixed by the team name."""
    team_tokens = _tokenize_name(team_name)

    best_repo: str | None = None
    best_length = 0
    for repo_name, tokens in repo_name_tokens.items():
        if len(tokens) > len(team_tokens):
            continue
        if team_tokens[: len(tokens)] != tokens:
            continue
        if len(tokens) > best_length:
            best_repo = repo_name
            best_length = len(tokens)

    return best_repo


def fetch_governance_config(url: str = GOVERNANCE_CONFIG_URL) -> dict[str, Any]:
    """Fetch and parse the Hiero governance config file."""
    response = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = yaml.safe_load(response.text)

    if not isinstance(data, dict):
        raise ValueError("Governance config did not parse into a mapping")

    return data


def build_repo_role_lookup(config: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Build repo -> user -> highest governance role lookup from config.yaml."""
    teams = config.get("teams", [])
    repositories = config.get("repositories", [])
    repo_name_tokens = {
        repo["name"]: _tokenize_name(repo["name"])
        for repo in repositories
        if isinstance(repo, dict) and isinstance(repo.get("name"), str)
    }

    team_members: dict[str, set[str]] = {}
    for team in teams:
        if not isinstance(team, dict):
            continue

        name = team.get("name")
        if not isinstance(name, str):
            continue

        members = set()
        for field in ("maintainers", "members"):
            values = team.get(field, [])
            if isinstance(values, list):
                members.update(
                    _normalize_username(user) for user in values if isinstance(user, str) and user
                )
        team_members[name] = members

    team_repo_affinity = {
        team_name: _best_matching_repo_for_team(team_name, repo_name_tokens)
        for team_name in team_members
    }

    repo_roles: dict[str, dict[str, str]] = {}
    for repo in repositories:
        if not isinstance(repo, dict):
            continue

        repo_name = repo.get("name")
        assignments = repo.get("teams", {})
        if not isinstance(repo_name, str) or not isinstance(assignments, dict):
            continue

        user_roles: dict[str, str] = {}
        for team_name, permission in assignments.items():
            if team_repo_affinity.get(team_name) != repo_name:
                continue

            role = permission_to_role(permission)
            if role is None:
                continue

            for user in team_members.get(team_name, set()):
                current_role = user_roles.get(user)
                if current_role is None or ROLE_PRIORITY[role] > ROLE_PRIORITY[current_role]:
                    user_roles[user] = role

        repo_roles[repo_name] = user_roles

    return repo_roles


def permission_to_role(permission: Any) -> str | None:
    """Normalize governance repo permissions into chart roles."""
    if not isinstance(permission, str):
        return None

    normalized = permission.lower()
    if normalized == "triage":
        return "triage"
    if normalized == "write":
        return "committer"
    if normalized in {"maintain", "admin"}:
        return "maintainer"
    return None


def summarize_role_counts(repo_role_lookup: dict[str, dict[str, str]]) -> dict[str, int]:
    """Return distinct user counts by highest role across all repositories."""
    users: dict[str, str] = {}
    for repo_lookup in repo_role_lookup.values():
        for user, role in repo_lookup.items():
            current_role = users.get(user)
            if current_role is None or ROLE_PRIORITY[role] > ROLE_PRIORITY[current_role]:
                users[user] = role

    counts: dict[str, int] = defaultdict(int)
    for role in users.values():
        counts[role] += 1
    return dict(counts)


def count_distinct_role_holders_by_role(
    repo_role_lookup: dict[str, dict[str, str]],
) -> dict[str, int]:
    """Return distinct user counts for each role across all repositories."""
    users_by_role: dict[str, set[str]] = defaultdict(set)
    for repo_lookup in repo_role_lookup.values():
        for user, role in repo_lookup.items():
            users_by_role[role].add(user)

    return {role: len(users) for role, users in users_by_role.items()}
