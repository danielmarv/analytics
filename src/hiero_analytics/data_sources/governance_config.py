from __future__ import annotations

import os
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
                members.update(user for user in values if isinstance(user, str) and user)
        team_members[name] = members

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
