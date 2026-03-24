"""Tests for governance-config role mapping helpers."""

from hiero_analytics.data_sources.governance_config import build_repo_role_lookup, permission_to_role


def test_permission_to_role_maps_repo_permissions():
    """Repository permissions should normalize into maintainer-pipeline roles."""
    assert permission_to_role("triage") == "triage"
    assert permission_to_role("write") == "committer"
    assert permission_to_role("maintain") == "maintainer"
    assert permission_to_role("admin") == "maintainer"
    assert permission_to_role("read") is None


def test_build_repo_role_lookup_assigns_highest_role_per_user():
    """Repo-affined teams should resolve each user to their highest repo role."""
    config = {
        "teams": [
            {
                "name": "repo-a-triage",
                "maintainers": ["triage-lead"],
                "members": ["alice"],
            },
            {
                "name": "repo-a-committers",
                "maintainers": ["commit-lead"],
                "members": ["alice", "bob"],
            },
            {
                "name": "repo-a-maintainers",
                "maintainers": ["maint-lead"],
                "members": ["carol"],
            },
        ],
        "repositories": [
            {
                "name": "repo-a",
                "teams": {
                    "repo-a-triage": "triage",
                    "repo-a-committers": "write",
                    "repo-a-maintainers": "maintain",
                },
            }
        ],
    }

    repo_role_lookup = build_repo_role_lookup(config)

    assert repo_role_lookup["repo-a"]["triage-lead"] == "triage"
    assert repo_role_lookup["repo-a"]["alice"] == "committer"
    assert repo_role_lookup["repo-a"]["bob"] == "committer"
    assert repo_role_lookup["repo-a"]["maint-lead"] == "maintainer"
    assert repo_role_lookup["repo-a"]["carol"] == "maintainer"


def test_build_repo_role_lookup_excludes_global_and_more_specific_repo_teams():
    """Broad teams and teams owned by a more specific repo should be ignored."""
    config = {
        "teams": [
            {
                "name": "solo-admins",
                "maintainers": ["solo-admin"],
                "members": [],
            },
            {
                "name": "solo-docs-admins",
                "maintainers": ["docs-admin"],
                "members": [],
            },
            {
                "name": "github-maintainers",
                "maintainers": ["global-admin"],
                "members": [],
            },
        ],
        "repositories": [
            {
                "name": "solo",
                "teams": {
                    "solo-admins": "admin",
                    "solo-docs-admins": "admin",
                    "github-maintainers": "maintain",
                },
            },
            {
                "name": "solo-docs",
                "teams": {
                    "solo-docs-admins": "admin",
                    "github-maintainers": "maintain",
                },
            },
        ],
    }

    repo_role_lookup = build_repo_role_lookup(config)

    assert repo_role_lookup["solo"] == {"solo-admin": "maintainer"}
    assert repo_role_lookup["solo-docs"] == {"docs-admin": "maintainer"}


def test_build_repo_role_lookup_normalizes_usernames():
    """GitHub usernames should be trimmed and matched case-insensitively."""
    config = {
        "teams": [
            {
                "name": "hiero-website-committers",
                "maintainers": ["LeadMaintainer "],
                "members": ["ExplorerIII"],
            }
        ],
        "repositories": [
            {
                "name": "hiero-website",
                "teams": {
                    "hiero-website-committers": "write",
                },
            }
        ],
    }

    repo_role_lookup = build_repo_role_lookup(config)

    assert repo_role_lookup["hiero-website"]["leadmaintainer"] == "committer"
    assert repo_role_lookup["hiero-website"]["exploreriii"] == "committer"
