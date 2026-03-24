from hiero_analytics.data_sources.governance_config import build_repo_role_lookup, permission_to_role


def test_permission_to_role_maps_repo_permissions():
    assert permission_to_role("triage") == "triage"
    assert permission_to_role("write") == "committer"
    assert permission_to_role("maintain") == "maintainer"
    assert permission_to_role("admin") == "maintainer"
    assert permission_to_role("read") is None


def test_build_repo_role_lookup_assigns_highest_role_per_user():
    config = {
        "teams": [
            {
                "name": "repo-triage",
                "maintainers": ["triage-lead"],
                "members": ["alice"],
            },
            {
                "name": "repo-committers",
                "maintainers": ["commit-lead"],
                "members": ["alice", "bob"],
            },
            {
                "name": "repo-maintainers",
                "maintainers": ["maint-lead"],
                "members": ["carol"],
            },
        ],
        "repositories": [
            {
                "name": "repo-a",
                "teams": {
                    "repo-triage": "triage",
                    "repo-committers": "write",
                    "repo-maintainers": "maintain",
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
