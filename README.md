# Analytics

## Overview

Stay up to date with hiero organisation activity and contributor diversity

This repository provides analytics for the [Hiero repositories](https://github.com/hiero-ledger).

## Setting Up Analytics Development

## Repository Setup

Before you begin, make sure you have:
- **Git** installed ([Download Git](https://git-scm.com/downloads))
- **Python 3.10+** installed ([Download Python](https://www.python.org/downloads/))
- A **GitHub account** ([Sign up](https://github.com/join))

### Step 1: Fork the Repository

Forking creates your own copy of the Hiero Python SDK that you can modify freely.

1. Go to [https://github.com/hiero-hackers/analytics](https://github.com/hiero-hackers/analytics)
2. Click the **Fork** button in the top-right corner
3. Select your GitHub account as the destination

You now have your own fork at `https://github.com/YOUR_USERNAME/hiero-hackers/analytics`

### Step 2: Clone Your Fork

Clone your fork to your local machine:

```bash
git clone https://github.com/YOUR_USERNAME/hiero-hackers/analytics.git
cd hiero-hackers/analytics
```

Replace `YOUR_USERNAME` with your actual GitHub username.

### Step 3: Add Upstream Remote

Connect your local repository to the original repository. This allows you to keep your fork synchronized with the latest changes.

```bash
git remote add upstream https://github.com/hiero-hackers/analytics.git
```

**What this does:**
- `origin` = your fork (where you push your changes)
- `upstream` = the original repository (where you pull updates from)

### Step 4: Verify Your Remotes

Check that both remotes are configured correctly:

```bash
git remote -v
```

You should see:
```
origin    https://github.com/YOUR_USERNAME/hiero-hackers/analytics.git (fetch)
origin    https://github.com/YOUR_USERNAME/hiero-hackers/analytics.git (push)
upstream  https://github.com/hiero-hackers/analytics.git (fetch)
upstream  https://github.com/hiero-hackers/analytics.git (push)
```

---

## Installation

#### Install uv

**On macOS/Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**On macOS (using Homebrew):**
```bash
brew install uv
```

**On Windows:**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Other installation methods:** [uv Installation Guide](https://docs.astral.sh/uv/getting-started/installation/)

#### Verify Installation

```bash
uv --version
```

## Install Dependencies

`uv` automatically manages the correct Python version based on the `.python-version` file in the project, so you don't need to worry about version conflicts.

Install project dependencies:

```bash
uv sync
```

**What this does:**
- Downloads and installs the correct Python version (if needed)
- Creates a virtual environment
- Installs all project dependencies
- Installs development tools (pytest, ruff, etc.)

## Environment Setup

Create a fine-grained personal access token [Personal Acess Tokens Info](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) and [Create Personal Access Token](https://github.com/settings/personal-access-tokens). Enable it for public repositorites and do not enable any extra access.

Create a `.env` file in the project root, copy and save your token.

```bash
GITHUB_TOKEN=yours
```

You'll need this token to increase your API rate limit when interacting with Github data. 

### Test Setup

Run the test suite to ensure everything is working:

```bash
uv run pytest
```

### HIP Progression

The HIP progression pipeline now analyzes the official Hiero Improvement Proposal catalog against SDK repositories with deterministic, evidence-tiered inference.

`High confidence` is intentionally rare. A repo-level `completed` call requires strong implementation evidence, test backing, merged state, and corroborating completion evidence such as a closed linked issue or changelog/docs update. Weak mentions, bot noise, docs-only changes, and follow-up or revert language are downweighted or penalized.

Run the repo-scoped pipeline:

```bash
uv run python -m hiero_analytics.run_hip_progression_for_repo --owner hiero-ledger --repo hiero-sdk-js
```

Run the default SDK batch pipeline and benchmark evaluation:

```bash
uv run python -m hiero_analytics.run_hip_progression_batch --evaluate
```

Useful options:

```bash
uv run python -m hiero_analytics.run_hip_progression_for_repo --limit 25 --author-scope committers
```

```bash
uv run python -m hiero_analytics.run_hip_progression_batch --repo hiero-sdk-js --repo hiero-sdk-python --limit 25
```

Use the smaller end-user bundle by default, or opt into the full debug bundle when needed:

```bash
uv run python -m hiero_analytics.run_hip_progression_batch --evaluate --latest-hip-limit 10 --export-profile review
```

```bash
uv run python -m hiero_analytics.run_hip_progression_batch --evaluate --latest-hip-limit 20 --export-profile full
```

The default scope now keeps the newest 10 official HIPs, ordered descending by HIP number, so reviewers are not overwhelmed by the full historical catalog. Increase `--latest-hip-limit` when you want a wider slice such as 20.

The repo runner writes outputs under `outputs/hip_progression/<owner>_<repo>/`. The batch runner writes outputs under `outputs/hip_progression/batch/`.

Top-level repo outputs now stay small and stakeholder-facing:
- `repo_hip_status.csv` with one row per HIP in scope for the repo, sorted by HIP id descending.
- `repo_hip_issues.csv` with issue URL, HIP likelihood score, and development status for HIP-linked issues.
- `repo_hip_status.png` as a stacked development-status view for the scoped HIPs.

Top-level batch outputs focus on the SDK group and org rollup:
- `sdk_hip_status_matrix.csv` with one row per HIP and one status flag column per SDK.
- `sdk_hip_rollup.csv` with raised-counts, in-progress counts, completed counts, and completion rate across the selected SDKs.
- `sdk_hip_development_status.png` with stacked `not_raised`, `issue_raised`, `in_progress`, and `completed` counts by HIP.
- `sdk_hip_completion_rate.png` with HIP completion rate across the selected SDKs.
- `approved_hip_org_rollup.csv` with approved HIP counts across the selected repos plus completed repo lists.
- `approved_hip_org_rollup.png` with the approved-HIP stacked rollup.

Accuracy and debug material now lives under `evaluation/`:
- `artifact_predictions.csv`
- `repo_predictions.csv`
- `manual_accuracy_review.csv`
- `review_breakdown.csv`
- `accuracy_summary.csv`
- `benchmark_metrics.csv`, `benchmark_confusion_matrix.csv`, and `benchmark_per_status.csv` when `--evaluate` is enabled

Deep diagnostics such as `artifacts.csv`, `artifact_features.csv`, `artifact_assessments.csv`, and `evidence_detail.csv` are only written under `debug/` when `--export-profile full` is used.

The scoring remains conservative. Stronger evidence now comes from direct HIP references in pull requests, implementation language such as `feat`, `adds`, or `introduces`, substantial source deltas, linked maintainer-owned artifacts, and implementation shapes that touch both source and tests. Negative contexts such as `unblock`, `follow-up`, `prep`, `cleanup only`, and `reverted` reduce confidence instead of inflating it.

Benchmark splits use a chronological 80/20 train-test partition separately for issues and pull requests so the newest slice stays untouched for later validation.

For status semantics, evidence tiers, evaluation details, and example output locations, see [docs/hip_progression.md](docs/hip_progression.md).
---

## License

- Available under the **Apache License, Version 2.0 (Apache-2.0)*
