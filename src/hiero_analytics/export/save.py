"""Helpers for writing analytics tables to disk."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def save_dataframe(
    df: pd.DataFrame,
    path: Path,
) -> None:
    """
    Save a dataframe to a CSV file.

    Args:
        df: The dataframe to save.
        path: The path where the CSV file will be saved.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _format_markdown_cell(value: object) -> str:
    """Convert a dataframe cell value into a markdown-safe string."""
    if pd.isna(value):
        return ""

    text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("|", "\\|")
    return text.replace("\n", "<br>")


def dataframe_to_markdown_table(df: pd.DataFrame) -> str:
    """Render a dataframe to a GitHub-flavored markdown table string."""
    if df.empty and len(df.columns) == 0:
        return "_No data._\n"

    headers = [_format_markdown_cell(column) for column in df.columns]
    separator = ["---"] * len(headers)
    lines = [
        f"| {' | '.join(headers)} |",
        f"| {' | '.join(separator)} |",
    ]

    for row in df.itertuples(index=False, name=None):
        cells = [_format_markdown_cell(value) for value in row]
        lines.append(f"| {' | '.join(cells)} |")
    return "\n".join(lines) + "\n"


def save_markdown_table(
    df: pd.DataFrame,
    path: Path,
) -> None:
    """
    Save a dataframe to a GitHub-flavored markdown table.

    Args:
        df: The dataframe to save.
        path: The path where the markdown file will be saved.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dataframe_to_markdown_table(df), encoding="utf-8")


def save_json(payload: object, path: Path) -> None:
    """Save structured data to a JSON file with stable formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
