"""Helpers for writing analytics tables to disk."""

from __future__ import annotations

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

    if df.empty and len(df.columns) == 0:
        path.write_text("_No data._\n", encoding="utf-8")
        return

    headers = [_format_markdown_cell(column) for column in df.columns]
    separator = ["---"] * len(headers)
    lines = [
        f"| {' | '.join(headers)} |",
        f"| {' | '.join(separator)} |",
    ]

    for row in df.itertuples(index=False, name=None):
        cells = [_format_markdown_cell(value) for value in row]
        lines.append(f"| {' | '.join(cells)} |")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
