from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from hiero_analytics.config.charts import DEFAULT_DPI, DEFAULT_FIGSIZE

from .style import apply_style


def _require_non_empty(df: pd.DataFrame) -> None:
    """
    Ensure the DataFrame is not empty.
    """
    if df.empty:
        raise ValueError("DataFrame is empty")


def _require_columns(df: pd.DataFrame, *columns: str) -> None:
    """
    Ensure required columns exist in the DataFrame.
    """
    missing = [col for col in columns if col not in df.columns]

    if missing:
        raise KeyError(f"Missing columns: {missing}")

def prepare_dataframe(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    """
    Validate and clean a dataframe for plotting.

    Ensures required columns exist, the dataframe is not empty,
    and removes rows with missing values in required columns.
    """
    _require_columns(df, *cols)
    _require_non_empty(df)

    data = df.dropna(subset=cols).copy()

    if data.empty:
        raise ValueError("No valid data available for plotting")

    return data

def create_figure(
    figsize: tuple[float, float] = DEFAULT_FIGSIZE,
) -> tuple[Figure, Axes]:
    """
    Create and configure a new matplotlib figure.
    """
    apply_style()

    fig, ax = plt.subplots(figsize=figsize)

    return fig, ax


def finalize_chart(
    fig: Figure,
    ax: Axes,
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    legend: bool = False,
    rotate_x: int | None = None,
) -> None:
    """
    Finalize and save a chart.
    """
    ax.set_title(title)
    
    if xlabel:
        ax.set_xlabel(xlabel)

    if ylabel:
        ax.set_ylabel(ylabel)

    if rotate_x is not None:
        for label in ax.get_xticklabels():
            label.set_rotation(rotate_x)
            label.set_ha("right")
            label.set_rotation_mode("anchor")

    if legend:
        ax.legend()

    # Improve spacing
    fig.tight_layout()

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save chart
    fig.savefig(output_path, dpi=DEFAULT_DPI, bbox_inches="tight")

    # Close figure to prevent memory leaks during batch runs
    plt.close(fig)