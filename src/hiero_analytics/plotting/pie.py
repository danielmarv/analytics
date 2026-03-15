from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import create_figure, finalize_chart, prepare_dataframe


def plot_pie(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    title: str,
    output_path: Path,
) -> None:
    """
    Plot a pie chart.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing pie chart data.
    label_col : str
        Column containing labels for each slice.
    value_col : str
        Column containing numeric values for each slice.
    title : str
        Chart title.
    output_path : Path
        File path where the chart image will be saved.
    """
    data = prepare_dataframe(df, label_col, value_col)

    data = df[[label_col, value_col]].dropna()

    if data.empty:
        raise ValueError("No valid data available for plotting")

    if data[value_col].sum() == 0:
        raise ValueError("Pie chart values sum to zero")

    fig, ax = create_figure()

    ax.pie(
        data[value_col],
        labels=data[label_col],
        autopct="%1.1f%%",
        startangle=90,
    )

    # Ensure circular shape
    ax.set_aspect("equal")

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel="",
        ylabel="",
        output_path=output_path,
    )