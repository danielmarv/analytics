import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

from .base import create_figure, finalize_chart


def plot_line(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    output_path: Path,
):
    create_figure()

    plt.plot(df[x_col], df[y_col], marker="o")

    finalize_chart(
        title=title,
        xlabel=x_col,
        ylabel=y_col,
        output_path=output_path,
    )


def plot_multi_line(
    df: pd.DataFrame,
    index_col: str,
    group_col: str,
    value_col: str,
    title: str,
    output_path: Path,
):
    create_figure()

    pivot = df.pivot(index=index_col, columns=group_col, values=value_col)

    pivot.plot()

    finalize_chart(
        title=title,
        xlabel=index_col,
        ylabel=value_col,
        output_path=output_path,
    )