from __future__ import annotations

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import pandas as pd
from pathlib import Path

from .base import create_figure, finalize_chart


def plot_bar(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    output_path: Path,
    rotate_x: int | None = None,
) -> None:

    if df.empty:
        raise ValueError("DataFrame is empty")

    create_figure()

    colors = cm.tab20(np.linspace(0, 1, len(df)))

    plt.bar(
        df[x_col],
        df[y_col],
        color=colors,
    )

    finalize_chart(
        title=title,
        xlabel=x_col,
        ylabel=y_col,
        output_path=output_path,
        rotate_x=rotate_x,
    )