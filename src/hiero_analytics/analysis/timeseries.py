import pandas as pd


def cumulative_timeseries(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    Build a cumulative count series over time.

    Parameters
    ----------
    df
        Input dataframe containing a datetime column.
    date_col
        Name of the datetime column to use for the timeline.

    Returns
    -------
    pd.DataFrame
        Dataframe with:
        - ``date_col``: timeline values
        - ``count``: cumulative count
    """
    if df.empty:
        return pd.DataFrame(columns=[date_col, "count"])

    out = (
        df[[date_col]]
        .dropna()
        .sort_values(date_col)
        .assign(count=1)
    )

    out["count"] = out["count"].cumsum()

    return out.reset_index(drop=True)