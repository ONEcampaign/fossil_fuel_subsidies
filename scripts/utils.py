"""Utility functions for the project."""

import pandas as pd
from itertools import combinations


def add_aggregates(df: pd.DataFrame,
                   agg_cols: list,
                   id_cols: list, value_col: str = "value",
                   *,
                   agg_func: str = 'sum'):
    """Add aggregate rows to the DataFrame for all combinations of the agg_cols."""

    # List to hold the aggregate DataFrames, starting with the original DataFrame
    aggregate_dfs = [df.copy()]

    # Generate aggregates for all combinations of agg_cols
    for i in range(1, len(agg_cols) + 1):
        for combo in combinations(agg_cols, i):
            group_cols = list(combo) + id_cols
            grouped_df = df.groupby(group_cols, as_index=False).agg({value_col: agg_func})

            # Set the non-used agg_cols to 'all'
            for col in agg_cols:
                if col not in combo:
                    grouped_df[col] = 'all'

            # Append grouped DataFrame to the list
            aggregate_dfs.append(grouped_df)

    # Add the final aggregation where all agg_cols are 'all'
    final_agg_df = df.groupby(id_cols, as_index=False).agg({value_col: agg_func})
    final_agg_df[agg_cols] = 'all'
    aggregate_dfs.append(final_agg_df)

    # Concatenate all aggregate DataFrames into one
    return pd.concat(aggregate_dfs, ignore_index=True)