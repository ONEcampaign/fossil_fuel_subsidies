"""Utility functions for the project."""

from itertools import combinations

import country_converter as coco
import numpy as np
import pandas as pd


def add_aggregates(
    df: pd.DataFrame,
    agg_cols: list,
    id_cols: list,
    value_col: str = "value",
    agg_value="all",
    *,
    agg_func: str = "sum"
):
    """Add aggregate rows to the DataFrame for all combinations of the agg_cols.

    Args:
        df: The DataFrame to add aggregates to
        agg_cols: The columns to aggregate
        id_cols: The columns to keep as identifiers
        value_col: The column to aggregate. Default is 'value'
        agg_value: The value to use for the non-aggregated columns. Default is 'all'
        agg_func: The aggregation function to use. Default is 'sum'
    """

    # List to hold the aggregate DataFrames, starting with the original DataFrame
    aggregate_dfs = [df.copy()]

    # Generate aggregates for all combinations of agg_cols
    for i in range(1, len(agg_cols) + 1):
        for combo in combinations(agg_cols, i):
            group_cols = list(combo) + id_cols
            grouped_df = df.groupby(group_cols, as_index=False).agg(
                {value_col: agg_func}
            )

            # Set the non-used agg_cols to 'all'
            for col in agg_cols:
                if col not in combo:
                    grouped_df[col] = agg_value

            # Append grouped DataFrame to the list
            aggregate_dfs.append(grouped_df)

    # Add the final aggregation where all agg_cols are 'all'
    final_agg_df = df.groupby(id_cols, as_index=False).agg({value_col: agg_func})
    final_agg_df[agg_cols] = agg_value
    aggregate_dfs.append(final_agg_df)

    # Concatenate all aggregate DataFrames into one
    return pd.concat(aggregate_dfs, ignore_index=True)


def convert_entities(
    entity_col: pd.Series,
    from_type=None,
    to_type="ISO3",
    not_found=np.nan,
    additional_mapping: dict = None,
) -> pd.Series:
    """Convert entities in a column to a different type.

    Args:
        entity_col: The column to convert
        from_type: The type of the entities in the column. If None, the type is inferred
        to_type: The type to convert the entities to. Default is 'ISO3'
        not_found: The value to use for entities that are not found in the conversion. Default is np.nan
        additional_mapping: Additional mapping to use for the conversion. Default is None

    Returns:
        A Series with the converted entities
    """

    mapper = {
        v: coco.convert(v, src=from_type, to=to_type, not_found=not_found)
        for v in entity_col.unique()
    }

    if additional_mapping:
        mapper.update(additional_mapping)

    return entity_col.map(mapper)
