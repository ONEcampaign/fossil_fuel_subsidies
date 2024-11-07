"""Formatting for the raw datasets"""

import numpy as np
import pandas as pd
from bblocks import convert_id
from bblocks.dataframe_tools.add import add_income_level_column

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import convert_entities


def format_ffs_data(ffs_data: pd.DataFrame) -> None:
    """Format the Fossil Fuel Subsidies data"""

    (
        ffs_data.rename(
            columns={"Country": "country_name", "Year": "year", "USD, nominal": "value"}
        )
        .drop(columns=["Source"])
        .assign(
            value=lambda d: pd.to_numeric(
                d["value"].str.replace(r"\s+", "", regex=True)
            )
        )
        .dropna(subset="value")
        .groupby(["country_name", "year"])
        .agg({"value": "sum"})
        .reset_index()
        .assign(
            iso3_code=lambda d: convert_id(
                d.country_name, from_type="regex", to_type="ISO3", not_found=np.nan
            )
        )
        .pipe(add_income_level_column, "iso3_code", id_type="ISO3")
        .to_csv(Paths.output / "fossil_fuel_subsidies.csv", index=False)
    )

    logger.info("Fossil Fuel Subsidies data formatted and saved to output folder.")


def format_cf_data(cf_data: pd.DataFrame) -> None:
    """Format the Climate Finance data"""

    (
        cf_data.loc[lambda d: d.year >= 2010]
        .rename(columns={"indicator": "marker", "provider": "provider_name"})
        .drop(columns=["methodology", "flow_type", "oecd_provider_code"])
        .groupby(["year", "provider_name"])
        .agg({"value": "sum"})
        .reset_index()
        .assign(
            iso3_code=lambda d: convert_id(
                d.provider_name, from_type="regex", to_type="ISO3"
            ),
            units="USD current",
        )
        .pipe(add_income_level_column, "iso3_code", id_type="ISO3")
        .sort_values(["provider_name", "year"])
        .reset_index(drop=True)
        .to_csv(Paths.output / "climate_finance_commitments.csv", index=False)
    )

    logger.info("Climate Finance data formatted and saved to output folder.")


if __name__ == "__main__":
    # Climate Finance data
    raw_cf_df = pd.read_parquet(
        Paths.raw_data / "climate_finance_provider_perspective_data.parquet"
    )
    # Fossil Fuel Subsidies data
    raw_ffs_df = pd.read_csv(Paths.raw_data / "fossil_fuel_subsidies.csv")

    format_ffs_data(ffs_data=raw_ffs_df)
    format_cf_data(cf_data=raw_cf_df)

    logger.info("Data formatting complete.")
