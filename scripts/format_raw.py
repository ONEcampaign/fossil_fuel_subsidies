"""Formatting for the raw datasets"""

import pandas as pd
from bblocks.dataframe_tools.add import add_income_level_column

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import convert_entities, add_aggregates

FFS = pd.read_csv(
    Paths.raw_data / "fossil_fuel_subsidies.csv"
)  # Fossil Fuel Subsidies data
CF = pd.read_parquet(
    Paths.raw_data / "climate_finance_provider_perspective_data.parquet"
)  # Climate Finance data


def format_ffs_data() -> None:
    """Format the Fossil Fuel Subsidies data"""

    (
        FFS.rename(
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
        .assign(iso3_code=lambda d: convert_entities(d.country_name))
        .pipe(add_income_level_column, "iso3_code", id_type="ISO3")
        .to_csv(Paths.output / "fossil_fuel_subsidies.csv", index=False)
    )

    logger.info("Fossil Fuel Subsidies data formatted and saved to output folder.")


def format_cf_data() -> None:
    """Format the Climate Finance data"""

    (
        CF.loc[lambda d: d.year >= 2010]
        .rename(columns={"indicator": "marker", "provider": "provider_name"})
        .drop(columns=["methodology", "flow_type", "oecd_provider_code"])
        .groupby(["year", "provider_name"])
        .agg({"value": "sum"})
        .reset_index()
        .assign(
            iso3_code=lambda d: convert_entities(
                d.provider_name,
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
    format_ffs_data()
    format_cf_data()
    logger.info("Data formatting complete.")
