"""Module to run the analysis and create the charts"""

import numpy as np
import pandas as pd
from bblocks import convert_id

from scripts.logger import logger
from scripts.utils import convert_entities

SM_COUNTRIES = [
    "USA",
    "FRA",
    "DEU",
    "GBR",
    "ITA",
    "CAN",
    "JPN",
    "SAU",
    # "AZE",  # Azerbaijan
    "ARE", # UAE
    # "BEL" # Belgium
]


def cf_high_income_agg(climate_data: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the climate finance commitments for high-income countries"""

    return (
        climate_data.loc[lambda d: d.income_level == "High income"]
        .groupby(["year"])
        .agg({"value": "sum"})
        .reset_index()
    )


def ffs_high_income_agg(ffs_data: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the fossil fuel subsidies for high-income countries"""

    return (
        ffs_data.loc[lambda d: d.income_level == "High income"]
        .groupby(["year"])
        .agg({"value": "sum"})
        .reset_index()
    )


def chart_1_data(climate: pd.DataFrame, subsidies: pd.DataFrame) -> pd.DataFrame:
    """create data for chart 1: Aggregate Climate finance commitments vs fossil fuel
    subsidies for high income countries"""

    cf = cf_high_income_agg(climate_data=climate).rename(
        columns={"value": "climate_finance_commitments"}
    )
    ffs = ffs_high_income_agg(ffs_data=subsidies).rename(
        columns={"value": "fossil_fuel_subsidies"}
    )

    df = cf.merge(ffs, on="year", how="outer").assign(currency="US$ current")
    logger.info("Chart 1 data processed")

    return df


def chart_2_data(climate: pd.DataFrame, subsidies: pd.DataFrame) -> pd.DataFrame:
    """create data for chart 2: small multiple of Climate finance commitments vs f
    ossil fuel subsidies for specified countries"""

    cf = (
        climate.loc[
            lambda d: d.iso3_code.isin(SM_COUNTRIES),
            ["year", "iso3_code", "value"],
        ]
        .rename(columns={"value": "climate_finance_commitments"})
        .reset_index(drop=True)
    )

    ffs = (
        subsidies.loc[
            lambda d: d.iso3_code.isin(SM_COUNTRIES),
            ["year", "iso3_code", "value"],
        ]
        .rename(columns={"value": "fossil_fuel_subsidies"})
        .reset_index(drop=True)
    )

    df = (
        pd.merge(cf, ffs, how="outer")
        .sort_values(["iso3_code", "year"])
        .assign(
            currency="US$ current",
            country_name=lambda d: convert_id(
                d.iso3_code, from_type="ISO3", to_type="name_short", not_found=np.nan
            ),
        )
        .reset_index(drop=True)
    )

    logger.info("Chart 2 data processed")

    return df
