"""Module to run the analysis and create the charts"""

import pandas as pd

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import convert_entities

CF = pd.read_csv(
    Paths.output / "climate_finance_commitments.csv"
)  # climate finance data
FFS = pd.read_csv(
    Paths.output / "fossil_fuel_subsidies.csv"
)  # fossil fuel subsidies data

small_multiple_countries = ["USA",
                            "FRA",
                            "DEU",
                            "GBR",
                            "ITA",
                            "CAN",
                            "JPN",
                            "SAU",
                            "AZE", # Azerbaijan
                            # "ARE", # UAE
                            # "BEL" # Belgium
                            ]


def cf_high_income_agg() -> pd.DataFrame:
    """Aggregate the climate finance commitments for high-income countries"""

    return (
        CF.loc[lambda d: d.income_level == "High income"]
        .groupby(["year"])
        .agg({"value": "sum"})
        .reset_index()
    )


def ffs_high_income_agg() -> pd.DataFrame:
    """Aggregate the fossil fuel subsidies for high-income countries"""

    return (
        FFS.loc[lambda d: d.income_level == "High income"]
        .groupby(["year"])
        .agg({"value": "sum"})
        .reset_index()
    )


def chart_1_data() -> pd.DataFrame:
    """create data for chart 1: Aggregate Climate finance commitments vs fossil fuel subsidies for high income countries"""

    cf = cf_high_income_agg().rename(columns={"value": "climate_finance_commitments"})
    ffs = ffs_high_income_agg().rename(columns={"value": "fossil_fuel_subsidies"})

    return cf.merge(ffs, on="year", how="outer").assign(currency="US$ current")


def chart_2_data():
    """create data for chart 2: small multiple of Climate finance commitments vs fossil fuel subsidies for specified countries"""

    cf = (
        CF.loc[
            lambda d: d.iso3_code.isin(small_multiple_countries),
            ["year", "iso3_code", "value"],
        ]
        .rename(columns={"value": "climate_finance_commitments"})
        .reset_index(drop=True)
    )

    ffs = (
        FFS.loc[
            lambda d: d.iso3_code.isin(small_multiple_countries),
            ["year", "iso3_code", "value"],
        ]
        .rename(columns={"value": "fossil_fuel_subsidies"})
        .reset_index(drop=True)
    )

    return (
        pd.merge(cf, ffs, how="outer")
        .sort_values(["iso3_code", "year"])
        .assign(
            currency="US$ current",
            country_name=lambda d: convert_entities(
                d.iso3_code, from_type="ISO3", to_type="name_short"
            ),
        )
        .reset_index(drop=True)
    )


if __name__ == "__main__":
    chart_1_data().to_csv(Paths.output / "chart_1_data.csv", index=False)
    logger.info("Chart 1 data successfully created and saved.")

    chart_2_data().to_csv(Paths.output / "chart_2_data.csv", index=False)
    logger.info("Chart 2 data successfully created and saved.")

    logger.info("Analysis completed.")
