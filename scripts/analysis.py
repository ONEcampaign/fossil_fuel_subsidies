"""Module to run the analysis and create the charts"""

import pandas as pd

from scripts.config import Paths
from scripts.logger import logger

CF = pd.read_csv(
    Paths.output / "climate_finance_commitments.csv"
)  # climate finance data
FFS = pd.read_csv(
    Paths.output / "fossil_fuel_subsidies.csv"
)  # fossil fuel subsidies data


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


if __name__ == "__main__":
    chart_1_data().to_csv(Paths.output / "chart_1_data.csv", index=False)
    logger.info("Chart 1 data successfully created and saved.")
