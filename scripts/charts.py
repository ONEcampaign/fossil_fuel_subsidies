"""Module to format the data for flourish charts"""

import pandas as pd

from scripts.config import Paths

chart_1_data = pd.read_csv(Paths.output / "chart_1_data.csv")
chart_2_data = pd.read_csv(Paths.output / "chart_2_data.csv")


def _add_max(df, column):
    """Add the value for latest year for each country to the data"""

    max_dict = df.loc[df.groupby(["country_name"])["year"].idxmax()].set_index(
        "country_name"
    )[column]

    return df.assign(max_value=df.country_name.map(max_dict))


def format_to_billion(value, decimals=1):
    """format value to billion"""

    value_in_billion = value / 1e9

    # if decimals is more than 0
    # if the remainer is 0, then format to integer
    if decimals > 0 and value_in_billion % 1 == 0:
        return f"${value_in_billion:.0f} billion"

    return f"${value_in_billion:.{decimals}f} billion"


def create_chart_2() -> pd.DataFrame:
    """Create data for chart 2"""

    _ffs = (
        chart_2_data.loc[:, ["year", "country_name", "fossil_fuel_subsidies"]]
        .pipe(_add_max, "fossil_fuel_subsidies")
        .assign(fossil_fuel_subsidies=lambda d: d.fossil_fuel_subsidies * -1)
        .assign(max_value=lambda d: d.max_value.apply(format_to_billion))
        .pivot(
            index=["country_name", "year"],
            columns="max_value",
            values="fossil_fuel_subsidies",
        )
        .reset_index()
        .assign(indicator="fossil_fuel_subsidies")
    )

    _cf = (
        chart_2_data.loc[:, ["year", "country_name", "climate_finance_commitments"]]
        .pipe(_add_max, "climate_finance_commitments")
        .assign(max_value=lambda d: d.max_value.apply(format_to_billion))
        .pivot(
            index=["country_name", "year"],
            columns="max_value",
            values="climate_finance_commitments",
        )
        .reset_index()
        .assign(indicator="climate_finance_commitments")
    )

    return pd.concat([_ffs, _cf], ignore_index=True)


def create_chart_1() -> pd.DataFrame:
    """Create data for chart 1"""

    return (
        chart_1_data.drop(columns="currency")
        .rename(
            columns={
                "climate_finance_commitments": "climate finance commitments",
                "fossil_fuel_subsidies": "fossil fuel subsidies",
            }
        )
        .melt(id_vars="year")
        .assign(
            max_value=lambda d: d.variable.map(
                d.loc[d.groupby(["variable"])["year"].idxmax()].set_index("variable")[
                    "value"
                ]
            )
        )
        .assign(max_value=lambda d: d.max_value.apply(format_to_billion, decimals=0))
        .assign(
            value=lambda d: d.value.where(
                d.variable != "fossil fuel subsidies", -d.value
            )
        )
        .pivot(index=["year", "variable"], columns="max_value", values="value")
        .reset_index()
    )
