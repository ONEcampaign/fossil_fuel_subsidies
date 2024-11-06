"""Module to format the data for flourish charts"""

import pandas as pd

from scripts.analysis import chart_1_data, chart_2_data
from scripts.config import Paths


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
    # if the remainder is 0, then format to integer
    if decimals > 0 and value_in_billion % 1 == 0:
        return f"${value_in_billion:.0f} billion"

    return f"${value_in_billion:.{decimals}f} billion"


def annotate_number(value):
    """This function takes a number
    If the value in billions is more than 0.01, then return the value in billions
    If the value in billions is less than 0.01, then return the value in millions
    If the value in billions is starts at the 2nd decimal, then return the value in billions with 2 decimals
    Otherwise return only 1 decimal
    """

    if value / 1e9 >= 0.01:
        value_in_billion = value / 1e9

        if value_in_billion > 1:
            return f"${value_in_billion:.1f} billion"
        elif value_in_billion < 1:
            return f"${value_in_billion:.2f} billion"

    elif value / 1e9 < 0.01:
        if value / 1e6 < 10:
            return f"${value/1e6:.1f} million"
        else:
            return f"${value/1e6:.0f} million"

    else:
        return None


def format_number_as_text(value):
    """Format a number as text with commas"""

    return f"{value:,.0f}"


def create_chart_2(chart2: pd.DataFrame) -> pd.DataFrame:
    """Create data for chart 2"""

    _ffs = (
        chart2.loc[:, ["year", "country_name", "fossil_fuel_subsidies"]]
        .pipe(_add_max, "fossil_fuel_subsidies")
        .assign(fossil_fuel_subsidies=lambda d: d.fossil_fuel_subsidies * -1)
        .assign(max_value=lambda d: d.max_value.apply(annotate_number))
        .assign(
            value_annotate=lambda d: (d.fossil_fuel_subsidies * -1).apply(
                annotate_number
            )
        )
        .pivot(
            index=["country_name", "year", "value_annotate"],
            columns="max_value",
            values="fossil_fuel_subsidies",
        )
        .reset_index()
        .assign(indicator="fossil fuel subsidies")
    )

    _cf = (
        chart2.loc[:, ["year", "country_name", "climate_finance_commitments"]]
        .pipe(_add_max, "climate_finance_commitments")
        .assign(max_value=lambda d: d.max_value.apply(annotate_number))
        .assign(
            value_annotate=lambda d: d.climate_finance_commitments.apply(
                annotate_number
            )
        )
        .pivot(
            index=["country_name", "year", "value_annotate"],
            columns="max_value",
            values="climate_finance_commitments",
        )
        .reset_index()
        .assign(indicator="climate finance commitments")
    )

    return pd.concat([_ffs, _cf], ignore_index=True)


def create_chart_1(chart1: pd.DataFrame) -> pd.DataFrame:
    """Create data for chart 1"""

    return (
        chart1.drop(columns="currency")
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


if __name__ == "__main__":
    raw_climate = pd.read_csv(Paths.output / "climate_finance_commitments.csv")
    raw_subsidies = pd.read_csv(Paths.output / "fossil_fuel_subsidies.csv")

    chart1_data = chart_1_data(climate=raw_climate, subsidies=raw_subsidies).pipe(
        create_chart_1
    )
    chart2_data = chart_2_data(climate=raw_climate, subsidies=raw_subsidies).pipe(
        create_chart_2
    )

    chart1_data.to_csv(Paths.output / "chart_1_base.csv", index=False)
    chart2_data.to_csv(Paths.output / "chart_2_base.csv", index=False)
