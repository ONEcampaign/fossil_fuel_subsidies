"""Module to format the data for flourish charts"""

import pandas as pd

from scripts.config import Paths

chart_2_data = pd.read_csv(Paths.output / "chart_2_data.csv")


def _add_max(df, column):
    """Add the value for latest year for each country to the data"""

    max_dict = df.loc[df.groupby(['country_name'])['year'].idxmax()].set_index('country_name')[column]

    return df.assign(max_value =  df.country_name.map(max_dict))


def format_to_billion(value):
    """format value to billion"""

    value_in_billion = value / 1e9

    # If the value is less than 1 billion, round to 1 decimal place, else round to nearest integer
    if value_in_billion < 1:
        formatted_value = f"${value_in_billion:.1f} billion"
    else:
        formatted_value = f"${int(round(value_in_billion))} billion"

    return formatted_value


def create_chart_2() -> pd.DataFrame:
    """Create data for chart 2"""

    _ffs = (chart_2_data
            .loc[:, ['year', 'country_name', 'fossil_fuel_subsidies']]
            .pipe(_add_max, "fossil_fuel_subsidies")
            .assign(fossil_fuel_subsidies = lambda d: d.fossil_fuel_subsidies *-1)
            .assign(max_value = lambda d: d.max_value.apply(format_to_billion))
            .pivot(index=['country_name', 'year'], columns = 'max_value', values='fossil_fuel_subsidies')
            .reset_index()
            .assign(indicator = 'fossil_fuel_subsidies')
            )

    _cf = (chart_2_data
           .loc[:, ['year', 'country_name', 'climate_finance_commitments']]
           .pipe(_add_max, "climate_finance_commitments")
           .assign(max_value = lambda d: d.max_value.apply(format_to_billion))
           .pivot(index=['country_name', 'year'], columns = 'max_value', values='climate_finance_commitments')
           .reset_index()
           .assign(indicator = 'climate_finance_commitments')
           )

    return pd.concat([_ffs, _cf], ignore_index=True)