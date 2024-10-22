import pandas as pd
from climate_finance.oecd.crdf.provider_perspective import get_provider_perspective
from climate_finance import set_climate_finance_data_path, ClimateData

from scripts.config import Paths

set_climate_finance_data_path(Paths.raw_data)


def _summarise_by_provider_indicator_flow_type(df: pd.DataFrame) -> pd.DataFrame:
    grouper = [
        "year",
        "provider",
        "oecd_provider_code",
        "methodology",
        "indicator",
        "flow_type",
    ]
    return (
        df.groupby(grouper, observed=True, dropna=False)[["value"]].sum().reset_index()
    )


def _clean_methodology_string(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(methodology=lambda d: d.methodology.str.title().str.strip())


def _pivot_indicators(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pivot(
            index=[
                "year",
                "provider",
                "oecd_provider_code",
                "methodology",
                "flow_type",
            ],
            columns="indicator",
            values="value",
        )
        .fillna(0)
        .reset_index()
    )


def _remove_cross_cutting_overlaps(df: pd.DataFrame) -> pd.DataFrame:
    """To avoid double counting, we remove the overlaps between the adaptation, mitigation
    and cross-cutting categories."""
    df["Adaptation"] = df["Adaptation"] - df["Cross-cutting"]
    df["Mitigation"] = df["Mitigation"] - df["Cross-cutting"]

    return df


def _melt_indicators(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(
        id_vars=["year", "provider", "oecd_provider_code", "methodology", "flow_type"],
        var_name="indicator",
        value_name="value",
    )


def _remove_zero_values(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[lambda d: d.value != 0]


def clean_provider_perspective(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.pipe(_clean_methodology_string)
        .pipe(_summarise_by_provider_indicator_flow_type)
        .pipe(_pivot_indicators)
        .pipe(_remove_cross_cutting_overlaps)
        .pipe(_melt_indicators)
        .pipe(_remove_zero_values)
        .sort_values(["provider", "year", "methodology", "indicator"])
    )

    return df


def crs_climate_finance(
    start_year: int, end_year: int, providers: list
) -> pd.DataFrame:
    """Get the CRS perspective data for the given years.

    CRS data does NOT include imputed multilateral flows.
    """
    indicators = {
        "climate_adaptation": "Adaptation",
        "climate_mitigation": "Mitigation",
        "climate_cross_cutting": "Cross-cutting",
    }

    climate = ClimateData(
        years=range(start_year, end_year + 1), providers=providers
    ).load_spending_data(
        methodology="OECD", flows="commitments", source="OECD_CRS_ALLOCABLE"
    )

    df = (
        climate.get_data()
        .assign(methodology="Rio Markers")
        .loc[lambda d: d.indicator != "not_climate_relevant"]
        .assign(indicator=lambda d: d.indicator.map(indicators))
        .pipe(clean_provider_perspective)
    )

    return df


def climate_finance_commitments(start_year: int, end_year: int) -> pd.DataFrame:
    """Get the provider perspective data for the given years.

    Provider data includes OECD DAC imputed multilateral flows.

    Args:
        start_year (int): The start year for the data.
        end_year (int): The end year for the data.

    Returns:
        pd.DataFrame: The cleaned provider perspective data.
    """

    # Container list
    dfs = []

    # Get the provider perspective data
    df = get_provider_perspective(
        start_year=start_year, end_year=end_year, force_update=False
    ).pipe(clean_provider_perspective)

    # Append the data to the container list
    dfs.append(df)

    # Get the unique provider codes
    providers = df.oecd_provider_code.unique().tolist()

    # If the start year is before 2012, get the CRS climate finance data
    if start_year < 2012:
        dfs.append(crs_climate_finance(start_year, 2011, providers))

    # Get the provider perspective data
    df = pd.concat(dfs, ignore_index=True)

    return df


def export_available_donors_and_years(df: pd.DataFrame):
    provider = (
        df.filter(["year", "provider"])
        .sort_values(["year", "provider"])
        .drop_duplicates()
        .groupby("provider")["year"]
        .apply(lambda x: ", ".join(x.astype(str)))
        .reset_index()
        .rename(columns={0: "years"})
    )

    provider.to_csv(
        Paths.output / "provider_perspective_available_donors_and_years.csv",
        index=False,
    )


if __name__ == "__main__":
    # Provider perspective data starts in 2012
    data = climate_finance_commitments(2009, 2022)
    export_available_donors_and_years(data)
