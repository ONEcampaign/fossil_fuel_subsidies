"""Formatting for the raw datasets"""

import pandas as pd

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import convert_entities, add_aggregates

FFS = pd.read_csv(Paths.raw_data / "fossil_fuel_subsidies.csv") # Fossil Fuel Subsidies data
CF = pd.read_csv(Paths.raw_data / "climate_finance_commitments_all.csv") # Climate Finance data


def format_ffs_data() -> None:
    """Format the Fossil Fuel Subsidies data

    This function formats the raw Fossil Fuel Subsidies data and saves it to the output folder.
    Formatting operations
    - Rename columns
    - Add units column
    - Convert value to numeric
    - Add aggregates for mechanism, beneficiary, and fuel_type
    - Add country_code column with converted country names
    - Save the formatted data to the output folder
    """

    (FFS
     .rename(columns = {"Country": 'country_name',
                        "Year": 'year',
                        "Fuel type": "fuel_type",
                        "Source": "source",
                        "USD, nominal": 'value',
                        "Mechanism": "mechanism",
                        "Beneficiary": "beneficiary"
                        }
             )
     .assign(units = "USD nominal")
     .drop(columns = "source")
     .assign(value = lambda d: pd.to_numeric(d['value'].str.replace(r'\s+', '', regex=True)))
     .pipe(add_aggregates, agg_cols = ["mechanism", "beneficiary", "fuel_type"], id_cols=['country_name', "year", "units"])
     .assign(country_code = lambda d: convert_entities(d.country_name))
     .reset_index(drop=True)
     .to_csv(Paths.output / "fossil_fuel_subsidies.csv", index=False)
     )

    logger.info("Fossil Fuel Subsidies data formatted and saved to output folder.")

def format_cf_data() -> None:
    """Format the Climate Finance data

    This function formats the raw Climate Finance data and saves it to the output folder.
    Formatting operations
    - Rename columns
    - Drop methodology and flow_type columns
    - Add aggregates for marker
    - Add country_code column with converted country names
    - Save the formatted data to the output folder
    """

    (CF
     .rename(columns = {"indicator": "marker", "provider": "country_name"})
     .drop(columns = ["methodology", "flow_type"])
     .pipe(add_aggregates, ['marker'], ['year', 'country_name'])
     .assign(country_code = lambda d: convert_entities(d.country_name, additional_mapping={'EU Institutions (excl. EIB)': "EUI"}),
             units = "USD current")
     .reset_index(drop=True)
     .to_csv(Paths.output / "climate_finance_providers.csv", index=False)
     )

    logger.info("Climate Finance data formatted and saved to output folder.")


if __name__ == "__main__":
    format_ffs_data()
    format_cf_data()
    logger.info("Data formatting complete.")