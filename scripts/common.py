import pandas as pd
from bblocks import convert_id, set_bblocks_data_path

from scripts.config import Paths

set_bblocks_data_path(Paths.raw_data)

g20_countries = [
    "Argentina",
    "Australia",
    "Brazil",
    "Canada",
    "China",
    "France",
    "Germany",
    "India",
    "Indonesia",
    "Italy",
    "Japan",
    "Mexico",
    "Russia",
    "Saudi Arabia",
    "South Africa",
    "South Korea",
    "Turkey",
    "United Kingdom",
    "United States",
    "EUI",
]

g20_donor_codes = (
    convert_id(
        pd.Series(g20_countries),
        "regex",
        "DACCode",
        additional_mapping={"EUI": 918},
    )
    .astype(int)
    .to_list()
)
