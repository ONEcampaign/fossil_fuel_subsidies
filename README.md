# Climate Finance Hypocrisy

This repository powers the analysis and visualisations for the page
[Climate Finance Hypocrisy](https://data.one.org/data-dives/climate-financing-hypocircy/).
Read the methodology in [this notebook](https://observablehq.com/d/0c7a2e77813dc5f4).

## Data
The data in the analysis is collected from the OECD's Creditor Reporting System (CRS)
and the Fossil Fuel Subsidy Tracker. The raw data can be found in the `raw_data` folder.
Formatted data and final datasets powering the charts can be found in the `output` folder.

## Analysis
To reproduce the analysis Python 3.10. Dependencies are managed by poetry and can be installed by running `poetry install`.
The `scripts` folder contains the scripts used to generate the data and visualisations.