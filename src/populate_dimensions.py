"""
populate_dimensions.py
Populates the dim_date and dim_series tables in the econ_panel MySQL database.
Run this before loading your macro data into fact_macro.
"""
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

user = os.getenv("MYSQL_USER", "root")
pw   = os.getenv("MYSQL_PW", "")
host = os.getenv("MYSQL_HOST", "localhost")
port = os.getenv("MYSQL_PORT", "3306")
db   = os.getenv("MYSQL_DB", "econ_panel")

# Create SQLAlchemy engine
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pw}@{host}:{port}/{db}",
    connect_args={"allow_local_infile": True}
)

# 1. Populate dim_date
print("Populating dim_date...")
dates = pd.date_range("1947-01-01", pd.Timestamp.today(), freq="QS")
dim_date = pd.DataFrame({"date_actual": dates})
dim_date = dim_date.assign(
    year=lambda d: d.date_actual.dt.year,
    quarter=lambda d: d.date_actual.dt.quarter,
    year_qtr=lambda d: d.year.astype(str) + "-Q" + d.quarter.astype(str),
    date_id=lambda d: d.year * 100 + d.quarter
)
dim_date.to_sql("dim_date", engine, if_exists="append", index=False, method="multi")
print(f"Inserted {len(dim_date)} rows into dim_date")

# 2. Populate dim_series
print("Populating dim_series...")
series_meta = [
    ("GDPC1",               "Real GDP",                       "Billions of 2017 dollars", "BEA/FRED"),
    ("UNRATE",              "Unemployment Rate",             "Percent",                 "BLS/FRED"),
    ("CPIAUCSL",            "CPI (All Urban)",               "Index 1982–84=100",       "BLS/FRED"),
    ("CIVPART",             "Labor-Force Participation",     "Percent",                 "BLS/FRED"),
    ("OPHNFB",              "Labor Productivity",            "Index 2017=100",          "BLS/FRED"),
    ("Y006RX1Q020SBEA",     "Private R&D (Real)",            "Billions of 2017 $",      "BEA/FRED"),
    ("Y057RX1Q020SBEA",     "Gov R&D (Real)",                "Billions of 2017 $",      "BEA/FRED"),
    ("Y006RC1Q027SBEA",     "Private R&D (Nominal)",         "Billions of current $",   "BEA/FRED"),
    ("Y057RC1Q027SBEA",     "Gov R&D (Nominal)",             "Billions of current $",   "BEA/FRED"),
    ("Y694RC1Q027SBEA",     "Total R&D (% GDP)",             "Percent of GDP",          "BEA/FRED"),
    ("A679RL1Q225SBEA",     "Real ICT Investment",           "Billions of 2017 $",      "BEA/FRED"),
    ("COMPRNFB",            "Real Hourly Compensation",      "Index 2017=100",          "BLS/FRED"),
    ("PATENTUSALLTOTAL",    "Total Patents Granted",         "Count per year",          "USPTO/FRED"),
    ("FEDFUNDS",            "Fed Funds Rate",                "Percent",                 "FED/FRED"),
    ("GS10",                "10-Year Treasury Yield",        "Percent",                 "FED/FRED"),
    ("USREC",               "NBER Recession Dummy",          "0/1",                     "NBER/FRED"),
    ("FDEFX",               "Defense Outlays (% GDP)",       "Percent of GDP",          "BEA/FRED"),
]
dim_series = pd.DataFrame(series_meta, columns=["fred_code","series_name","units","source"])
dim_series.to_sql("dim_series", engine, if_exists="append", index=False, method="multi")
print(f"Inserted {len(dim_series)} rows into dim_series")
