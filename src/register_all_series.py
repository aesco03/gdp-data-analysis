# src/register_series.py
import glob
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

URL = (
    f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:"
    f"{os.getenv('MYSQL_PW')}@{os.getenv('MYSQL_HOST')}:"
    f"{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
)
engine = create_engine(URL)

# 1. Gather all codes from your clean folder
files = glob.glob("data/clean/fred_*.parquet")
codes = {os.path.basename(f).replace("fred_","").replace(".parquet","") for f in files}
print("Discovered codes:", codes)

# 2. For each code, insert if missing
with engine.begin() as conn:
    for code in sorted(codes):
        conn.execute(text("""
            INSERT IGNORE INTO dim_series
              (fred_code, series_name, units, source)
            VALUES
              (:c, :c, 'unknown', 'FRED')
        """), {"c": code})

print("✅ Registered all missing series codes.")
