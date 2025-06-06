# src/csv_to_parquet.py
"""
Convert *all* your FRED CSVs—both in data/raw/fred/ and data/processed/fred/—
into full-history Parquet files under data/clean/.
"""
import os, glob
import pandas as pd

# Ensure the clean folder exists
os.makedirs("data/clean", exist_ok=True)

# Collect CSVs from both locations
csv_paths = (
    glob.glob("data/raw/fred/*.csv") +
    glob.glob("data/processed/fred/*.csv")
)

for csv_path in csv_paths:
    code = os.path.basename(csv_path).replace(".csv", "")
    print(f"Converting {code} → Parquet")
    # Read the full history, parsing the 'date' column
    df = pd.read_csv(csv_path, parse_dates=['date'])
    # Write every row out to clean/
    df.to_parquet(f"data/clean/fred_{code}.parquet", index=False)

print("✅ All FRED CSVs converted to full-history Parquets.")
