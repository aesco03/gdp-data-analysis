# src/load_macro.py
"""
Idempotent loader for fact_macro:
– Reads all data/clean/fred_*.parquet
– Registers each series_id from dim_series
– Converts the first column to datetime
– Skips any (date_id, series_id) already in the table
– Appends only the new rows
"""
import os, glob, pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ——— 0. Debug: show working dir & files ——————————————
print("Working dir:", os.getcwd())
parquet_files = glob.glob("data/clean/fred_*.parquet")
print("Files found:", parquet_files)

# ——— 1. Create engine —————————————————————————————————
load_dotenv()  
engine = create_engine(
    f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:"
    f"{os.getenv('MYSQL_PW')}@{os.getenv('MYSQL_HOST')}"
    f":{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}",
    connect_args={"allow_local_infile": True}
)
with engine.connect() as conn:
    min_date_id, max_date_id = conn.execute(
        text("SELECT MIN(date_id), MAX(date_id) FROM dim_date")
    ).one()

# ——— 2. Helpers ————————————————————————————————————
def get_series_id(code):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT series_id FROM dim_series WHERE fred_code = :c"),
            {"c": code}
        ).scalar_one()

def get_existing_date_ids(series_id):
    with engine.connect() as conn:
        return set(conn.execute(
            text("SELECT date_id FROM fact_macro WHERE series_id = :sid"),
            {"sid": series_id}
        ).scalars().all())

# ——— 3. Load loop ——————————————————————————————————
total_loaded = 0
# … your imports and setup …

for path in parquet_files:
    code = os.path.basename(path).replace("fred_","").replace(".parquet","")
    sid  = get_series_id(code)
    print(f"\nLoading {code} (series_id={sid})…")

    df = pd.read_parquet(path, engine="pyarrow")
    df = df.rename(columns={df.columns[0]:"date", df.columns[1]:"value_num"})
    df["date"] = pd.to_datetime(df["date"])
    
    # *** NEW: roll up to quarterly ***
    df["year_qtr"] = df["date"].dt.to_period("Q")
    df_q = (
        df.groupby("year_qtr", as_index=False)["value_num"]
          .mean()   # or .sum() if that makes sense
    )
    df_q = df_q.dropna(subset=["value_num"])
    
    df_q["date_id"]     = df_q["year_qtr"].dt.year*100 + df_q["year_qtr"].dt.quarter
    df_q["series_id"]   = sid
    # keep only those quarters that exist in dim_date
    df_q = df_q[(df_q["date_id"] >= min_date_id) & (df_q["date_id"] <= max_date_id)]


    # filter out already-loaded quarters
    existing = get_existing_date_ids(sid)
    to_insert = df_q[~df_q["date_id"].isin(existing)]
    if to_insert.empty:
        print("  No new quarters to insert; skipping.")
        continue

    # insert
    records = to_insert[["date_id","series_id","value_num"]]
    count = len(records)
    records.to_sql("fact_macro", engine,
                   if_exists="append", index=False, method="multi")
    print(f"  ➔ Inserted {count} new quarterly rows")
    total_loaded += count


print(f"\n✔ Done: loaded {total_loaded} new rows into fact_macro")
