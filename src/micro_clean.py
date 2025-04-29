#!/usr/bin/env python
"""
QC and standardise the *micro-economic* tables:
  • private_events.csv
  • public_yearly.csv
Saves clean Parquet files into data/clean/.
"""

import pandas as pd
from pathlib import Path

RAW  = Path("data/raw")
CLEAN = Path("data/clean"); CLEAN.mkdir(exist_ok=True)

def qc(df, name):
    print(f"\nQC ➜ {name}")
    print(df.describe(include="all").T)

    # basic fixes
    df.replace({"—": None, "": None}, inplace=True)        # stray em-dashes?
    df = df.convert_dtypes()
    df.ffill(inplace=True); df.bfill(inplace=True)

    # plausibility traps
    assert (df["employees"] > 0).all(),  "neg / zero head-count"
    assert (df["valuation_b"] < 5_000).all(), "valuation too large?"
    assert (df["year"].between(1970, 2025)).all(), "year out of range"
    return df

for fn in ["private_events.csv", "public_yearly.csv"]:
    df = pd.read_csv(RAW/fn)
    df = qc(df, fn)
    df.to_parquet(CLEAN/f"{fn[:-4]}.parquet", index=False)
    print(" ✔ saved →", CLEAN/f"{fn[:-4]}.parquet")
