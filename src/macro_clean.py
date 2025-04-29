#!/usr/bin/env python3
"""
QC and standardise the *micro-economic* tables
  • private_events.csv
  • public_yearly.csv

Usage:
    python micro_clean.py

Creates cleaned files in:  ../data/clean/
      private_events.parquet (or .csv)
      public_yearly.parquet  (or .csv)
"""

import pandas as pd, re, importlib.util
from pathlib import Path

# ── paths ──────────────────────────────────────────────────────────
HERE         = Path(__file__).resolve().parent          # src/
RAW_PRIMARY  = HERE                                     # first look here
RAW_FALLBACK = HERE.parent / "data" / "raw"             # optional second place
CLEAN        = HERE.parent / "data" / "clean"
CLEAN.mkdir(parents=True, exist_ok=True)

SAVE_PARQUET = importlib.util.find_spec("pyarrow") is not None \
               or importlib.util.find_spec("fastparquet") is not None

# ── helper: locate file ────────────────────────────────────────────
def load_csv(name: str) -> pd.DataFrame:
    for root in (RAW_PRIMARY, RAW_FALLBACK):
        f = root / name
        if f.exists():
            return pd.read_csv(f)
    raise FileNotFoundError(f"{name} not found in src/ or data/raw/")

# ── helper: QC & basic cleaning ────────────────────────────────────
def qc(df: pd.DataFrame, name: str) -> pd.DataFrame:
    print(f"\nQC ➜ {name}")
    print(df.describe(include='all').T)

    # 1) canonical NA & dtype
    df.replace({"—": None, "": None}, inplace=True)
    df = df.convert_dtypes()

    # 2) forward/back fill tiny holes (if any)
    df.ffill(inplace=True); df.bfill(inplace=True)

    # 3) derived cols & name harmonisation
    if "valuation_usd" in df.columns and "valuation_b" not in df.columns:
        df["valuation_b"] = df["valuation_usd"] / 1_000_000_000

    if "date" in df.columns and "year" not in df.columns:
        df["year"] = pd.to_datetime(df["date"], errors="coerce").dt.year

    # 4) plausibility checks (only if columns exist)
    if "employees" in df.columns:
        assert (df["employees"] > 0).all(),  "neg/zero head-count"
    if "valuation_b" in df.columns:
        assert (df["valuation_b"] < 5_000).all(), "valuation too large?"
    if "year" in df.columns:
        assert (df["year"].between(1970, 2025)).all(), "year out of range"

    return df

# ── process each micro table ───────────────────────────────────────
for fn in ["private_events.csv", "public_yearly.csv"]:
    df  = load_csv(fn)
    df  = qc(df, fn)

    out = CLEAN / f"{fn[:-4]}.{'parquet' if SAVE_PARQUET else 'csv'}"
    if SAVE_PARQUET:
        df.to_parquet(out, index=False)
    else:
        df.to_csv(out, index=False)

    print("✔ saved →", out.relative_to(Path.cwd()))

print("\n🏁 micro_clean.py completed successfully")
