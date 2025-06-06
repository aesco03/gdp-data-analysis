#!/usr/bin/env python3
"""
ETL for macro-economic series:
  • FRED CSVs       → data/raw/fred/*.csv
  • BLS TXT/CSVs    → data/raw/bls/*.txt, *.csv
  • BEA Excel       → data/raw/bea/*.xls*
  • Maddison Excel  → data/raw/maddison/*.xls*

Output: tidy 2-column CSVs in data/clean/, wiping out any old files first.
"""

import pandas as pd, re, shutil
from pathlib import Path

# ── Paths ───────────────────────────────────────────────────────────
BASE      = Path(__file__).resolve().parent        # project root
RAW_DIR   = BASE / "data" / "raw"
CLEAN_DIR = BASE / "data" / "clean"

# 1) Nuke the old clean folder so NOTHING lingers
if CLEAN_DIR.exists():
    shutil.rmtree(CLEAN_DIR)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

def sanitize(name: str) -> str:
    """Make a filesystem-safe stem from any string."""
    return re.sub(r"[^\w]+", "_", name).strip("_").lower()

def save_csv(df: pd.DataFrame, stem: str):
    """Save df[date,value] to data/clean/stem.csv."""
    out = CLEAN_DIR / f"{stem}.csv"
    df.to_csv(out, index=False)
    print(f"✓ saved {out.name}")

# ── 2) FRED series ──────────────────────────────────────────────────
fred_folder = RAW_DIR / "fred"
if fred_folder.exists():
    for f in fred_folder.glob("*.csv"):
        df = pd.read_csv(f)
        # assume first two columns are date + value
        df = df.rename(columns={df.columns[0]:"date", df.columns[1]:"value"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        stem = f"fred_{sanitize(f.stem)}"
        save_csv(df[["date","value"]], stem)

# ── 3) BLS series ──────────────────────────────────────────────────
bls_folder = RAW_DIR / "bls"
if bls_folder.exists():
    for f in bls_folder.iterdir():
        if f.suffix.lower() not in [".txt", ".csv"]:
            continue
        print(f"Processing BLS file: {f.name}")
        sep = "\t" if f.suffix.lower()==".txt" else ","
        df = pd.read_csv(f, sep=sep, comment="#", dtype=str)

        # Build a date column
        if {"year","period"}.issubset(df.columns):
            df["date"] = pd.to_datetime(
                df["year"].astype(str) + df["period"].str[1:] + "01",
                errors="coerce"
            )
        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        else:
            print(f"⚠️  skipping {f.name}: no date info")
            continue

        # Find the first numeric column
        num_cols = [c for c in df.columns if c not in ("year","period","date")]
        valcol = None
        for c in num_cols:
            try:
                pd.to_numeric(df[c].str.replace(",",""), errors="raise")
                valcol = c
                break
            except Exception:
                continue
        if not valcol:
            print(f"⚠️  skipping {f.name}: no numeric column found")
            continue

        df["value"] = pd.to_numeric(df[valcol].str.replace(",",""), errors="coerce")
        stem = f"bls_{sanitize(f.stem)}"
        save_csv(df[["date","value"]], stem)

# ── 4) BEA key source ───────────────────────────────────────────────
bea_folder = RAW_DIR / "bea"
if bea_folder.exists():
    for f in bea_folder.glob("*.xls*"):
        try:
            df = pd.read_excel(f, sheet_name=0, skiprows=7)
            df = df.iloc[:, :2].rename(columns={df.columns[0]:"year", df.columns[1]:"value"})
            df["date"] = pd.to_datetime(df["year"], format="%Y", errors="coerce")
            stem = f"bea_{sanitize(f.stem)}"
            save_csv(df[["date","value"]], stem)
        except Exception as e:
            print(f"⚠️  could not process BEA file {f.name}: {e}")

# ── 5) Maddison Project GDP ─────────────────────────────────────────
mpd_folder = RAW_DIR / "maddison"
if mpd_folder.exists():
    for f in mpd_folder.glob("*.xls*"):
        try:
            mpd = pd.read_excel(f, sheet_name="Full data", engine="openpyxl")
            usa = mpd[mpd["countrycode"]=="USA"]
            df  = usa[["year","rgdpnapc"]].rename(columns={"rgdpnapc":"value"})
            df["date"] = pd.to_datetime(df["year"], format="%Y", errors="coerce")
            save_csv(df[["date","value"]], "maddison_gdp")
        except Exception as e:
            print(f"⚠️  could not process Maddison file {f.name}: {e}")

print("\n🏁 macro_clean.py completed — cleaned CSVs in data/clean/")
