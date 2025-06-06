# src/compute_wide_panel.py
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
url = (
    f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:"
    f"{os.getenv('MYSQL_PW')}@{os.getenv('MYSQL_HOST')}:"
    f"{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
)
engine = create_engine(url)

# 1. Read the wide view
df = pd.read_sql("SELECT * FROM vw_macro ORDER BY date", engine, parse_dates=['date'])
print("Rows from vw_macro:", len(df))

# ---- debug missingness ----
print(df.head(), "\n")
print("NULL counts per column:\n", df.isnull().sum(), "\n")

# 2. (Optional) fill forward so you don’t lose rows
df[['real_gdp','ict_inv','priv_rd']] = df[['real_gdp','ict_inv','priv_rd']].ffill()

# 3. Compute shares & growth rates
df['rd_gdp']  = df['priv_rd']  / df['real_gdp']
df['ict_gdp'] = df['ict_inv']  / df['real_gdp']
df['gdp_g']   = df['real_gdp'].pct_change()
df['ict_g']   = df['ict_inv'].pct_change()
df['rd_g']    = df['priv_rd'].pct_change()
df['ict_g'] = df['ict_g'].clip(lower=-0.2, upper=0.2)
df['rd_g']  = df['rd_g'] .clip(lower=-0.2, upper=0.2)

df['ict_share_g'] = df['ict_gdp'].pct_change()
df['rd_share_g']  = df['rd_gdp'] .pct_change()
# 4. Drop only the genuinely impossible-to-compute rows (e.g. first quarter)
df = df.dropna(subset=['gdp_g','ict_g','rd_g'])
print("Rows after dropping growth-NaNs:", len(df))

# print("Merging macroeconomic control variables from FRED series...")

# # 1. Compensation (as proxy for consumption)
# comp = pd.read_parquet("data/clean/fred_COMPRNFB.parquet")
# comp = comp.rename(columns={'value': 'compensation'})
# comp['consumption_g'] = comp['compensation'].pct_change()
# df = df.merge(comp[['date', 'consumption_g']], on='date', how='left')

# comp = pd.read_parquet("data/clean/fred_COMPRNFB.parquet")
# print("Columns in fred_COMPRNFB:", comp.columns.tolist())


# 2. Unemployment Rate
unemp = pd.read_parquet("data/clean/fred_UNRATE.parquet")
unemp = unemp.rename(columns={'UNRATE': 'unemp_rate'})
unemp['unemp_rate_g'] = unemp['unemp_rate'].pct_change()
df = df.merge(unemp[['date', 'unemp_rate_g']], on='date', how='left')

# 3. CPI - Inflation Proxy

cpi = pd.read_parquet("data/clean/fred_CPIAUCSL.parquet")
cpi = cpi.rename(columns={'CPIAUCSL':'cpi'})
cpi['inflation_g'] = cpi['cpi'].pct_change()
df = df.merge(cpi[['date', 'inflation_g']], on='date', how='left')

# 4. Fed Funds Rate (Monetary Policy)
fed = pd.read_parquet("data/clean/fred_FEDFUNDS.parquet")
fed = fed.rename(columns={'FEDFUNDS': 'fedfunds'})
fed['fedfunds_g'] = fed['fedfunds'].pct_change()
df = df.merge(fed[['date', 'fedfunds_g']], on='date', how='left')

# 5. Labor Force Participation Rate
lfpr = pd.read_parquet("data/clean/fred_CIVPART.parquet")
lfpr = lfpr.rename(columns={'CIVPART': 'lfpr'})
lfpr['lfpr_g'] = lfpr['lfpr'].pct_change()
df = df.merge(lfpr[['date', 'lfpr_g']], on='date', how='left')

# # 6. Federal Defense Expenditures
# fdef = pd.read_parquet("data/clean/fred_FDEFEX.parquet")
# fdef = fdef.rename(columns={'value': 'fed_spending'})
# fdef['fed_spend_g'] = fdef['fed_spending'].pct_change()
# df = df.merge(fdef[['date', 'fed_spend_g']], on='date', how='left')

# # 7. Recession Dummy (0 or 1)
# rec = pd.read_parquet("data/clean/fred_USREC.parquet")
# rec = rec.rename(columns={'value': 'recession'})
# df = df.merge(rec[['date', 'recession']], on='date', how='left')

# print("Control variable merge complete.")


# 5. Persist
os.makedirs("data/processed", exist_ok=True)
out = "data/processed/wide_panel.parquet"
df.to_parquet(out, index=False)
print(f"Saved {out} with {len(df)} rows")
