import pandas as pd
import os

# Base year for normalization (pick something modern)
BASE_YEAR = 2020

# Load CPI
cpi = pd.read_csv("data/raw/fred/CPIAUCSL.csv")
cpi.columns = ['date', 'CPI']
cpi['date'] = pd.to_datetime(cpi['date'])
cpi['year'] = cpi['date'].dt.year
cpi = cpi.groupby('year')['CPI'].mean().reset_index()

# Get CPI for base year
base_cpi = cpi.loc[cpi['year'] == BASE_YEAR, 'CPI'].values[0]

# Series to normalize
to_normalize = {
    "GPDIC1": "Gross_Private_Investment",
    "Y006RC1Q027SBEA": "Private_RD",
    "Y057RC1Q027SBEA": "Gov_RD",
}

input_dir = "data/raw/fred/"
output_dir = "data/processed/"
os.makedirs(output_dir, exist_ok=True)

for code, name in to_normalize.items():
    print(f"Normalizing {name} ({code})...")
    df = pd.read_csv(f"{input_dir}{code}.csv")
    df.columns = ['date', name]
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df = df.merge(cpi, on='year', how='left')
    df[f"{name}_real"] = df[name] * (base_cpi / df['CPI'])
    df[[ 'date', f"{name}_real" ]].to_csv(f"{output_dir}{code}_normalized.csv", index=False)

print(" All series normalized to constant dollars.")
