import pandas as pd
import os

# Setup
input_dir = "data/processed/"
output_dir = "data/processed/"
os.makedirs(output_dir, exist_ok=True)

# Load datasets
gdp = pd.read_csv(os.path.join(input_dir, "maddison_gdp.csv"))
gdp = gdp[gdp['country'] == 'United States']
gdp = gdp[['year', 'gdp_pc_2011usd']]  # Only keep needed columns
gdp.rename(columns={'gdp_pc_2011usd': 'GDP_per_capita_2011USD'}, inplace=True)

private_rd = pd.read_csv(os.path.join(input_dir, "Y006RC1Q027SBEA_normalized.csv"))
gov_rd = pd.read_csv(os.path.join(input_dir, "Y057RC1Q027SBEA_normalized.csv"))

# Merge step-by-step
merged = gdp.merge(private_rd, on='year', how='outer')
merged = merged.merge(gov_rd, on='year', how='outer')

# Save merged file
merged.to_csv(os.path.join(output_dir, "merged_data.csv"), index=False)
