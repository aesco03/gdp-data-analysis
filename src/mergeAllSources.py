#Empty for now
import pandas as pd
import os

input_dir = "data/processed/"
output_dir = "data/processed/"
os.makedirs(output_dir, exist_ok=True)

#load normalized data
gdp = pd.read_csv(os.path.join(input_dir, "GPDIC1_normalized.csv"))
private_rd = pd.read_csv(os.path.join(input_dir, "Y006RC1Q027SBEA_normalized.csv"))
gov_rd = pd.read_csv(os.path.join(input_dir, "Y057RC1Q027SBEA_normalized.csv"))
merged = gdp.merge(private_rd, on='date', how='outer')
merged = merged.merge(gov_rd, on='date', how='outer')

#save file
merged.to_csv(os.path.join(output_dir, "merged_data.csv"), index=False)

