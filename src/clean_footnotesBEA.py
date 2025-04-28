import pandas as pd
import os

# Path to your processed BEA folder
processed_bea_dir = '/Users/adrianescobar/Desktop/workspace/DataSci_3343/DA_FinalProj/gdp-data-analysis/data/processed/bea'

# Go through every CSV in that folder
for filename in os.listdir(processed_bea_dir):
    if filename.endswith('.csv'):
        filepath = os.path.join(processed_bea_dir, filename)
        df = pd.read_csv(filepath)
        
        # Keep only rows where the first column is NOT empty or NaN
        df = df[df.iloc[:, 0].notna()]
        
        # Remove rows where the first column contains "Source" or "Consists of"
        df = df[~df.iloc[:, 0].astype(str).str.contains('Source|Consists of|Addenda', na=False)]

        # Save it BACK over itself
        df.to_csv(filepath, index=False)

print("Done cleaning footnotes 🚀")
