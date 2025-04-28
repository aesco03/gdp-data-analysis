import pandas as pd
import os

# Path to the weird Excel file
input_file = '/Users/adrianescobar/Desktop/workspace/DataSci_3343/DA_FinalProj/gdp-data-analysis/data/raw/bea/gdpkeysource-4q24-3rd.xlsx'

# Output file path
output_file = '/Users/adrianescobar/Desktop/workspace/DataSci_3343/DA_FinalProj/gdp-data-analysis/data/processed/bea/BEA_keysource_cleaned.csv'

# Read the 'Second' sheet
df = pd.read_excel(input_file, sheet_name='Second', header=None)

# Skip junk at the top
df = df.iloc[7:, :]

# Drop totally empty columns
df = df.dropna(axis=1, how='all')

# Reset the index
df = df.reset_index(drop=True)

# Now build *dynamic* column names based on actual width
columns = ['id', 'indicator_name']
for i in range(2, len(df.columns)):
    columns.append(f'value_{i-1}')  # value_1, value_2, etc.

# Set the columns
df.columns = columns

# Drop rows where id is NaN (footnotes, junk, etc.)
df = df[df['id'].notna()]

# Save to CSV
df.to_csv(output_file, index=False)

print("🎯 Cleaned and saved BEA keysource file to:", output_file)
