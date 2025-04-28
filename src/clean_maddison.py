import pandas as pd
import os

# Load the sheet with GDP per capita in 2011 USD prices
xls = pd.ExcelFile('data/raw/maddison/mpd2023_web.xlsx')
df = pd.read_excel(xls, sheet_name='GDPpc')

# Rename first column
df.rename(columns={df.columns[0]: "year"}, inplace=True)

# Drop any unnecessary rows (in case of header junk)
df = df[df['year'].apply(lambda x: isinstance(x, (int, float)))]

# Melt the wide dataframe
df_melted = df.melt(id_vars='year', var_name='country', value_name='gdp_pc_2011usd')

# Drop missing data
df_melted.dropna(inplace=True)

# Save to processed folder
os.makedirs("data/processed", exist_ok=True)
df_melted.to_csv("data/processed/maddison_gdp.csv", index=False)

print(" GDP per capita data cleaned and saved.")
