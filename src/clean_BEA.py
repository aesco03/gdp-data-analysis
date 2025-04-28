import pandas as pd
import os

input_path = '/Users/adrianescobar/Desktop/workspace/DataSci_3343/DA_FinalProj/gdp-data-analysis/data/raw/bea'
output_path = '/Users/adrianescobar/Desktop/workspace/DataSci_3343/DA_FinalProj/gdp-data-analysis/data/processed/bea'

# 🔥 Create the processed folder if it doesn't exist
os.makedirs(output_path, exist_ok=True)

file_name = 'gdp4q24-3rd.xlsx'
xlsx = pd.ExcelFile(os.path.join(input_path, file_name))
# Ignore ReadMe and Contents
tables = [sheet for sheet in xlsx.sheet_names if sheet.startswith('Table')]

for table in tables:
    print(f"Processing {table}...")
    
    df = pd.read_excel(
        os.path.join(input_path, file_name),
        sheet_name=table,
        skiprows=6  # You might have to adjust this depending on the actual file
    )

    # Remove empty columns
    df = df.dropna(how='all', axis=1)

    # Save clean CSV
    output_file = f"{table}_cleaned.csv"
    df.to_csv(os.path.join(output_path, output_file), index=False)

print("All BEA tables cleaned and saved.")
