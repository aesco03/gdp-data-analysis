# src/fetch_fred_data.py

import os
from dotenv import load_dotenv
import pandas as pd
from fredapi import Fred

load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)

# Sample economic indicators (feel free to add more)
indicators = {
    "GDP": "GDPC1",                         # US Gross Domestic Product
    "Unemployment Rate": "UNRATE",       # US Unemployment Rate
    "CPI": "CPIAUCSL",                    # Consumer Price Index
    "Labor Force Participation": "CIVPART", 
    "Productivity": "OPHNFB",            # Nonfarm Business Sector: Output per Hour
    "Private Investment": "GPDIC1",
    "Private R&D (Nominal)": "Y006RC1Q027SBEA",
    "Gov R&D (Nominal)": "Y057RC1Q027SBEA",
    "Total R&D (Nominal GDP share)": "Y694RC1Q027SBEA",
}

output_dir = "data/raw/fred/"
os.makedirs(output_dir, exist_ok=True)

for name, code in indicators.items():
    print(f"Fetching {name} ({code})...")
    
    try:
        series = fred.get_series(code)
        df = series.reset_index()
        df.columns = ['date', code]

        # Print start and end date of the data
        start_date = df['date'].min()
        end_date = df['date'].max()
        print(f"  ➤ Date range: {start_date.date()} to {end_date.date()}")

        # Save to CSV
        df.to_csv(f"{output_dir}{code}.csv", index=False)
        print(f"  ➤ Saved {name} to {output_dir}{code}.csv\n")

    except Exception as e:
        print(f"❌ Error fetching {name} ({code}): {e}\n")

print("✅ Done fetching FRED data.")
