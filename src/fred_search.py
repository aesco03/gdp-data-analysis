import os
from dotenv import load_dotenv
from fredapi import Fred

load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)

# Search for available R&D series
print("Searching for R&D data series in FRED...")
search_results = fred.search('research and development')

print(f"Type of search_results: {type(search_results)}")

# If it's a DataFrame (which is likely)
if hasattr(search_results, 'columns'):
    print(f"Columns: {search_results.columns.tolist()}")
    
    # Display the first 20 results
    for i, (idx, row) in enumerate(search_results.iterrows()):
        if i < 20:
            # Adapt this based on the actual columns in the DataFrame
            print(f"ID: {row.get('id', idx)}, Title: {row.get('title', 'N/A')}")
        else:
            remaining = len(search_results) - 20
            print(f"...and {remaining} more results.")
            break
else:
    # If it's something else, just print it out directly
    print("Search results structure:")
    print(search_results)

print("\nTotal results found:", len(search_results))