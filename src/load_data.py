
import pandas as pd

def load_merged_data(path="data/processed/merged_data.csv"):
    try:
        df = pd.read_csv(path)
        print(f"✅ Loaded merged dataset with {df.shape[0]} rows and {df.shape[1]} columns.")
        return df
    except FileNotFoundError:
        print("Error will occur if mergeAllSources isn't ran first")
        return None
