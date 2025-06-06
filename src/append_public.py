import pandas as pd, yfinance as yf
from data_helpers import get_employee_count

def public_snapshot(ticker, sector="tech"):
    t   = yf.Ticker(ticker)
    mc  = t.fast_info["lastMarketCap"]
    emp = get_employee_count(ticker)
    rev = t.get_income_stmt().loc["Total Revenue"][0] if emp else None
    return {
        "company": ticker,
        "date": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "valuation_usd": mc,
        "employees": emp,
        "accession_or_source": "yfinance/10-K",
        "revenue_per_employee": round(rev/emp) if rev and emp else None,
        "tech_sector": sector,
        "acquirer": "Public"
    }

df = pd.read_csv("private_events.csv")
df = pd.concat([df, pd.DataFrame([public_snapshot("NVDA","semiconductor")])], ignore_index=True)
df.to_csv("private_events.csv", index=False)
print("✅ Nvidia snapshot appended to private_events.csv")
