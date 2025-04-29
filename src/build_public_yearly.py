import pandas as pd, yfinance as yf

TICKERS = ["NVDA", "AAPL", "MSFT", "META", "GOOGL", "AMZN"]
rows    = []

for t in TICKERS:
    yf_t = yf.Ticker(t)

    # 1) Market-cap  – robust field path
    mc = (yf_t.fast_info.get("marketCap")            # yfinance ≥0.2.37
          or yf_t.get_info().get("marketCap")        # legacy field
          or yf_t.basic_info.get("marketCap"))       # new fallback
    if mc is None:
        raise ValueError(f"marketCap missing for {t}")

    # 2) Employees  – Yahoo always stores fullTimeEmployees
    emp = (yf_t.fast_info.get("lastFullTimeEmployees")
           or yf_t.get_info().get("fullTimeEmployees"))

    rows.append({"ticker": t, "market_cap": mc, "employees": emp})
    print(f"{t:<5}  mc=${mc:,.0f}  emp={emp}")

pd.DataFrame(rows).to_csv("public_yearly.csv", index=False)
print("\n✅ public_yearly.csv written (look in project folder)")
