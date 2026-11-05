from io import StringIO
import pandas as pd
import requests
from producer import publish

START_DATE = "2020-01-01"
END_DATE = "2026-10-21"

# Mixed frequencies on purpose: daily, weekly, monthly, quarterly.
SERIES = {
    "DPRIME": "Bank Prime Loan Rate",            # daily
    "T10YIE": "10-Year Breakeven Inflation",     # daily
    "T5YIE": "5-Year Breakeven Inflation",       # daily
    "DFF": "Federal Funds Rate",                 # daily
    "DGS10": "10-Year Treasury Yield",           # daily
    "DGS1": "1-Year Treasury Yield",             # daily
    "DCOILWTICO": "FRED WTI Crude Oil",          # daily (overlaps EIA)
    "DHHNGSP": "FRED Henry Hub Natural Gas",     # daily (overlaps EIA)
    "GASREGW": "US Regular Gas Price",           # weekly
    "CPIAUCSL": "CPI All Urban Consumers",       # monthly
    "UNRATE": "Unemployment Rate",               # monthly
    "GDP": "Gross Domestic Product",             # quarterly
}

def fetch_and_publish(producer):
    count = 0
    for sid, name in SERIES.items():
        try:
            response = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv",params={"id": sid, "cosd": START_DATE, "coed": END_DATE},timeout=30)
            response.raise_for_status()
            # dtype=str + keep_default_na=False: keep "", ".", etc. exactly as sent
            df = pd.read_csv(StringIO(response.text), dtype=str,keep_default_na=False)
            date_col = "observation_date" if "observation_date" in df.columns else "DATE"
            if sid not in df.columns or date_col not in df.columns:
                print(f"FRED: unexpected response for {sid}")
                continue
            for _, row in df.iterrows():      # NO filtering of blanks / "."
                if publish(producer, "fred", name, row[date_col], row[sid],
                           series_id=sid):
                    count += 1
        except requests.RequestException as exc:
            print(f"FRED error for {sid}: {exc}")
    print(f"FRED: buffered {count} observations")
    return count