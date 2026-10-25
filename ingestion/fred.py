from datetime import date, timedelta
from io import StringIO

import pandas as pd
import requests

from producer import publish

START_DATE = "2020-01-01"
END_DATE = "2026-10-21"

def fetch_and_publish(producer):
    series = {
        "DPRIME": "Bank Prime Loan Rate",
        "T10YIE": "10-Year Breakeven Inflation",
        "T5YIE": "5-Year Breakeven Inflation",
        "DFF": "Federal Funds Rate",
        "DGS10": "10-Year Treasury Yield",
        "DGS1": "1-Year Treasury Yield",
    }

    count = 0

    for sid, name in series.items():
        try:
            response = requests.get(
                "https://fred.stlouisfed.org/graph/fredgraph.csv",
                params={
                    "id": sid,
                    # "cosd": (
                    #     date.today() - timedelta(days=14)
                    # ).isoformat(),
                    "cosd": START_DATE,
                    "coed": END_DATE,
                },
                timeout=30,
            )
            response.raise_for_status()

            df = pd.read_csv(StringIO(response.text))

            if sid not in df.columns:
                print(f"FRED: unexpected response for {sid}")
                continue

            for _, row in df.iterrows():
                value = row[sid]

                if pd.isna(value) or value == ".":
                    continue

                if publish(
                    producer,
                    "fred",
                    name,
                    row["observation_date"],
                    value,
                ):
                    count += 1

        except requests.RequestException as exc:
            print(f"FRED error for {sid}: {exc}")

    print(f"FRED: published {count} observations")
    return count
