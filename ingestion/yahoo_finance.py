import yfinance as yf
from producer import publish

START_DATE = "2020-01-01"
END_DATE = "2026-10-21"

# Several markets with different holiday calendars -> NaN gaps on the days
# one exchange is closed but another is open (kept on purpose).
TICKERS = {
    "^DJI": "Dow Jones",
    "^IXIC": "NASDAQ",
    "^GSPC": "S&P 500",
    "^FTSE": "FTSE 100",
    "^GDAXI": "DAX",
    "^N225": "Nikkei 225",
    "CL=F": "WTI Crude Futures",
    "NG=F": "Natural Gas Futures",
}

# raw prices, plus separate Adj Close
# dividends / splits columns (mostly 0 / NaN)
def fetch_and_publish(producer):
    stocks = yf.download(list(TICKERS),start=START_DATE,end=END_DATE,interval="1d",auto_adjust=False,actions=True,progress=False)
    if stocks.empty:
        print("Yahoo Finance returned no data.")
        return 0
    count = 0
    # Wide frame: one row per calendar day any ticker traded; NaNs preserved.
    for field in ("Close", "Adj Close", "Volume"):
        if field not in stocks:
            continue
        frame = stocks[field]
        for ticker, name in TICKERS.items():
            if ticker not in frame.columns:
                continue
            label = name if field == "Close" else f"{name} {field}"
            for dt, value in frame[ticker].items():   # NO dropna
                if publish(producer, "yfinance", label, dt.date(), value, series_id=ticker):
                    count += 1
    print(f"Yahoo Finance: buffered {count} observations")
    return count
