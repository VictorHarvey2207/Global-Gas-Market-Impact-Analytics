import yfinance as yf

from producer import publish

def fetch_and_publish(producer):
    tickers = {
        "^DJI": "Dow Jones",
        "^IXIC": "NASDAQ",
        "^GSPC": "S&P 500",
    }

    stocks = yf.download(
        list(tickers),
        period="14d",
        interval="1d",
        auto_adjust=True,
        progress=False,
    )

    if stocks.empty or "Close" not in stocks:
        print("Yahoo Finance returned no data.")
        return 0

    count = 0
    closes = stocks["Close"]

    for ticker, name in tickers.items():
        if ticker not in closes.columns:
            continue

        for dt, value in closes[ticker].dropna().items():
            if publish(producer, "yfinance", name, dt.date(), value):
                count += 1

    print(f"Yahoo Finance: published {count} observations")
    return count
