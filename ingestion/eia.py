import requests
from producer import publish

API_KEY = "pbCjJGMGQMmMBQGLWjNTHQh7Dp9ncPNwjNtt9qLc"

# Same commodities at daily / weekly / monthly frequency. Period formats
# differ per frequency (YYYY-MM-DD, YYYY-MM-DD for week-ending, YYYY-MM).
SERIES = {
    "PET.RBRTE.D": "Brent Crude Oil",
    "PET.RWTC.D": "Cushing WTI Crude Oil",
    "NG.RNGWHHD.D": "Henry Hub Natural Gas",
    "PET.RBRTE.W": "Brent Crude Oil Weekly",
    "PET.RWTC.W": "Cushing WTI Crude Oil Weekly",
    "NG.RNGWHHD.W": "Henry Hub Natural Gas Weekly",
    "PET.RBRTE.M": "Brent Crude Oil Monthly",
    "PET.RWTC.M": "Cushing WTI Crude Oil Monthly",
    "NG.RNGWHHD.M": "Henry Hub Natural Gas Monthly",
}

def fetch_and_publish(producer):
    count = 0
    for sid, name in SERIES.items():
        try:
            response = requests.get(f"https://api.eia.gov/v2/seriesid/{sid}",params={"api_key": API_KEY, "length": 5000},timeout=30)
            response.raise_for_status()
            result = response.json()
            if "response" not in result:
                print(f"EIA error for {name}: {result}")
                continue
            for row in result["response"]["data"]:   # NO null / "." filtering
                extra = {"unit": row.get("units"),"description": row.get("series-description")}
                if publish(producer, "eia", name, row.get("period"),row.get("value"), series_id=sid, extra=extra):
                    count += 1
        except (requests.RequestException, ValueError) as exc:
            print(f"EIA error for {name}: {exc}")
    print(f"EIA: buffered {count} observations")
    return count