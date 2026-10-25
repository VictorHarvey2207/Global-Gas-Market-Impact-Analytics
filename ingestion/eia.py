import os

import requests

from producer import publish

def fetch_and_publish(producer):
    api_key = "pbCjJGMGQMmMBQGLWjNTHQh7Dp9ncPNwjNtt9qLc"

    series = {
        "PET.RBRTE.D": "Brent Crude Oil",
        "PET.RWTC.D": "Cushing WTI Crude Oil",
        "NG.RNGWHHD.D": "Henry Hub Natural Gas",
    }

    count = 0

    for sid, name in series.items():
        try:
            response = requests.get(
                f"https://api.eia.gov/v2/seriesid/{sid}",
                params={"api_key": api_key},
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()

            if "response" not in result:
                print(f"EIA error for {name}: {result}")
                continue

            for row in result["response"]["data"][-5:]:
                value = row.get("value")

                if value is None or value == ".":
                    continue

                if publish(
                    producer,
                    "eia",
                    name,
                    row["period"],
                    value,
                ):
                    count += 1

        except (requests.RequestException, ValueError) as exc:
            print(f"EIA error for {name}: {exc}")

    print(f"EIA: published {count} observations")
    return count
