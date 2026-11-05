# Dealing with messy-ass data

The professor wants "bad" data so we can show off Spark. So the ingestion layer
**no longer cleans anything**, and `consumer.py` no longer cleans either. You get
raw strings. This file lists every kind of mess, where it comes from, and a short
pointer for fixing it.

Two kinds of mess:
- **Natural** – exactly what the APIs return, we just stopped filtering it.
- **Injected** – added on purpose by `ingestion/messify.py` (seeded, reproducible).
  Turn it off with `MESSY_MODE=0`; change the seed with `MESSY_SEED=<int>` (default 42).

## What a message looks like now

```json
{"source": "fred", "indicator": "Federal Funds Rate", "series_id": "DFF",
 "date": "2024-01-05", "value": "5.33", "ingested_at": "2026-11-05T10:00:00.123",
 "unit": null, "description": null}
```

- **Every field is a string (or null).** `consumer.py` returns all columns as `StringType`.
  `value` and `date` are *not* typed – that is your job.
- New fields: `series_id` (stable ID, use it instead of `indicator`), `ingested_at`,
  `unit` / `description` (EIA only, null elsewhere).

## Natural mess (from the sources)

| Mess | Where | Pointer |
|---|---|---|
| Missing values: FRED sends an empty field on holidays (`2024-12-25,`), EIA sends `null`, Yahoo sends NaN | all | `df.filter(col("value").isNotNull())` after casting; or impute with `last(..., ignorenulls=True)` over a window ordered by date |
| NaNs from different exchange calendars (FTSE/DAX/Nikkei closed when Dow is open) | yfinance | Don't drop blindly; forward-fill per `series_id` with a window |
| Mixed frequencies: daily, weekly, monthly, quarterly series side by side (FRED `GASREGW` weekly, `CPIAUCSL`/`UNRATE` monthly, `GDP` quarterly; EIA `.D/.W/.M`) | fred, eia | Check `series_id` suffix / known frequency. Resample to one grain (e.g. `date_trunc("month", date)` + `avg`), or forward-fill low-frequency series onto a daily calendar via join |
| Same thing from two sources (WTI and Henry Hub appear in both EIA and FRED; Brent/WTI/NG also at 3 frequencies in EIA; Yahoo futures too) | eia, fred, yfinance | Pick one source per commodity, or join and compare them – they won't match exactly |
| Different date formats per frequency: EIA monthly is `YYYY-MM` (no day), others `YYYY-MM-DD` | eia | Handle `YYYY-MM` explicitly (`to_date(col, "yyyy-MM")`) |
| Multiple price fields: `Close`, `Adj Close`, `Volume` are separate indicators (`"Dow Jones Volume"` etc.) | yfinance | Filter by indicator suffix or by `series_id` + naming; don't average prices with volumes |
| Volume for some tickers/indices may be 0 or NaN (unchecked – inspect it) | yfinance | If 0 volume is meaningless for a series, treat it as missing |
| EIA default range/order (we no longer pass start/end/sort) | eia | Don't assume ordering – always `orderBy` / use windows with explicit `orderBy("date")` |
| Records for the same date can have different `ingested_at` if re-run | all | Dedup by key, keep latest `ingested_at` |

## Injected mess (`messify.py`) – approximate rates per record

| Mess | Rate | Example | Pointer |
|---|---|---|---|
| **Exact duplicates** | ~3% | same message twice | `dropDuplicates(["source","indicator","series_id","date","value"])` |
| **Conflicting duplicates** (same key, value ±3%, later `ingested_at`) | ~1% | `5.33` and `5.4912` | Window by `(series_id, date)`, `row_number()` ordered by `ingested_at desc`, keep 1. Or average – justify the choice |
| **Null value** | ~2% | `"value": null` | `isNotNull` / impute |
| **Junk strings in value** | ~1.5% | `"N/A"`, `"n/a"`, `"null"`, `""`, `"."`, `"--"`, `"NaN"`, `"missing"` | `col("value").cast("double")` turns them to null (no error). Cast first, then handle nulls. Note `"NaN"` casts to a real NaN – use `isnan` too |
| **Sentinel values** | ~1% | `-999`, `-9999`, `0`, `999999` | Replace known sentinels with null (`when(col("v").isin(...), None)`) *before* computing stats. `0` is only wrong for series that can't be 0 (prices, yields) |
| **Outliers** | ~0.5% | value ×100, ÷100, ×1000, or negated | z-score / IQR per `series_id`, or compare to a rolling median over a window; flag, don't silently drop |
| **Formatted numbers** | ~4% | `"1,234.56"`, `" 5.33 "`, `"$45.10"`, `"5.33%"`, `"5.330000e+00"` | `regexp_replace(col("value"), r"[,$%\s]", "")` then `cast("double")` |
| **Mixed date formats** | ~18% | `01/05/2024`, `05-Jan-2024`, `20240105`, `2024/01/05`, `05.01.2024`, `January 05, 2024`, `2024-01-05T00:00:00Z` | Try several formats with `coalesce(to_date(c,"yyyy-MM-dd"), to_date(c,"MM/dd/yyyy"), ...)`. **`01/05/2024` is US month/day; `05.01.2024` is EU day.month** – the separator tells you which. Set `spark.sql.legacy.timeParserPolicy=LEGACY` if Spark 3 throws on parse |
| **Unparseable dates** | ~2% | `"TBD"`, `""`, `"unknown"`, `"0000-00-00"` | They become null after `to_date` – filter them (or quarantine into a bad-records dataset) |
| **Indicator name variants** | ~5% | `"s&p 500"`, `" S&P 500"`, `"S&P_500"`, `"S&P500"`, `"SandP 500"` | Normalize: `lower(trim(...))`. Better: **group by `series_id`** (never messed with) and map it to a canonical name |
| **Source name variants** | ~3% | `"FRED"`, `"Fred"`, `"fred_api"`, `" fred"` | `lower(trim(regexp_replace(source, "_api$", "")))` |
| **Out-of-order delivery** | all | records shuffled in windows of 500 | Never rely on arrival order; always `orderBy("date")` / window `orderBy` |

> `series_id` is **not** messed with, so it is the reliable key. `indicator`,
> `source`, `date`, `value` are the messy columns.

## Suggested cleaning order

1. Read raw (`consumer.read_market_data`) – keep a copy of the raw DF (`.cache()` or write Parquet) so you can compare before/after.
2. Normalize text: `trim`, `lower` on `source`; map names via `series_id`.
3. Parse dates (multiple formats), quarantine unparseable ones.
4. Clean value strings (regex) → `cast("double")`; replace sentinels with null.
5. Flag/handle outliers per `series_id`.
6. Dedup (exact, then conflicting by latest `ingested_at`).
7. Handle nulls (drop or forward-fill by window).
8. Align frequencies (resample / join to a calendar).
9. Write cleaned data to Parquet partitioned by `source` or year.

Count rows at every step – the numbers make a good slide for the report.

## Dealing with it as a "next guy"

- Want clean data again for testing? `MESSY_MODE=0 python run_all.py` removes the **injected** mess only. The natural mess (blanks, NaN, mixed frequencies, raw strings) stays.
- Rates above are from `messify.py` constants – if you change them there, update this file.
- Spark tips for this data: `explain()` your joins, broadcast the small dimension (`series_id` → canonical name/frequency), `repartition` by `series_id` before windows, cache the cleaned DF if reused.
