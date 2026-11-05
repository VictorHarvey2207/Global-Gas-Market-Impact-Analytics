# Dealing with messy-ass data

The professor wants "bad" data so we can show off Spark. So the ingestion layer
**no longer cleans anything**, and `consumer.py` no longer cleans either. You get
raw strings. This file lists every kind of mess, where it comes from, and a short
pointer for fixing it.

Two kinds of mess:
- Natural – exactly what the APIs return, we just stopped filtering it.
- Injected – added on purpose by `ingestion/messify.py` (seeded, reproducible).
  Turn it off with `MESSY_MODE=0`; change the seed with `MESSY_SEED=<int>` (default 42).

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

## Dealing with it as Spark team

- Want clean data again for testing? `MESSY_MODE=0 python run_all.py` removes the injected mess only. The natural mess (blanks, NaN, mixed frequencies, raw strings) stays.
- Spark tips for this data: `explain()` your joins, broadcast the small dimension (`series_id` → canonical name/frequency), `repartition` by `series_id` before windows, cache the cleaned DF if reused.
