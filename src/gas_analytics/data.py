"""Strict raw-data boundary: retain negative prices, reject invalid observations."""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F

from .config import EXOGENOUS, RAW_COLUMNS, TARGET


def normalize_raw(frame: DataFrame) -> DataFrame:
    """Adapters must supply the raw CSV contract; no inferred/logged price units."""
    if len(frame.columns) != len(RAW_COLUMNS) or set(frame.columns) != set(RAW_COLUMNS):
        raise ValueError(f"Expected exactly these raw columns: {list(RAW_COLUMNS)}. "
                         "compiled_dataset.csv is not supported: its units/preprocessing differ.")
    date_text = F.trim(F.col("Date").cast("string"))
    date = F.when(date_text.rlike(r"^\d{4}-\d{2}-\d{2}$"), date_text.try_cast("date"))
    parsed = frame.select(date.alias("date"), *[
        F.col(c).try_cast("double").alias(c) for c in (TARGET, *EXOGENOUS)
    ])
    # Missing covariates are allowed. Nonempty invalid numbers are a source error.
    invalid = []
    for c in (TARGET, *EXOGENOUS):
        text = F.trim(F.col(c).cast("string"))
        number = text.try_cast("double")
        invalid.append(text.isNotNull() & (text != "") &
                       (number.isNull() | F.isnan(number) | (F.abs(number) == float("inf"))))
    if frame.filter(reduce(lambda a, b: a | b, invalid)).limit(1).count():
        raise ValueError("Nonempty invalid, NaN, or infinite numeric value in raw input.")
    if parsed.filter(F.col("date").isNull() | F.col(TARGET).isNull()).limit(1).count():
        raise ValueError("Every observation requires a valid date and observed target price.")
    if parsed.groupBy("date").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate dates are ambiguous; resolve them at ingestion.")
    if not parsed.limit(1).count():
        raise ValueError("Input dataset is empty.")
    return parsed


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    return normalize_raw(spark.read.option("header", True).option("mode", "FAILFAST")
                         .option("inferSchema", False).csv(path))


def profile_raw(frame: DataFrame) -> dict:
    row = frame.agg(
        F.count("*").alias("rows"), F.min("date").alias("start"), F.max("date").alias("end"),
        F.min(TARGET).alias("target_min"), F.max(TARGET).alias("target_max"),
        F.sum(F.when(F.col(TARGET) <= 0, 1).otherwise(0)).alias("nonpositive_prices"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in EXOGENOUS],
    ).first().asDict()
    row["missing_covariates"] = {c: row.pop(c) for c in EXOGENOUS}
    return row
