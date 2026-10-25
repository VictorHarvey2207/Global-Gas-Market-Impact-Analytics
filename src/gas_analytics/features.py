"""Causal, observation-based features shared by training and batch inference."""

from pyspark.sql import DataFrame, Window, functions as F

from .config import Config, EXOGENOUS, TARGET


def feature_names(config: Config) -> list[str]:
    return (["price_now"] + [f"price_lag_{n}" for n in config.price_lags]
            + [name for n in config.rolling_windows
               for name in (f"price_mean_{n}", f"price_std_{n}")]
            + ["price_change_1"]
            + [f"{c}_available" for c in EXOGENOUS]
            + [f"{c}_missing" for c in EXOGENOUS])


def make_features(raw: DataFrame, config: Config, *, with_labels: bool = True) -> DataFrame:
    """At close of origin t, predict the next observed price t+1 (not calendar t+1).

    One series is deliberately ordered globally. This stage has a single-series
    window bottleneck; partition by instrument before extending to multiple markets.
    No interpolation, backfill, percentage return, or logarithm is applied.
    """
    order = Window.orderBy("date")
    filled = raw
    available = Window.orderBy("date").rowsBetween(
        -config.exogenous_lag_sessions - config.forward_fill_sessions,
        -config.exogenous_lag_sessions,
    )
    expressions = [F.col(TARGET).alias("price_now"),
                   F.row_number().over(order).alias("_observation")]
    for n in config.price_lags:
        expressions.append(F.lag(TARGET, n).over(order).alias(f"price_lag_{n}"))
    for n in config.rolling_windows:
        window = order.rowsBetween(-n + 1, 0)
        expressions += [F.avg(TARGET).over(window).alias(f"price_mean_{n}"),
                        F.stddev_pop(TARGET).over(window).alias(f"price_std_{n}")]
    expressions.append((F.col(TARGET) - F.lag(TARGET).over(order)).alias("price_change_1"))
    for c in EXOGENOUS:
        expressions.append(F.last(c, ignorenulls=True).over(available).alias(f"{c}_available"))
    if with_labels:
        expressions += [F.lead(TARGET).over(order).alias("label"),
                        F.lead("date").over(order).alias("target_date")]
    filled = filled.select(F.col("date").alias("origin_date"), *expressions)
    for c in EXOGENOUS:
        filled = filled.withColumn(f"{c}_missing", F.col(f"{c}_available").isNull().cast("double"))
    # Labels are attached before filtering warmup, so the horizon cannot change.
    filled = filled.filter(F.col("_observation") > config.warmup_sessions).drop("_observation")
    columns = ["origin_date", *feature_names(config)]
    if with_labels:
        columns += ["target_date", "label"]
        filled = filled.filter(F.col("label").isNotNull())
    return filled.select(*columns)


def chronological_split(frame: DataFrame, config: Config) -> tuple[dict[str, DataFrame], dict]:
    size = frame.count()
    train_end = int(size * config.train_fraction)
    val_end = train_end + int(size * config.validation_fraction)
    if min(train_end, val_end - train_end, size - val_end) < 10:
        raise ValueError("Need at least 10 labeled examples in each chronological split.")
    ranked = frame.withColumn("_row", F.row_number().over(Window.orderBy("target_date")))
    boundaries = ranked.filter(F.col("_row").isin(train_end, val_end)).orderBy("_row").collect()
    train_date, validation_date = (r.target_date for r in boundaries)
    parts = {
        "train": frame.filter(F.col("target_date") <= F.lit(train_date)),
        "validation": frame.filter((F.col("target_date") > F.lit(train_date)) &
                                   (F.col("target_date") <= F.lit(validation_date))),
        "test": frame.filter(F.col("target_date") > F.lit(validation_date)),
    }
    metadata = {}
    for name, part in parts.items():
        metadata[name] = part.agg(F.count("*").alias("rows"),
                                  F.min("target_date").alias("first_target_date"),
                                  F.max("target_date").alias("last_target_date")).first().asDict()
    return parts, metadata
