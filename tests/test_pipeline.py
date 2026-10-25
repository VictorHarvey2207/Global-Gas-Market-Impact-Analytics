from dataclasses import replace
from datetime import date, timedelta

import pytest
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from gas_analytics.artifacts import prepare, read_json
from gas_analytics.config import Config, EXOGENOUS, RAW_COLUMNS, TARGET
from gas_analytics.data import normalize_raw
from gas_analytics.features import chronological_split, feature_names, make_features
from gas_analytics.prediction import predict_latest
from gas_analytics.training import fit_preprocessor, train

pytestmark = pytest.mark.spark


def fixture_raw(spark, n=160):
    rows = []
    for i in range(n):
        price = -37.63 if i == 50 else 40 + i * 0.25 + (i % 7) * 0.2
        covariates = [None if 55 <= i <= 65 else str(10 + j + i * 0.1)
                      for j in range(len(EXOGENOUS))]
        rows.append((str(date(2020, 1, 1) + timedelta(days=i)), str(price), *covariates))
    return spark.createDataFrame(rows, list(RAW_COLUMNS))


def small_config(tmp_path):
    return Config(output_dir=str(tmp_path / "artifacts"), price_lags=(1, 3),
                  rolling_windows=(3, 5), linear_reg_params=(0.1,),
                  linear_elastic_net_params=(0.0,), tree_depths=(3,),
                  tree_min_instances=(3,), shuffle_partitions=2)


def test_negative_prices_preserved_and_bad_inputs_rejected(spark):
    raw = fixture_raw(spark, 80)
    normalized = normalize_raw(raw)
    assert normalized.agg(F.min(TARGET)).first()[0] == -37.63
    with pytest.raises(ValueError, match="Duplicate dates"):
        normalize_raw(raw.unionByName(raw.limit(1)))
    with pytest.raises(ValueError, match="invalid"):
        normalize_raw(raw.withColumn(EXOGENOUS[0], F.lit("Infinity")))
    with pytest.raises(ValueError, match="valid date"):
        normalize_raw(raw.withColumn("Date", F.lit("2020-02-31")))
    with pytest.raises(ValueError, match="compiled_dataset"):
        normalize_raw(raw.withColumn("MA_5", F.lit("1.2")))


def test_future_changes_cannot_modify_past_features(spark, tmp_path):
    config = small_config(tmp_path)
    raw = normalize_raw(fixture_raw(spark))
    cutoff = date(2020, 3, 1)
    changed = raw.withColumn(TARGET, F.when(F.col("date") > F.lit(cutoff), -9999)
                             .otherwise(F.col(TARGET)))
    for c in EXOGENOUS:
        changed = changed.withColumn(c, F.when(F.col("date") > F.lit(cutoff), 999999)
                                     .otherwise(F.col(c)))
    columns = ["origin_date", *feature_names(config)]
    before = make_features(raw, config).filter(F.col("origin_date") <= F.lit(cutoff))
    after = make_features(changed, config).filter(F.col("origin_date") <= F.lit(cutoff))
    assert before.select(columns).orderBy("origin_date").collect() == (
        after.select(columns).orderBy("origin_date").collect())
    # Tomorrow's label can change while today's features must not.
    assert before.filter(F.col("origin_date") == F.lit(cutoff)).first().label != (
        after.filter(F.col("origin_date") == F.lit(cutoff)).first().label)


def test_horizon_is_next_observation_not_next_calendar_day(spark, tmp_path):
    config = small_config(tmp_path)
    raw = normalize_raw(fixture_raw(spark, 80)).filter(F.dayofweek("date").between(2, 6))
    features = make_features(raw, config)
    friday = features.filter(F.dayofweek("origin_date") == 6).orderBy("origin_date").first()
    assert (friday.target_date - friday.origin_date).days == 3
    expected = raw.filter(F.col("date") == F.lit(friday.target_date)).first()[TARGET]
    assert friday.label == expected


def test_bounded_fill_never_backfills_from_future(spark, tmp_path):
    config = small_config(tmp_path)
    raw = normalize_raw(fixture_raw(spark))
    features = make_features(raw, config).orderBy("origin_date")
    c = f"{EXOGENOUS[0]}_available"
    # Origin 60: look at rows [54, 59], so row 54 remains available.
    day60 = date(2020, 1, 1) + timedelta(days=60)
    assert features.filter(F.col("origin_date") == F.lit(day60)).first()[c] == 15.4
    # Origin 61: only rows [55, 60], all missing; no future row can fill this gap.
    day61 = day60 + timedelta(days=1)
    row = features.filter(F.col("origin_date") == F.lit(day61)).first()
    assert row[c] is None
    assert row[f"{EXOGENOUS[0]}_missing"] == 1


def test_chronological_disjoint_splits_and_train_only_imputation(spark, tmp_path):
    config = small_config(tmp_path)
    features = make_features(normalize_raw(fixture_raw(spark)), config)
    parts, metadata = chronological_split(features, config)
    assert metadata["train"]["last_target_date"] < metadata["validation"]["first_target_date"]
    assert metadata["validation"]["last_target_date"] < metadata["test"]["first_target_date"]
    assert sum(m["rows"] for m in metadata.values()) == features.count()
    assert not parts["train"].join(parts["test"], "target_date").count()
    # Huge holdout values must not affect the fitted missing-value replacement.
    training = spark.createDataFrame([(1.0,), (3.0,), (None,)], "x double")
    holdout = spark.createDataFrame([(None,), (1e12,)], "x double")
    transformer = fit_preprocessor(training, ["x"])
    assert transformer.transform(holdout).filter(F.col("x").isNull()).first()["x__imputed"] == 1.0


def test_end_to_end_save_reload_and_inference_parity(spark, tmp_path):
    config = small_config(tmp_path)
    csv_path = tmp_path / "raw.csv"
    # Small fixture only; production code never collects the training corpus to Python.
    import csv
    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(RAW_COLUMNS)
        for row in fixture_raw(spark).collect():
            writer.writerow(row)
    config = replace(config, input_path=str(csv_path))
    prepared = prepare(spark, config, "fixture")
    run = train(spark, config, prepared, "fixture")
    metrics = read_json(run / "metrics.json")
    assert set(metrics["test"]) == {"linear_regression", "decision_tree", "persistence"}
    assert metrics["selected_model"] == min(
        metrics["validation"], key=lambda name: metrics["validation"][name]["rmse"])
    output = predict_latest(spark, run, model_name="linear_regression")
    prediction = read_json(output / "prediction.json")
    assert prediction["target_date"] is None
    assert prediction["origin_date"] == str(date(2020, 1, 1) + timedelta(days=159))

    pipeline = PipelineModel.load((run / "linear_regression").as_uri())
    from_train = spark.read.parquet((prepared / "test").as_uri())
    inference = make_features(normalize_raw(fixture_raw(spark)), config, with_labels=False)
    trained_predictions = pipeline.transform(from_train).select("origin_date", "prediction")
    inference_predictions = pipeline.transform(inference).select(
        "origin_date", F.col("prediction").alias("inference_prediction"))
    difference = trained_predictions.join(inference_predictions, "origin_date").agg(
        F.max(F.abs(F.col("prediction") - F.col("inference_prediction")))).first()[0]
    assert difference == pytest.approx(0, abs=1e-12)
    saved = spark.read.parquet((run / "test_predictions").as_uri()).filter(
        F.col("model") == "linear_regression")
    actual = saved.select("origin_date", F.col("prediction").alias("saved_prediction"))
    difference = trained_predictions.join(actual, "origin_date").agg(
        F.max(F.abs(F.col("prediction") - F.col("saved_prediction")))).first()[0]
    assert difference == pytest.approx(0, abs=1e-12)
    with pytest.raises(FileExistsError):
        train(spark, config, prepared, "fixture")
