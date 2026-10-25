"""Chronological holdout tuning, then refit on train+validation and test once."""

import logging
import math
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, LinearRegression
from pyspark.sql import DataFrame, functions as F

from .artifacts import new_directory, read_json, write_json
from .config import Config
from .features import feature_names

LOG = logging.getLogger(__name__)


def regression_metrics(predictions: DataFrame) -> dict:
    valid = (F.col("prediction").isNotNull() & ~F.isnan("prediction") &
             (F.abs(F.col("prediction")) != float("inf")))
    error = F.col("prediction") - F.col("label")
    stats = predictions.agg(
        F.count("*").alias("n"), F.sum(valid.cast("int")).alias("valid"),
        F.avg(F.abs(error)).alias("mae"), F.avg(error * error).alias("mse"),
        F.var_pop("label").alias("variance"),
    ).first()
    if not stats.n or stats.valid != stats.n:
        raise ValueError("Evaluation requires a finite prediction for every labeled row.")
    return {"rows": stats.n, "mae": stats.mae, "rmse": math.sqrt(stats.mse),
            "r2": 1 - stats.mse / stats.variance if stats.variance > 0 else None}


def fit_preprocessor(train: DataFrame, columns: list[str]) -> PipelineModel:
    counts = train.agg(*[F.count(c).alias(c) for c in columns]).first().asDict()
    empty = [c for c, count in counts.items() if count == 0]
    if empty:
        raise ValueError(f"Entirely missing features in the fitting interval: {empty}")
    imputed = [f"{c}__imputed" for c in columns]
    return Pipeline(stages=[
        Imputer(strategy="median", inputCols=columns, outputCols=imputed, relativeError=0.001),
        VectorAssembler(inputCols=imputed, outputCol="features", handleInvalid="error"),
    ]).fit(train)


def candidate_parameters(config: Config) -> dict[str, list[dict]]:
    return {
        "linear_regression": [
            {"regParam": reg, "elasticNetParam": elastic}
            for reg in config.linear_reg_params for elastic in config.linear_elastic_net_params
            if reg != 0 or elastic == config.linear_elastic_net_params[0]
        ],
        "decision_tree": [
            {"maxDepth": depth, "minInstancesPerNode": minimum}
            for depth in config.tree_depths for minimum in config.tree_min_instances
        ],
    }


def estimator(name: str, parameters: dict, config: Config):
    if name == "linear_regression":
        # Spark's internal standardization is learned only from the fitting frame.
        return LinearRegression(featuresCol="features", labelCol="label", maxIter=200,
                                tol=1e-6, standardization=True, solver="auto", **parameters)
    if name == "decision_tree":
        return DecisionTreeRegressor(featuresCol="features", labelCol="label",
                                     seed=config.seed, maxBins=config.tree_max_bins, **parameters)
    raise ValueError(f"Unknown model: {name}")


def explain_model(name: str, fitted, columns: list[str]) -> dict:
    if name == "linear_regression":
        return {"intercept": float(fitted.intercept),
                "coefficients": dict(zip(columns, map(float, fitted.coefficients)))}
    return {"feature_importances": dict(zip(columns, map(float, fitted.featureImportances))),
            "depth": fitted.depth, "num_nodes": fitted.numNodes}


def train(spark, config: Config, prepared: str | Path, run_id: str | None = None) -> Path:
    prepared = Path(prepared).resolve()
    source = read_json(prepared / "manifest.json")
    if source.get("status") != "complete" or source["contract"] != config.feature_contract():
        raise ValueError("Prepared data contract does not match config; rerun preprocessing.")
    columns = feature_names(config)
    if source["feature_names"] != columns:
        raise ValueError("Prepared feature ordering does not match the current implementation.")
    if (source["config"]["train_fraction"] != config.train_fraction or
            source["config"]["validation_fraction"] != config.validation_fraction):
        raise ValueError("Split fractions changed; rerun preprocessing.")
    destination = new_directory(config, "runs", run_id)
    parts = {name: spark.read.parquet((prepared / name).as_uri()).cache()
             for name in ("train", "validation", "test")}
    caches = list(parts.values())
    try:
        preprocessor = fit_preprocessor(parts["train"], columns)
        vectors = {name: preprocessor.transform(parts[name]).select(
            "features", "label", "price_now").cache() for name in ("train", "validation")}
        caches += list(vectors.values())
        baseline_validation = regression_metrics(
            parts["validation"].withColumn("prediction", F.col("price_now")))
        tuning = []
        winners = {}
        for name, candidates in candidate_parameters(config).items():
            best = None
            for parameters in candidates:
                LOG.info("Validation: %s %s", name, parameters)
                fitted = estimator(name, parameters, config).fit(vectors["train"])
                scores = regression_metrics(fitted.transform(vectors["validation"]))
                record = {"model": name, "parameters": parameters, "validation": scores}
                tuning.append(record)
                if best is None or scores["rmse"] < best["validation"]["rmse"]:
                    best = record
            winners[name] = best
        # Choose deployment candidate before any test result is computed.
        validation_scores = {"persistence": baseline_validation,
                             **{name: best["validation"] for name, best in winners.items()}}
        selected = min(validation_scores, key=lambda name: validation_scores[name]["rmse"])
        LOG.info("Selected on validation RMSE: %s", selected)

        fit_frame = parts["train"].unionByName(parts["validation"])
        final_preprocessor = fit_preprocessor(fit_frame, columns)
        fit_vectors = final_preprocessor.transform(fit_frame).select("features", "label").cache()
        caches.append(fit_vectors)
        baseline = parts["test"].withColumn("prediction", F.col("price_now"))
        scores = {"persistence": regression_metrics(baseline)}
        predictions = [baseline.select("origin_date", "target_date", "label", "prediction",
                                       F.lit("persistence").alias("model"))]
        explanations = {}
        for name, best in winners.items():
            fitted = estimator(name, best["parameters"], config).fit(fit_vectors)
            pipeline = PipelineModel(stages=[*final_preprocessor.stages, fitted])
            pipeline.write().save((destination / name).as_uri())
            predicted = pipeline.transform(parts["test"]).cache()
            caches.append(predicted)
            scores[name] = regression_metrics(predicted)
            explanations[name] = explain_model(name, fitted, columns)
            predictions.append(predicted.select("origin_date", "target_date", "label", "prediction",
                                                F.lit(name).alias("model")))
        combined = predictions[0]
        for prediction in predictions[1:]:
            combined = combined.unionByName(prediction)
        combined.write.mode("errorifexists").parquet((destination / "test_predictions").as_uri())
        baseline_rmse = scores["persistence"]["rmse"]
        for score in scores.values():
            score["rmse_skill_vs_persistence"] = (
                1 - score["rmse"] / baseline_rmse if baseline_rmse > 0 else None)
        write_json(destination / "tuning.json", tuning)
        write_json(destination / "metrics.json", {
            "units": "USD/barrel", "selection_metric": "validation_rmse",
            "validation": validation_scores, "test": scores,
            "selected_model": selected,
            "best_parameters": {name: best["parameters"] for name, best in winners.items()},
        })
        write_json(destination / "explanations.json", explanations)
        write_json(destination / "manifest.json", {
            "status": "complete", "config": config.to_dict(),
            "contract": config.feature_contract(), "feature_names": columns,
            "prepared_data": str(prepared), "source": source["source"],
            "splits": source["splits"], "spark_version": spark.version,
            "selected_model": selected, "refit_interval": "train + validation",
            "refit_last_target_date": source["splits"]["validation"]["last_target_date"],
            "available_models": ["persistence", *winners],
        })
    finally:
        for cached in caches:
            cached.unpersist()
    return destination
