"""Reload saved preprocessing and model; never fit using inference data."""

from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from .artifacts import new_directory, read_json, source_fingerprint, write_json
from .config import Config
from .data import read_raw_csv
from .features import feature_names, make_features


def predict_latest(spark, run: str | Path, *, input_path: str | None = None,
                   model_name: str | None = None, output_dir: str | None = None) -> Path:
    run = Path(run).resolve()
    manifest = read_json(run / "manifest.json")
    config = Config(**manifest["config"])
    if manifest.get("status") != "complete" or manifest["contract"] != config.feature_contract():
        raise ValueError("Model artifact is incomplete or uses a different feature contract.")
    if manifest["feature_names"] != feature_names(config):
        raise ValueError("Saved feature order differs from the current implementation.")
    name = model_name or manifest["selected_model"]
    if name not in manifest["available_models"]:
        raise ValueError(f"Unknown saved model: {name}")
    source = input_path or config.input_path
    raw = read_raw_csv(spark, source)
    latest = make_features(raw, config, with_labels=False).orderBy(F.desc("origin_date")).limit(1)
    if not latest.count():
        raise ValueError(f"Inference needs at least {config.warmup_sessions + 1} observations.")
    if name == "persistence":
        scored = latest.withColumn("prediction", F.col("price_now"))
    else:
        pipeline = PipelineModel.load((run / name).as_uri())
        scored = pipeline.transform(latest)
    row = scored.select("origin_date", "price_now", "prediction").first().asDict()
    # Explicitly distinguish historical scoring from a forecast beyond the fitting interval.
    row.update({"model": name, "units": "USD/barrel", "horizon_sessions": 1,
                "target_date": None, "target_date_note": "next observed session; calendar unknown",
                "after_fitting_interval": str(row["origin_date"]) >= manifest["refit_last_target_date"],
                "run": str(run), "source": source_fingerprint(source)})
    destination_config = Config(**{**config.to_dict(), "output_dir": output_dir or config.output_dir})
    destination = new_directory(destination_config, "predictions")
    write_json(destination / "prediction.json", row)
    return destination
