"""Local batch artifacts. Remote stores and service adapters are future work."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import uuid

from .config import Config
from .data import profile_raw, read_raw_csv
from .features import chronological_split, feature_names, make_features


def write_json(path: Path, value: dict | list) -> None:
    text = json.dumps(value, indent=2, ensure_ascii=False, default=str, allow_nan=False)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def new_directory(config: Config, category: str, run_id: str | None = None) -> Path:
    if run_id is None:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", run_id):
        raise ValueError("run-id must be 1-100 letters, digits, hyphens or underscores.")
    path = Path(config.output_dir).resolve() / category / run_id
    path.mkdir(parents=True, exist_ok=False)
    return path


def source_fingerprint(path: str) -> dict:
    result = {"path": path}
    if "://" not in path and Path(path).is_file():
        digest = hashlib.sha256()
        with Path(path).open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        result["sha256"] = digest.hexdigest()
    return result


def prepare(spark, config: Config, run_id: str | None = None) -> Path:
    destination = new_directory(config, "prepared", run_id)
    raw = read_raw_csv(spark, config.input_path).cache()
    samples = make_features(raw, config).cache()
    try:
        profile = profile_raw(raw)
        parts, splits = chronological_split(samples, config)
        for name, part in parts.items():
            part.write.mode("errorifexists").parquet((destination / name).as_uri())
        # A manifest is written last: its existence marks a completed preprocessing run.
        write_json(destination / "manifest.json", {
            "status": "complete", "contract": config.feature_contract(),
            "feature_names": feature_names(config), "config": config.to_dict(),
            "source": source_fingerprint(config.input_path), "raw_profile": profile,
            "splits": splits, "spark_version": spark.version,
            "excluded_warmup_rows": config.warmup_sessions,
            "excluded_unlabeled_tail_rows": 1,
        })
    finally:
        samples.unpersist()
        raw.unpersist()
    return destination
