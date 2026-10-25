"""Commands are orchestration only; transformations accept Spark DataFrames."""

import argparse
import json
import logging
from pathlib import Path

from .artifacts import prepare, read_json
from .config import Config, load_config
from .prediction import predict_latest
from .runtime import create_spark
from .training import train


def main(argv=None):
    parser = argparse.ArgumentParser(description="Spark batch oil-price forecasting")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("preprocess", "train", "run"):
        command = sub.add_parser(name)
        command.add_argument("--config", default="config/default.json")
        command.add_argument("--run-id", help="Optional unique name; existing output is never replaced")
        if name == "train":
            command.add_argument("--prepared", required=True)
    predict = sub.add_parser("predict")
    predict.add_argument("--run", required=True)
    predict.add_argument("--input", help="Raw CSV history; must include enough past observations")
    predict.add_argument("--model", choices=["persistence", "linear_regression", "decision_tree"])
    predict.add_argument("--output-dir")
    predict.add_argument("--spark-master", help="Override execution location, not feature settings")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.command == "predict":
        values = read_json(Path(args.run) / "manifest.json")["config"]
        if args.spark_master:
            values["spark_master"] = args.spark_master
        config = Config(**values)
    else:
        config = load_config(args.config)
    spark = create_spark(config)
    try:
        if args.command in ("preprocess", "run"):
            prepared = prepare(spark, config, args.run_id)
            print(json.dumps({"prepared": str(prepared)}), flush=True)
        if args.command in ("train", "run"):
            prepared = args.prepared if args.command == "train" else prepared
            result = train(spark, config, prepared, args.run_id)
            print(json.dumps({"run": str(result)}), flush=True)
        if args.command == "predict":
            result = predict_latest(spark, args.run, input_path=args.input, model_name=args.model,
                                    output_dir=args.output_dir)
            print(json.dumps({"predictions": str(result)}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
