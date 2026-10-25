"""Small, explicit configuration contract; paths are relative to the config file."""

from dataclasses import asdict, dataclass, fields
import json
import math
from pathlib import Path

TARGET = "cushing_crude_oil_price"
EXOGENOUS = (
    "dow_jones_adj_close_price",
    "nasdaq_adj_close_price",
    "sp_adj_close_price",
    "usd_to_uk_exchange_rate",
    "federal_funds_rate",
    "bank_prime_loan_rate",
    "treasury_1_year_rate",
    "breakeven_inflation_5_year_rate",
)
RAW_COLUMNS = ("Date", TARGET, *EXOGENOUS)
CONTRACT_VERSION = 1


@dataclass(frozen=True)
class Config:
    input_path: str = "data/raw/oil_data.csv"
    output_dir: str = "artifacts"
    target: str = TARGET
    horizon_sessions: int = 1
    price_lags: tuple[int, ...] = (1, 2, 5, 10)
    rolling_windows: tuple[int, ...] = (5, 10, 20)
    exogenous_lag_sessions: int = 1
    forward_fill_sessions: int = 5
    train_fraction: float = 0.7
    validation_fraction: float = 0.15
    linear_reg_params: tuple[float, ...] = (0.0, 0.1, 1.0)
    linear_elastic_net_params: tuple[float, ...] = (0.0, 0.5)
    tree_depths: tuple[int, ...] = (3, 5, 8)
    tree_min_instances: tuple[int, ...] = (10, 30)
    tree_max_bins: int = 32
    seed: int = 42
    spark_master: str | None = "local[2]"
    shuffle_partitions: int = 4

    def __post_init__(self):
        if self.target != TARGET or self.horizon_sessions != 1:
            raise ValueError("This contract supports raw oil price and a one-session horizon only.")
        for name in ("price_lags", "rolling_windows", "tree_depths", "tree_min_instances"):
            values = getattr(self, name)
            if not values or any(type(x) is not int or x < 1 for x in values):
                raise ValueError(f"{name} must contain positive integers.")
            if len(set(values)) != len(values):
                raise ValueError(f"{name} must not contain duplicates.")
        if max(self.tree_depths) > 30:
            raise ValueError("Spark decision trees support maxDepth <= 30.")
        for name in ("exogenous_lag_sessions", "forward_fill_sessions"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer.")
        for name in ("shuffle_partitions", "tree_max_bins"):
            value = getattr(self, name)
            if type(value) is not int or value < 2:
                raise ValueError(f"{name} must be an integer >= 2.")
        if not (0 < self.train_fraction < 1 and 0 < self.validation_fraction < 1
                and self.train_fraction + self.validation_fraction < 1):
            raise ValueError("Train/validation fractions must leave a nonempty test fraction.")
        for name, upper in (("linear_reg_params", math.inf), ("linear_elastic_net_params", 1)):
            values = getattr(self, name)
            if not values or any(not math.isfinite(x) or not 0 <= x <= upper for x in values):
                raise ValueError(f"Invalid values for {name}.")

    @property
    def warmup_sessions(self) -> int:
        return max(max(self.price_lags), max(self.rolling_windows) - 1,
                   self.exogenous_lag_sessions)

    def to_dict(self) -> dict:
        return json.loads(json.dumps(asdict(self)))

    def feature_contract(self) -> dict:
        keys = ("target", "horizon_sessions", "price_lags", "rolling_windows",
                "exogenous_lag_sessions", "forward_fill_sessions")
        data = self.to_dict()
        return {"version": CONTRACT_VERSION, "target_scale": "raw_usd_per_barrel",
                **{k: data[k] for k in keys}}


def load_config(path: str | Path) -> Config:
    path = Path(path).resolve()
    values = json.loads(path.read_text(encoding="utf-8"))
    unknown = set(values) - {f.name for f in fields(Config)}
    if unknown:
        raise ValueError(f"Unknown config keys: {sorted(unknown)}")
    for key in ("input_path", "output_dir"):
        value = values.get(key, getattr(Config(), key))
        if "://" not in value:
            value = str((path.parent / value).resolve())
        values[key] = value
    # Only the batch CSV reader accepts Spark-readable URIs. Artifact management is local.
    if "://" in values["output_dir"]:
        raise ValueError("output_dir must be a local path; remote artifact storage is future work.")
    return Config(**values)
