import json

import pytest

from gas_analytics.config import Config, load_config
from gas_analytics.training import candidate_parameters


def test_config_paths_follow_config_location(tmp_path, monkeypatch):
    folder = tmp_path / "settings"
    folder.mkdir()
    path = folder / "experiment.json"
    path.write_text(json.dumps({"input_path": "../data/raw.csv", "output_dir": "../results"}))
    monkeypatch.chdir(tmp_path.parent)
    config = load_config(path)
    assert config.input_path == str(tmp_path / "data" / "raw.csv")
    assert config.output_dir == str(tmp_path / "results")


def test_does_not_silently_change_horizon_or_accept_empty_holdout():
    with pytest.raises(ValueError, match="one-session"):
        Config(horizon_sessions=2)
    with pytest.raises(ValueError, match="test fraction"):
        Config(train_fraction=0.8, validation_fraction=0.2)
    with pytest.raises(ValueError, match="nonnegative"):
        Config(exogenous_lag_sessions=-1)


def test_unknown_config_key_fails_instead_of_ignoring_typo(tmp_path):
    path = tmp_path / "config.json"
    path.write_text('{"horizon_sesions": 10}')
    with pytest.raises(ValueError, match="Unknown config"):
        load_config(path)


def test_unregularized_linear_candidate_is_not_duplicated():
    candidates = candidate_parameters(Config())["linear_regression"]
    assert len([c for c in candidates if c["regParam"] == 0]) == 1
