from dataclasses import replace

import pytest

from dataset_preparation.config import PreparationConfig
from shared.errors import ConfigurationError


def config(**changes: object) -> PreparationConfig:
    values = PreparationConfig(
        source_root="hdfs://namenode:9000/abassurance",
        output_root="hdfs://namenode:9000/abassurance-datasets/churn",
        observation_date="2025-07-01",
    )

    return replace(values, **changes)


@pytest.mark.parametrize("value", ["2025-02-30", "20250701", "2025-7-1", "yesterday", ""])
def test_explicit_iso_observation_date(value):
    with pytest.raises(ConfigurationError, match="YYYY-MM-DD"):
        config(observation_date=value)


@pytest.mark.parametrize(
    "value",
    [
        "file:///tmp/data",
        "hdfs://namenode:9000/abassurance",
        "hdfs://namenode:9000/abassurance/snapshots/output",
        "hdfs://namenode:9000/abassurance/../data",
        "hdfs://namenode:9000/abassurance/%2e%2e/data",
        "hdfs://alias:9000/abassurance",
    ],
)
def test_output_must_be_independent_hdfs_root(value):
    with pytest.raises(ConfigurationError):
        config(output_root=value)


def test_training_fails_closed_without_invented_labels():
    with pytest.raises(ConfigurationError, match="effective cancellation date"):
        config().require_supported_purpose()

    config(purpose="technical-validation").require_supported_purpose()


def test_cli_refuses_training_before_creating_spark(monkeypatch, caplog):
    from dataset_preparation.run import main

    monkeypatch.setattr("sys.argv", ["prepare", "--observation-date", "2025-07-01"])

    assert main() == 1
    assert "Training dataset unavailable" in caplog.text
