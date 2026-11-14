from dataclasses import replace

import pytest

from dataset_preparation.config import PreparationConfig
from dataset_preparation.demo import SCENARIOS, synthetic_records
from shared.errors import ConfigurationError


def config(**changes: object) -> PreparationConfig:
    settings = PreparationConfig(
        source_root="hdfs://namenode:9000/abassurance",
        output_root="hdfs://namenode:9000/abassurance-synthetic/dataset",
        history_root="hdfs://namenode:9000/abassurance-synthetic/history",
        observation_date="2025-07-01",
        purpose="synthetic-training",
    )

    return replace(settings, **changes)


@pytest.mark.parametrize("horizon", [0, -1, 3651, True])
def test_invalid_horizon_is_refused(horizon):
    with pytest.raises(ConfigurationError, match="horizon-days"):
        config(horizon_days=horizon)


def test_history_and_horizon_are_explicit_in_training_configuration():
    config().require_supported_purpose()

    settings = config(cancellation_types=("voluntary", "voluntary"))

    assert settings.cancellation_types == ("voluntary",)

    with pytest.raises(ConfigurationError, match="cancellation-types"):
        config(cancellation_types=("expired",))

    with pytest.raises(ConfigurationError, match="must not overlap"):
        config(output_root="hdfs://namenode:9000/abassurance-synthetic/history/dataset")


def test_demo_is_deterministic_synthetic_and_separate_from_existing_contracts():
    records = synthetic_records()

    assert records == synthetic_records()
    assert len(records["contracts"]) == 104
    assert len(SCENARIOS) == 13

    assert all(row["is_synthetic"] for rows in records.values() for row in rows)
    assert all("SYNTH-" in row["contract_id"] for row in records["contracts"])
    assert len({row["contract_id"] for row in records["contracts"]}) == 104
