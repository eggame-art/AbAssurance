import hashlib
import json
from dataclasses import replace
from math import ceil, log

import pytest
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_preparation.config import PreparationConfig
from abassurance.systems.data_preparation.demo import synthetic_records
from abassurance.systems.data_preparation.run import dataset_recipe


def config(**changes):
    values = {
        "source_root": "hdfs://namenode:9000/abassurance",
        "output_root": "hdfs://namenode:9000/datasets",
        "history_root": "hdfs://namenode:9000/history",
        "observation_date": "2025-01-01",
        "observation_dates": ("2025-01-01", "2025-07-01"),
        "evaluation_start": "2025-07-01",
    }

    return PreparationConfig(**(values | changes))


def test_canonical_dates_and_sample_plan_are_reproducible():
    assert config(observation_dates=("2025-07-01", "2025-01-01")) == config()
    assert config().required_evaluation_customers_per_class == ceil(log(40) / 0.02) == 185
    assert config(evaluation_confidence=0.51, evaluation_margin=0.49).required_evaluation_customers_per_class == 3


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"observation_dates": ("2025-01-01", "2025-01-01")}, "Duplicate"),
        ({"observation_dates": ("2025-02-30", "2025-07-01")}, "YYYY-MM-DD"),
        ({"evaluation_start": None}, "require --evaluation-start"),
        ({"evaluation_start": "2024-12-01"}, "both sides"),
        ({"observation_dates": ("2025-06-01", "2025-07-01")}, "overlap"),
        ({"embargo_days": 200}, "overlap"),
        ({"embargo_days": -1}, "non-negative"),
        ({"evaluation_confidence": 1}, "confidence"),
        ({"evaluation_margin": 0}, "margin"),
        ({"purpose": "technical-validation", "history_root": None}, "cannot certify"),
    ],
)
def test_invalid_or_impossible_split_is_refused(changes, message):
    with pytest.raises(ConfigurationError, match=message):
        config(**changes)


def test_existing_single_date_command_remains_valid_but_has_no_independent_evaluation():
    single = replace(config(), observation_dates=(), evaluation_start=None)

    assert single.dates == ("2025-01-01",)


def test_temporal_demo_preserves_base_demo_and_adds_later_disjoint_identities():
    baseline = synthetic_records(13)
    temporal = synthetic_records(13, temporal_cohorts=True)

    assert temporal == synthetic_records(13, temporal_cohorts=True)
    assert len({row["contract_id"] for row in temporal["contracts"]}) == 52

    for entity, rows in baseline.items():
        assert all(row in temporal[entity] for row in rows)

    assert all(row["is_synthetic"] for rows in temporal.values() for row in rows)


def test_dataset_identifier_includes_all_split_settings():
    settings = config()

    def identifier(value):
        recipe = dataset_recipe(value, value.history_root, {"snapshot": "fixed"})
        return hashlib.sha256(json.dumps(recipe, sort_keys=True).encode()).hexdigest()

    changed = [
        replace(settings, embargo_days=1),
        replace(settings, evaluation_margin=0.2),
        replace(settings, evaluation_confidence=0.9),
        replace(settings, evaluation_start="2025-06-30"),
        replace(settings, observation_dates=("2025-01-01", "2025-02-01", "2025-07-01")),
    ]

    assert all(identifier(value) != identifier(settings) for value in changed)
    assert identifier(replace(settings, observation_dates=tuple(reversed(settings.dates)))) == identifier(settings)
