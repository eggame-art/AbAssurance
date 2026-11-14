from copy import deepcopy
from dataclasses import replace

import pytest

from model_training.config import TrainingConfig
from model_training.data import authorize
from model_training.evaluation import classification_metrics
from shared.errors import ConfigurationError


@pytest.fixture
def training_config():
    return TrainingConfig("hdfs://namenode:9000/datasets", "hdfs://namenode:9000/models")


@pytest.mark.parametrize(
    "changes",
    [
        {"features": ("customer_id",)},
        {"features": ("is_cancelled",)},
        {"features": ("horizon_end",)},
        {"features": ("cancellation_date",)},
        {"features": ()},
        {"features": ("claim_count", "claim_count")},
        {"dataset_id": "../bad"},
        {"dataset_root": "file:///tmp/data"},
        {"output_root": "hdfs://namenode:9000/datasets/models"},
        {"output_root": "hdfs://namenode:9000/%64ata"},
        {"mode": "override"},
        {"algorithm": "other"},
        {"imputation": "other"},
        {"class_weight": "other"},
        {"threshold": 1.0},
        {"threshold": float("nan")},
        {"reg_param": -1.0},
        {"elastic_net_param": 2.0},
        {"tolerance": 0},
        {"max_iter": 0},
        {"num_trees": True},
        {"max_depth": 31},
        {"seed": 2**40},
    ],
)
def test_invalid_training_configuration(training_config, changes):
    with pytest.raises(ConfigurationError):
        replace(training_config, **changes)


def ready_report(synthetic=False):
    return {
        "is_synthetic": synthetic,
        "training_ready": not synthetic,
        "synthetic_training_ready": False,
        "training_blockers": ["Insufficient evaluation customers"] if synthetic else [],
        "recipe": {"purpose": "synthetic-training" if synthetic else "training", "evaluation_start": "2026-01-01"},
        "target": {"column": "is_cancelled", "available": True},
        "validation": dict.fromkeys(
            (
                "temporal_non_overlap",
                "customer_disjoint",
                "contract_disjoint",
                "observation_key_unique",
                "persisted_parquet_verified",
            ),
            True,
        ),
    }


def test_authorization_preserves_readiness_guards(training_config):
    authorize(ready_report(), training_config)

    experimental = replace(training_config, mode="experimental")
    report = ready_report(True)
    original = deepcopy(report)

    authorize(report, experimental)
    assert report == original

    for data, config in (
        (report, training_config),
        (ready_report(), experimental),
        ({**ready_report(), "training_ready": False}, training_config),
        ({**ready_report(), "training_blockers": ["quality"]}, training_config),
    ):
        with pytest.raises(ConfigurationError):
            authorize(data, config)

    for field in original["validation"]:
        invalid = deepcopy(report)
        invalid["validation"][field] = False

        with pytest.raises(ConfigurationError):
            authorize(invalid, experimental)


def test_experiments_still_require_labels_history_and_chronology(training_config):
    config = replace(training_config, mode="experimental")

    for key, value in (
        ("target", {"column": "is_cancelled", "available": False}),
        ("recipe", {"purpose": "technical-validation"}),
        ("recipe", {"purpose": "synthetic-training", "evaluation_start": None}),
        ("is_synthetic", None),
    ):
        with pytest.raises(ConfigurationError):
            authorize({**ready_report(True), key: value}, config)


def test_positive_class_metrics_and_zero_denominators():
    metrics = classification_metrics(tn=5, fp=2, fn=1, tp=3)

    assert metrics["precision"] == pytest.approx(3 / 5)
    assert metrics["recall"] == pytest.approx(3 / 4)
    assert metrics["f1"] == pytest.approx(2 / 3)
    assert metrics["confusion_matrix"]["rows_actual_columns_predicted"] == [[5, 2], [1, 3]]
    assert metrics["class_counts"] == {"0": 7, "1": 4}
    assert classification_metrics(4, 0, 2, 0)["f1"] == 0
