import json
from dataclasses import replace
from datetime import datetime

import pytest

from messaging.config import SOURCE_TABLES, KafkaConfig
from pipeline.validation import ID_FIELDS
from shared.errors import ConfigurationError
from spark_pipeline.decoding import decode_message
from spark_pipeline.run import bind_state, state_lock, write_json_atomic

MINIMAL = {
    "abassurance": {
        "customers": {"AB_CLIENT_ID": 100, "AB_PRENOM": "Jean", "AB_NOM": "Dupont"},
        "contracts": {"AB_POLICY_NUMBER": "C1", "AB_CLIENT_ID": 100},
        "claims": {"AB_CLAIM_ID": 1, "AB_POLICY_NUMBER": "C1"},
        "payments": {"AB_PAYMENT_ID": 1, "AB_POLICY_NUMBER": "C1"},
    },
    "assureplus": {
        "customers": {"AP_USER_ID": 100, "AP_FULL_NAME": "Marie Martin"},
        "contracts": {"AP_CONTRACT_REF": "C1", "AP_USER_ID": 100, "AP_MONTHLY_PREMIUM": "99.99"},
        "claims": {"AP_SINISTRE_NUM": 1, "AP_CONTRACT_REF": "C1"},
        "payments": {"AP_PAYMENT_REF": "00000000-0000-0000-0000-000000002000", "AP_CONTRACT_REF": "C1"},
    },
}


def message(source="abassurance", entity="customers", record=None, *, prefix="insurance", offset=0):
    record = MINIMAL[source][entity] if record is None else record
    primary_key = SOURCE_TABLES[source][entity][1]
    return {
        "key": f"{source}:{entity}:{record.get(primary_key)}",
        "value": json.dumps({"source_system": source, "entity": entity, "data": record}),
        "topic": f"{prefix}.{source}.{entity}",
        "partition": 0,
        "offset": offset,
        "timestamp": None,
    }


def decode(row):
    return decode_message(row["value"], row["key"], row["topic"], "insurance")


@pytest.mark.parametrize("source", MINIMAL)
@pytest.mark.parametrize("entity", ID_FIELDS)
def test_spark_row_adapter_preserves_origins_and_uses_existing_mappers(source, entity):
    from pipeline.run import MAPPERS

    decoded = decode(message(source, entity))
    assert decoded["reason"] is None
    assert decoded["source_system"] == source
    assert decoded["entity"] == entity
    assert decoded["mapped_json"] is not None
    assert json.loads(decoded["mapped_json"]) == MAPPERS[entity](MINIMAL[source][entity], source)


@pytest.mark.parametrize("value", [None, "{", "[]", "null", '"hello"', '{"source_system":[]}', '{"x":NaN}'])
def test_spark_adapter_reports_bad_envelopes(value):
    result = decode({**message(), "value": value})
    assert result["reason"]
    assert result["mapped_json"] is None


@pytest.mark.parametrize(
    "change, reason",
    [
        ({"topic": "insurance.assureplus.customers"}, "topic"),
        ({"key": None}, "key"),
        ({"key": "abassurance:customers:999"}, "key"),
    ],
)
def test_spark_adapter_checks_transport_identity(change, reason):
    assert reason in decode({**message(), **change})["reason"]


@pytest.mark.parametrize(
    "entity, fields, reason",
    [
        ("customers", {"AB_CLIENT_ID": None}, "Missing source identifier"),
        ("customers", {"AB_DATE_NAISSANCE": "wrong"}, "Invalid isoformat"),
        ("payments", {"AB_MONTANT": "-1"}, "Invalid amount"),
        ("contracts", {"AB_DATE_DEBUT": "2025-12-31", "AB_DATE_FIN": "2025-01-01"}, "precedes"),
    ],
)
def test_spark_adapter_reuses_cleaning_validation(entity, fields, reason):
    assert reason in decode(message(entity=entity, record={**MINIMAL["abassurance"][entity], **fields}))["reason"]


def test_spark_adapter_preserves_decimal_precision_and_normalization():
    row = message("assureplus", "contracts")
    decoded = decode(row)
    assert decoded["mapped_json"] is not None
    assert json.loads(decoded["mapped_json"])["annual_premium"] == "1199.88"
    row["value"] = row["value"].replace('"99.99"', "12345678901234.56")
    decoded = decode(row)
    assert decoded["mapped_json"] is not None
    assert json.loads(decoded["mapped_json"])["annual_premium"] == "148148146814814.72"


@pytest.mark.parametrize(
    "source, entity, date_field, value",
    [
        ("abassurance", "customers", "AB_DATE_NAISSANCE", datetime(1990, 5, 12)),
        ("abassurance", "contracts", "AB_DATE_DEBUT", datetime(2025, 1, 1, 10, 30)),
        ("abassurance", "payments", "AB_DATE_PAIEMENT", datetime(2025, 1, 5, 12, 15)),
        ("assureplus", "contracts", "AP_START_DATE", datetime(2025, 1, 1)),
        ("assureplus", "payments", "AP_PAYMENT_DATETIME", datetime(2025, 1, 5, 10)),
    ],
)
def test_spark_restores_native_dates_before_reusing_mappers(source, entity, date_field, value):
    from pipeline.run import MAPPERS

    native = {**MINIMAL[source][entity], date_field: value}
    row = message(source, entity, {**native, date_field: value.isoformat()})
    decoded = decode(row)
    assert decoded["reason"] is None
    assert decoded["mapped_json"] is not None
    assert json.loads(decoded["mapped_json"]) == MAPPERS[entity](native, source)


def test_spark_state_is_bound_to_source_and_keeps_raw_with_checkpoint(tmp_path):
    config = KafkaConfig()
    bind_state(tmp_path, config)
    bind_state(tmp_path, config)
    with pytest.raises(ConfigurationError, match="source changed"):
        bind_state(tmp_path, replace(config, topic_prefix="other"))
    (tmp_path / "checkpoint").mkdir()
    with pytest.raises(ConfigurationError, match="together"):
        bind_state(tmp_path, config)


def test_spark_state_lock_is_released_and_excludes_parallel_writers(tmp_path):
    with state_lock(tmp_path), pytest.raises(ConfigurationError, match="already in use"), state_lock(tmp_path):
        pytest.fail("Concurrent writer acquired the state lock")
    with state_lock(tmp_path):
        pass


def test_spark_atomic_manifest_replaces_complete_json(tmp_path):
    path = tmp_path / "CURRENT.json"
    write_json_atomic(path, {"snapshot": "first"})
    write_json_atomic(path, {"snapshot": "second"})
    assert json.loads(path.read_text("utf-8")) == {"snapshot": "second"}
    assert list(tmp_path.iterdir()) == [path]
