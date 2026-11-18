import json
from copy import deepcopy
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_api import consolidated
from abassurance.systems.data_integration.mapping.identifiers import make_id
from abassurance.systems.data_integration.pipeline.run import MAPPERS
from abassurance.systems.integration_api.schemas import IntegrationRun
from abassurance.systems.spark_pipeline import hdfs
from abassurance.systems.spark_pipeline.schemas import RAW_SCHEMA, entity_schema
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

pytestmark = pytest.mark.spark_integration

ENTITIES = ["clients", "contracts", "claims", "payments"]

OWNERS = [(source, identifier) for source in ("abassurance", "assureplus") for identifier in ("100", "101")]


def native_frame(spark, rows, schema):
    def literal(value, kind):
        if isinstance(kind, StructType):
            fields = ",".join(f"'{field.name}',{literal(value[field.name], field.dataType)}" for field in kind)
            return f"named_struct({fields})"

        text = "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"

        return f"CAST({text} AS {kind.simpleString()})"

    query = " UNION ALL ".join(
        "SELECT " + ",".join(f"{literal(row.get(field.name), field.dataType)} AS `{field.name}`" for field in schema)
        for row in rows
    )

    return spark.sql(query)


@pytest.fixture
def dataset(spark, demo_sources, envelopes, monkeypatch):
    raw_rows = envelopes(demo_sources)
    frames = {}

    kafka_schema = StructType(
        [StructField("topic", StringType()), StructField("partition", IntegerType()), StructField("offset", LongType())]
    )

    for entity in MAPPERS:
        records = []

        for raw in raw_rows:
            envelope = json.loads(raw["value"])

            if envelope["entity"] != entity:
                continue

            source = envelope["source_system"]
            original = demo_sources[source][entity][raw["offset"]]
            mapped = MAPPERS[entity](original, source)

            records.append({**mapped, "_kafka": {key: raw[key] for key in ("topic", "partition", "offset")}})

        schema = StructType([*entity_schema(entity), StructField("_kafka", kafka_schema)])

        frames[entity] = native_frame(spark, records, schema)

    store = {"raw": native_frame(spark, raw_rows, RAW_SCHEMA), "manifest": {"snapshot": "b" * 32}}

    run = IntegrationRun(
        id="a" * 32, status="succeeded", snapshot="b" * 32, created_at=datetime.now(UTC), finished_at=datetime.now(UTC)
    )

    monkeypatch.setenv("SOURCE_DATA_KIND", "demo")
    monkeypatch.setattr(consolidated, "committed_run", lambda _: run)

    monkeypatch.setattr(
        hdfs,
        "HdfsState",
        lambda *_: SimpleNamespace(read_json=lambda _: store["manifest"], path=lambda _: "test-archive"),
    )

    monkeypatch.setattr(hdfs, "read_snapshot", lambda _spark, _root, entity, **_: frames[entity])

    reader = SimpleNamespace(schema=lambda _: SimpleNamespace(parquet=lambda _: store["raw"]))

    return SimpleNamespace(spark=SimpleNamespace(read=reader), raw_rows=raw_rows, store=store, frames=frames)


@pytest.mark.parametrize("entity", ENTITIES)
@pytest.mark.parametrize("owner", OWNERS)
def test_customer_cannot_read_another_customer_or_source(dataset, entity, owner):
    source, identifier = owner

    response = consolidated.get_consolidated(dataset.spark, entity, 0, 25, None, owner=owner)

    common = "customers" if entity == "clients" else entity

    assert response.total == 1
    assert len(response.records) == 1

    row = response.records[0]

    assert row.data["source_system"] == source

    if common == "customers":
        assert row.data["customer_id"] == make_id(source, identifier, "customers")

        source_field = "AB_CLIENT_ID" if source == "abassurance" else "AP_USER_ID"
        assert str(row.original[source_field]) == identifier

    elif common == "contracts":
        assert row.data["customer_id"] == make_id(source, identifier, "customers")

        source_field = "AB_CLIENT_ID" if source == "abassurance" else "AP_USER_ID"
        assert str(row.original[source_field]) == identifier

    else:
        reference = f"C-{identifier}"

        assert row.data["contract_id"] == make_id(source, reference, "contracts")

        source_field = "AB_POLICY_NUMBER" if source == "abassurance" else "AP_CONTRACT_REF"
        assert row.original[source_field] == reference

    assert "_kafka" not in response.columns
    assert "_kafka" not in row.data


@pytest.mark.parametrize("entity", ENTITIES)
def test_unknown_customer_has_no_records_or_total_from_other_clients(dataset, entity):
    response = consolidated.get_consolidated(dataset.spark, entity, 0, 25, None, owner=("abassurance", "999"))

    assert response.total == 0
    assert response.records == []


def test_pagination_cannot_escape_the_owner_filter(dataset):
    response = consolidated.get_consolidated(dataset.spark, "clients", 1, 1, None, owner=("abassurance", "100"))

    assert response.total == 1
    assert response.records == []


def test_admin_can_read_all_customers_and_filter_contracts(dataset):
    response = consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None)

    assert response.total == 4

    selected = make_id("assureplus", "100", "customers")

    response = consolidated.get_consolidated(dataset.spark, "contracts", 0, 25, None, customer_filter=selected)

    assert response.total == 1
    assert response.records[0].data["customer_id"] == selected


def test_wrong_customer_archive_is_rejected_instead_of_returning_private_data(spark, dataset):
    rows = deepcopy(dataset.raw_rows)

    for raw in rows:
        envelope = json.loads(raw["value"])

        if envelope["source_system"] != "abassurance" or envelope["entity"] != "customers":
            continue

        if envelope["data"]["AB_CLIENT_ID"] != 100:
            continue

        envelope["data"]["AB_CLIENT_ID"] = 101
        envelope["data"]["AB_MAIL"] = "other-customer-private@example.test"
        raw["value"] = json.dumps(envelope)

    dataset.store["raw"] = native_frame(spark, rows, RAW_SCHEMA)

    with pytest.raises(ValueError, match="Archived source"):
        consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None, owner=("abassurance", "100"))


def test_duplicate_archived_offsets_are_rejected(spark, dataset):
    duplicate = next(row for row in dataset.raw_rows if row["topic"].endswith("abassurance.customers"))

    dataset.store["raw"] = native_frame(spark, dataset.raw_rows + [duplicate], RAW_SCHEMA)

    with pytest.raises(ValueError, match="association is not unique"):
        consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None, owner=("abassurance", "100"))


def test_snapshot_mismatch_is_rejected(dataset):
    dataset.store["manifest"] = {"snapshot": "c" * 32}

    with pytest.raises(ValueError, match="Snapshot identity differs"):
        consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None)


def test_invalid_data_classification_is_rejected(dataset, monkeypatch):
    monkeypatch.setenv("SOURCE_DATA_KIND", "unexpected")

    with pytest.raises(ConfigurationError):
        consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None)


def test_unspecified_data_classification_is_preserved(dataset, monkeypatch):
    monkeypatch.setenv("SOURCE_DATA_KIND", "unspecified")

    response = consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None, owner=("abassurance", "100"))

    assert response.data_kind == "unspecified"


def test_customer_filter_cannot_be_used_on_other_entities(dataset):
    with pytest.raises(ValueError, match="only supported for contracts"):
        consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None, customer_filter="another-customer")


def test_committed_run_without_snapshot_is_rejected(dataset, monkeypatch):
    monkeypatch.setattr(consolidated, "committed_run", lambda _: SimpleNamespace(snapshot=None))

    with pytest.raises(ValueError, match="has no snapshot"):
        consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None)


def test_large_archive_identifiers_and_decimal_values_remain_exact(spark, dataset):
    rows = deepcopy(dataset.raw_rows)

    for row in rows:
        value = json.loads(row["value"])

        if value["source_system"] != "abassurance" or value["entity"] != "customers":
            continue

        value["data"]["ARCHIVED_LARGE_ID"] = 2**60 + 1
        value["data"]["ARCHIVED_AMOUNT"] = 12.25
        row["value"] = json.dumps(value)

    dataset.store["raw"] = native_frame(spark, rows, RAW_SCHEMA)

    response = consolidated.get_consolidated(dataset.spark, "clients", 0, 25, None, owner=("abassurance", "100"))

    assert response.records[0].original["ARCHIVED_LARGE_ID"] == str(2**60 + 1)
    assert response.records[0].original["ARCHIVED_AMOUNT"] == "12.25"


@pytest.mark.parametrize("owner", OWNERS)
def test_prediction_only_calculates_features_for_the_owners_contract(
    spark, dataset, monkeypatch, owner: tuple[str, str]
):
    from unittest.mock import Mock

    from abassurance.systems.prediction_api import contracts
    from abassurance.systems.prediction_api.service import PredictionService
    from fastapi import HTTPException

    monkeypatch.setattr(contracts, "committed_run", lambda _: SimpleNamespace(id="a" * 32, snapshot="b" * 32))
    monkeypatch.setattr(
        contracts, "HdfsState", lambda *_: SimpleNamespace(read_json=lambda _: dataset.store["manifest"])
    )
    monkeypatch.setattr(contracts, "read_snapshot", lambda _spark, _root, entity, **_: dataset.frames[entity])
    prediction = Mock(
        return_value={
            "cancellation_probability": 0.2,
            "predicted_class": 0,
            "model_id": "c" * 32,
            "status": "experimental",
            "business_validated": False,
        }
    )
    service = cast(
        PredictionService,
        Mock(spec=PredictionService, info=SimpleNamespace(feature_columns=["claim_count"]), predict=prediction),
    )
    source, identifier = owner
    customer_id = make_id(source, identifier, "customers")

    def prepare(frames, observation):
        for entity in ("customers", "contracts", "claims", "payments"):
            rows = frames[entity].collect()
            assert len(rows) == 1
            assert rows[0].source_system == owner[0]
            assert getattr(rows[0], "customer_id", customer_id) == customer_id
            if entity != "customers":
                assert rows[0].contract_id == make_id(owner[0], f"C-{owner[1]}", "contracts")
        return SimpleNamespace(
            dataset=SimpleNamespace(first=lambda: SimpleNamespace(asDict=lambda: {"claim_count": 1})),
            close=lambda: None,
        )

    prepared = Mock(side_effect=prepare)
    monkeypatch.setattr(contracts, "PreparedDataset", prepared)
    for other in OWNERS:
        body = contracts.ContractInput(
            customer_id=UUID(customer_id),
            contract_id=UUID(make_id(other[0], f"C-{other[1]}", "contracts")),
            integration_id="a" * 32,
            observation_date=date(2025, 7, 1),
        )
        if other == owner:
            result = contracts.predict_contract(spark, body, service, owner=owner)
            assert result.customer_id == body.customer_id
        else:
            with pytest.raises(HTTPException) as error:
                contracts.predict_contract(spark, body, service, owner=owner)
            assert error.value.status_code == 404
    assert prepared.call_count == 1
    assert prediction.call_count == 1
