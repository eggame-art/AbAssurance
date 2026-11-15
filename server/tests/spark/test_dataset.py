import os
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_preparation.config import PreparationConfig
from abassurance.systems.data_preparation.run import prepare_dataset
from abassurance.systems.spark_pipeline.hdfs import HdfsState


def frames_from(spark, records):
    from abassurance.systems.spark_pipeline.schemas import entity_schema

    frames = {}

    for entity in ("customers", "contracts", "claims", "payments"):
        schema = entity_schema(entity)
        rows = [dict.fromkeys(schema.fieldNames()) | record for record in records.get(entity, [])]

        frames[entity] = spark.createDataFrame(rows, schema)

    return frames


@pytest.fixture
def records():
    return {
        "customers": [
            {"customer_id": "AB-1", "source_system": "abassurance", "source_id": "1"},
            {"customer_id": "AP-1", "source_system": "assureplus", "source_id": "1"},
        ],
        "contracts": [
            {
                "contract_id": "AB-C1",
                "source_system": "abassurance",
                "source_reference": "C1",
                "customer_id": "AB-1",
                "start_date": "2025-01-01",
                "contract_status": "RESILIE",
            },
            {
                "contract_id": "AB-C2",
                "source_system": "abassurance",
                "source_reference": "C2",
                "customer_id": "AB-1",
                "start_date": "2025-02-01",
            },
            {
                "contract_id": "AP-C1",
                "source_system": "assureplus",
                "source_reference": "C1",
                "customer_id": "AP-1",
                "start_date": "2025-01-01",
            },
        ],
        "claims": [
            {
                "claim_id": "AB-S1",
                "source_system": "abassurance",
                "source_id": "S1",
                "contract_id": "AB-C1",
                "incident_date": "2025-02-01T12:00:00",
            },
            {
                "claim_id": "AB-S2",
                "source_system": "abassurance",
                "source_id": "S2",
                "contract_id": "AB-C1",
                "incident_date": "2025-03-01T00:00:00",
            },
            {
                "claim_id": "AP-S1",
                "source_system": "assureplus",
                "source_id": "S1",
                "contract_id": "AP-C1",
                "incident_date": "2025-04-01T12:00:00",
            },
        ],
        "payments": [
            {
                "payment_id": "AB-P1",
                "source_system": "abassurance",
                "source_id": "P1",
                "contract_id": "AB-C1",
                "payment_date": "2025-02-01",
                "amount": "10.10",
            },
            {
                "payment_id": "AB-P2",
                "source_system": "abassurance",
                "source_id": "P2",
                "contract_id": "AB-C1",
                "payment_date": "2025-03-01",
                "amount": "20.40",
            },
            {
                "payment_id": "AB-P3",
                "source_system": "abassurance",
                "source_id": "P3",
                "contract_id": "AB-C1",
                "payment_date": "2025-04-01",
                "amount": None,
            },
            {
                "payment_id": "AP-P1",
                "source_system": "assureplus",
                "source_id": "P1",
                "contract_id": "AP-C1",
                "payment_date": "2025-05-01",
                "amount": None,
            },
        ],
    }


@pytest.mark.spark_integration
def test_aggregates_are_per_contract_and_cross_source_ids_stay_separate(spark, records):
    from abassurance.systems.data_preparation.features import PreparedDataset

    prepared = PreparedDataset(frames_from(spark, records), "2025-07-01")

    try:
        rows = {row.contract_id: row for row in prepared.dataset.collect()}

        assert len(rows) == 3

        assert rows["AB-C1"].claim_count == 2
        assert rows["AB-C1"].payment_count == 3
        assert rows["AB-C1"].payment_amount_known_total == Decimal("30.50")
        assert rows["AB-C1"].payment_amount_missing_count == 1
        assert rows["AB-C1"].contract_tenure_days == 181
        assert rows["AB-C1"].days_since_last_payment == 91

        assert rows["AP-C1"].claim_count == 1
        assert rows["AP-C1"].payment_amount_known_total is None

        assert rows["AB-C2"].payment_count == 0
        assert rows["AB-C2"].claim_count == 0
        assert rows["AB-C2"].payment_amount_known_total == Decimal("0.00")
        assert rows["AB-C2"].days_since_last_payment is None

        assert all(row.is_cancelled is None and not row.training_eligible for row in rows.values())

        assert "contract_status" not in prepared.dataset.columns
        assert "end_date" not in prepared.dataset.columns
        assert "annual_premium" not in prepared.dataset.columns

        assert prepared.report()["missing_features"]["payment_amount_known_total"] == 1

    finally:
        prepared.close()


@pytest.mark.spark_integration
def test_invalid_relations_duplicates_dates_and_missing_values_are_audited(spark, records):
    from abassurance.systems.data_preparation.features import PreparedDataset

    records["customers"].append(records["customers"][1])

    records["contracts"].extend(
        [
            {**records["contracts"][0], "contract_id": "AB-C3", "source_reference": "C3", "customer_id": "AP-1"},
            {**records["contracts"][0], "contract_id": "AB-C4", "source_reference": "C4", "start_date": None},
            {**records["contracts"][0], "contract_id": "AB-C5", "source_reference": "C5", "start_date": "2025-02-30"},
            {**records["contracts"][0], "contract_id": "AB-C6", "source_reference": "C6", "start_date": "2025-07-01"},
        ]
    )

    records["claims"].extend(
        [
            {**records["claims"][0], "claim_id": "AB-F1", "source_id": "F1", "incident_date": "2025-07-01T00:00:00"},
            {**records["claims"][0], "claim_id": "AB-F2", "source_id": "F2", "incident_date": None},
            {**records["claims"][0], "claim_id": "AB-F3", "source_id": "F3", "incident_date": "2024-12-31"},
            {**records["claims"][0], "claim_id": "AB-F4", "source_id": "F4", "contract_id": "AP-C1"},
        ]
    )

    records["payments"].extend(
        [
            records["payments"][0],
            {**records["payments"][1], "payment_id": "AB-X", "source_id": "X", "amount": "bad"},
            {**records["payments"][1], "payment_id": "AB-Y", "source_id": "Y", "amount": "-1.00"},
        ]
    )

    prepared = PreparedDataset(frames_from(spark, records), "2025-07-01")

    try:
        rows = {row.contract_id: row for row in prepared.dataset.collect()}

        assert set(rows) == {"AB-C1", "AB-C2"}
        assert rows["AB-C1"].claim_count == 2
        assert rows["AB-C1"].payment_count == 2
        assert rows["AB-C1"].payment_amount_known_total == Decimal("20.40")

        reasons = {(row.entity, row.record_id): row.reason for row in prepared.excluded.collect()}

        assert reasons["contracts", "AB-C4"] == "missing_or_invalid_start_date"
        assert reasons["contracts", "AB-C5"] == "missing_or_invalid_start_date"
        assert reasons["contracts", "AB-C6"] == "contract_not_started"

        assert reasons["claims", "AB-F1"] == "event_on_or_after_observation"
        assert reasons["claims", "AB-F2"] == "missing_or_invalid_event_date"
        assert reasons["claims", "AB-F3"] == "event_before_contract_start"
        assert reasons["claims", "AB-F4"] == "orphan_or_cross_source_parent"

        assert reasons["payments", "AB-P1"] == "duplicate_identifier"

        report = prepared.report()

        for entity, count in report["input"].items():
            excluded = sum(row["count"] for row in report["excluded"] if row["entity"] == entity)

            assert count == report["accepted"][entity] + excluded

    finally:
        prepared.close()


@pytest.mark.spark_integration
def test_empty_dataset_has_schema_and_zero_counts(spark):
    from abassurance.systems.data_preparation.features import PreparedDataset

    prepared = PreparedDataset(frames_from(spark, {}), "2025-07-01")

    try:
        report = prepared.report()

        assert report["records"] == 0
        assert report["unknown_targets"] == 0
        assert "is_cancelled" in prepared.dataset.columns

    finally:
        prepared.close()


@pytest.mark.spark_integration
def test_duplicate_contracts_are_all_excluded_without_arbitrary_selection(spark, records):
    from abassurance.systems.data_preparation.features import PreparedDataset

    records["contracts"].append({**records["contracts"][0], "start_date": "2025-02-01"})

    prepared = PreparedDataset(frames_from(spark, records), "2025-07-01")

    try:
        contract_ids = {row.contract_id for row in prepared.dataset.collect()}

        assert contract_ids == {"AB-C2", "AP-C1"}
        assert prepared.excluded.filter("entity = 'contracts' AND reason = 'duplicate_identifier'").count() == 2
        assert prepared.excluded.filter("entity = 'payments' AND reason = 'orphan_or_cross_source_parent'").count() == 3

    finally:
        prepared.close()


@pytest.mark.spark_integration
def test_future_events_and_current_status_changes_cannot_change_features(spark, records):
    from abassurance.systems.data_preparation.features import PreparedDataset

    baseline = PreparedDataset(frames_from(spark, records), "2025-07-01")

    try:
        expected = sorted(baseline.dataset.collect(), key=lambda row: row.contract_id)

    finally:
        baseline.close()

    for contract in records["contracts"]:
        contract.update(contract_status="CANCELLED", end_date="2025-08-01", annual_premium="99999.99")

    for customer in records["customers"]:
        customer.update(customer_status="INACTIVE", loyalty_score=0)

    records["claims"].append(
        {**records["claims"][0], "claim_id": "AB-FUTURE", "source_id": "FUTURE", "incident_date": "2025-08-01T12:00:00"}
    )

    records["payments"].append(
        {
            **records["payments"][0],
            "payment_id": "AB-FUTURE",
            "source_id": "FUTURE",
            "payment_date": "2025-08-01",
            "amount": "99999.99",
        }
    )

    updated = PreparedDataset(frames_from(spark, records), "2025-07-01")

    try:
        actual = sorted(updated.dataset.collect(), key=lambda row: row.contract_id)

        assert actual == expected

    finally:
        updated.close()


@pytest.fixture
def hdfs_preparation(spark, records):
    root = os.environ.get("HDFS_TEST_ROOT", "hdfs://namenode:9000/abassurance-tests")
    base = HdfsState(spark, f"{root}/{uuid4().hex}")

    config = PreparationConfig(base.path("source"), base.path("dataset"), "2025-07-01", "technical-validation")

    source = HdfsState(spark, config.source_root)
    identifier = uuid4().hex
    frames = frames_from(spark, records)
    counts = {}

    for entity, frame in frames.items():
        frame.write.parquet(source.path(f"snapshots/{identifier}/unified/{entity}"))
        counts[entity] = frame.count()

    spark.createDataFrame([], "reason string").write.parquet(source.path(f"snapshots/{identifier}/rejected"))

    manifest = {"snapshot": identifier, "format": "parquet", "counts": {"accepted": counts}}

    source.write_json_atomic(f"snapshots/{identifier}/report.json", manifest)
    source.write_json_atomic("CURRENT.json", manifest)

    try:
        yield config, source, manifest

    finally:
        base.fs.delete(base.java_path(""), True)


@pytest.mark.hdfs_integration
def test_hdfs_publication_repeatability_pinning_and_interrupted_write(spark, hdfs_preparation, monkeypatch):
    from abassurance.systems.data_preparation import snapshot
    from pyspark.sql.readwriter import DataFrameWriter

    config, source, manifest = hdfs_preparation
    report = prepare_dataset(spark, config)
    output = HdfsState(spark, config.output_root)

    assert report["counts"]["records"] == 3
    assert report["training_ready"] is False
    assert source.read_json("CURRENT.json") == manifest

    first = spark.read.parquet(report["data_path"])

    assert {row.source_snapshot for row in first.collect()} == {manifest["snapshot"]}
    assert output.read_json("CURRENT.json") == report
    assert prepare_dataset(spark, config) == report
    assert spark.read.parquet(report["data_path"]).exceptAll(first).count() == 0

    original_write = DataFrameWriter.parquet

    def fail(writer, path, *args, **kwargs):
        if path.endswith("/excluded"):
            raise OSError("Interrupted dataset write")

        return original_write(writer, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, "parquet", fail)

        with pytest.raises(OSError, match="Interrupted"):
            prepare_dataset(spark, replace(config, observation_date="2025-08-01"))

    assert output.read_json("CURRENT.json") == report
    assert not output.exists(".lock")

    recovered = prepare_dataset(spark, replace(config, observation_date="2025-08-01"))

    assert recovered["counts"]["records"] == 3
    assert recovered["dataset_id"] != report["dataset_id"]

    original_read = snapshot.read_snapshot

    def advance_current(*args, **kwargs):
        source.write_json_atomic("CURRENT.json", {"snapshot": "incomplete"})
        return original_read(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(snapshot, "read_snapshot", advance_current)

        pinned, frames = snapshot.load_current(spark, source)

        assert pinned == manifest
        assert frames["contracts"].count() == 3

    source.write_json_atomic("CURRENT.json", manifest)


@pytest.mark.hdfs_integration
def test_hdfs_invalid_source_is_not_replaced_with_a_guess(spark, hdfs_preparation):
    from abassurance.systems.data_preparation.snapshot import load_current

    config, source, manifest = hdfs_preparation

    source.write_json_atomic("CURRENT.json", {"snapshot": "../../elsewhere", "format": "parquet"})

    with pytest.raises(ConfigurationError, match="Invalid HDFS snapshot"):
        load_current(spark, source)

    source.write_json_atomic("CURRENT.json", {**manifest, "extra": True})

    with pytest.raises(ConfigurationError, match="does not match"):
        load_current(spark, source)

    wrong_counts = {**manifest, "counts": {"accepted": {**manifest["counts"]["accepted"], "contracts": 100}}}

    source.write_json_atomic("CURRENT.json", wrong_counts)
    source.write_json_atomic(f"snapshots/{manifest['snapshot']}/report.json", wrong_counts)

    with pytest.raises(ConfigurationError, match="counts do not match"):
        prepare_dataset(spark, config)

    source.write_json_atomic(f"snapshots/{manifest['snapshot']}/report.json", manifest)
    source.write_json_atomic("CURRENT.json", manifest)

    source.fs.delete(source.java_path(f"snapshots/{manifest['snapshot']}/unified/payments/_SUCCESS"), False)

    with pytest.raises(ConfigurationError, match="Incomplete source Parquet"):
        prepare_dataset(spark, config)


@pytest.mark.hdfs_integration
@pytest.mark.parametrize("failure_point", ["report.json", "CURRENT.json"])
def test_hdfs_metadata_failure_never_publishes_partial_data(spark, hdfs_preparation, monkeypatch, failure_point):
    from pyspark.sql.readwriter import DataFrameWriter

    config, source, manifest = hdfs_preparation
    original = HdfsState.write_json_atomic

    def fail(storage, path, value):
        if storage.root == config.output_root and path.endswith(failure_point):
            raise OSError("Interrupted metadata publication")

        return original(storage, path, value)

    with monkeypatch.context() as patch:
        patch.setattr(HdfsState, "write_json_atomic", fail)

        with pytest.raises(OSError, match="Interrupted metadata"):
            prepare_dataset(spark, config)

    output = HdfsState(spark, config.output_root)

    assert not output.exists("CURRENT.json")
    assert not output.exists(".lock")
    assert source.read_json("CURRENT.json") == manifest

    def forbid_recompute(*args, **kwargs):
        pytest.fail("A completed run must be reused after CURRENT publication failure")

    with monkeypatch.context() as patch:
        if failure_point == "CURRENT.json":
            patch.setattr(DataFrameWriter, "parquet", forbid_recompute)

        report = prepare_dataset(spark, config)

    assert output.read_json("CURRENT.json") == report
    assert report["counts"]["records"] == 3
