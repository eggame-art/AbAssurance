import json
from collections import Counter
from copy import deepcopy

import pytest

from cleaning import clean_datasets
from pipeline.run import MAPPERS
from pipeline.validation import ID_FIELDS, validate_datasets

pytestmark = pytest.mark.spark_integration


def records_without_provenance(frame):
    return [row.asDict(recursive=True) for row in frame.drop("_kafka").collect()]


def test_distributed_mapping_16_records_republications_and_exact_values(spark, demo_sources, envelopes):
    from spark_pipeline.schemas import RAW_SCHEMA
    from spark_pipeline.transform import Snapshot

    raw = spark.createDataFrame(envelopes(demo_sources, copies=3), RAW_SCHEMA).repartition(4)
    snapshot = Snapshot(raw, "insurance")

    try:
        assert snapshot.counts() == {
            "input": 48,
            "accepted": dict.fromkeys(MAPPERS, 4),
            "rejected": 32,
            "duplicates": 32,
            "invalid": 0,
        }

        expected, rejected = clean_datasets(demo_sources, MAPPERS)

        assert rejected == []

        actual = {entity: records_without_provenance(frame) for entity, frame in snapshot.datasets.items()}

        validate_datasets(actual)

        for entity, rows in actual.items():
            key = ID_FIELDS[entity]

            assert sorted(rows, key=lambda row: row[key]) == sorted(expected[entity], key=lambda row: row[key])

            assert Counter(row["source_system"] for row in rows) == {"abassurance": 2, "assureplus": 2}

            assert len({row[key] for row in rows}) == 4
            assert {row._kafka.offset for row in snapshot.datasets[entity].collect()} == {0, 1}

        assert {row["annual_premium"] for row in actual["contracts"]} == {"1199.88"}

        oracle_customers = [row for row in actual["customers"] if row["source_system"] == "abassurance"]

        sqlserver_claims = [row for row in actual["claims"] if row["source_system"] == "assureplus"]

        assert {row["birth_date"] for row in oracle_customers} == {"1990-05-12"}
        assert {row["incident_date"] for row in sqlserver_claims} == {"2025-03-10T14:30:00"}

        assert all(row.value and row.topic and row.reason for row in snapshot.rejected.collect())

        accepted_events = {
            (row._kafka.topic, row._kafka.partition, row._kafka.offset)
            for frame in snapshot.datasets.values()
            for row in frame.collect()
        }

        rejected_events = {(row.topic, row.partition, row.offset) for row in snapshot.rejected.collect()}

        input_events = {(row["topic"], row["partition"], row["offset"]) for row in envelopes(demo_sources, copies=3)}

        assert accepted_events.isdisjoint(rejected_events)
        assert accepted_events | rejected_events == input_events

    finally:
        snapshot.close()


def test_invalid_rows_cascade_and_corrections_are_not_hidden_by_duplicates(spark, demo_sources, envelopes):
    from spark_pipeline.schemas import RAW_SCHEMA
    from spark_pipeline.transform import Snapshot

    bad = deepcopy(demo_sources)

    bad["abassurance"]["customers"][0]["AB_DATE_NAISSANCE"] = "bad-date"
    bad["abassurance"]["contracts"][0]["AB_CLIENT_ID"] = 999
    bad["abassurance"]["contracts"][1].update(AB_DATE_DEBUT="2026-01-01", AB_DATE_FIN="2025-01-01")

    bad["assureplus"]["payments"][0]["AP_AMOUNT_PAID"] = "-1"
    bad["assureplus"]["contracts"][0]["AP_USER_ID"] = 999

    bad_rows = envelopes(bad)

    corrections = {source: {entity: [] for entity in MAPPERS} for source in demo_sources}

    for entity in ("customers", "contracts"):
        corrections["abassurance"][entity] = [demo_sources["abassurance"][entity][0]]

    good_rows = [{**row, "offset": 200} for row in envelopes(corrections)]

    malformed = {**bad_rows[0], "offset": 300, "value": "{not-json"}

    raw = spark.createDataFrame(bad_rows + good_rows + [malformed], RAW_SCHEMA)
    snapshot = Snapshot(raw, "insurance")

    try:
        counts = snapshot.counts()

        assert counts["input"] == 19
        assert counts["input"] == sum(counts["accepted"].values()) + counts["rejected"]

        assert counts["accepted"] == {"customers": 4, "contracts": 2, "claims": 2, "payments": 2}

        actual = {entity: records_without_provenance(frame) for entity, frame in snapshot.datasets.items()}

        validate_datasets(actual)

        reasons = [row.reason for row in snapshot.rejected.collect()]

        assert any("Invalid isoformat" in reason for reason in reasons)
        assert any("precedes" in reason for reason in reasons)
        assert any("Invalid amount" in reason for reason in reasons)
        assert any("Orphan customer_id" in reason for reason in reasons)
        assert any("Orphan contract_id" in reason for reason in reasons)

        assert {row["contract_id"] for row in actual["contracts"]} == {"AB-C-100", "AP-C-101"}

    finally:
        snapshot.close()


def test_finite_stream_restart_and_failed_export(spark, tmp_path, demo_sources, envelopes, monkeypatch):
    from spark_pipeline import run
    from spark_pipeline.schemas import RAW_SCHEMA

    incoming = tmp_path / "incoming"
    incoming.mkdir()

    state = tmp_path / "state"

    def add_file(name, rows):
        content = "\n".join(json.dumps(row, default=str) for row in rows) + "\n"

        (incoming / name).write_text(content, encoding="utf-8")

    rows = envelopes(demo_sources)

    add_file("children.json", [row for row in rows if row["topic"].endswith(("claims", "payments"))])

    add_file("parents.json", [row for row in rows if row["topic"].endswith(("customers", "contracts"))])

    def ingest():
        stream = spark.readStream.schema(RAW_SCHEMA).option("maxFilesPerTrigger", 1).json(str(incoming))

        run.ingest_available(stream, state)

    ingest()

    first = run.materialize(spark, state, "insurance")

    assert first["counts"]["input"] == 16
    assert first["counts"]["rejected"] == 0
    assert first["counts"]["accepted"] == dict.fromkeys(MAPPERS, 4)

    ingest()

    assert run.materialize(spark, state, "insurance")["counts"] == first["counts"]

    add_file("republication.json", envelopes(demo_sources, copies=2)[16:])

    ingest()

    replayed = run.materialize(spark, state, "insurance")

    assert replayed["counts"]["input"] == 32
    assert replayed["counts"]["duplicates"] == 16
    assert replayed["counts"]["accepted"] == first["counts"]["accepted"]

    current = (state / "CURRENT.json").read_bytes()
    original = run.write_json_atomic

    def fail_report(path, value):
        if path.name == "report.json":
            raise OSError("Simulated interrupted export")

        original(path, value)

    monkeypatch.setattr(run, "write_json_atomic", fail_report)

    with pytest.raises(OSError, match="interrupted"):
        run.materialize(spark, state, "insurance")

    assert (state / "CURRENT.json").read_bytes() == current


def test_empty_stream_produces_inspectable_zero_counts(spark, tmp_path):
    from spark_pipeline.run import ingest_available, materialize
    from spark_pipeline.schemas import RAW_SCHEMA

    incoming = tmp_path / "incoming"
    incoming.mkdir()

    state = tmp_path / "state"

    stream = spark.readStream.schema(RAW_SCHEMA).json(str(incoming))

    ingest_available(stream, state)

    report = materialize(spark, state, "insurance")

    assert report["counts"] == {
        "input": 0,
        "accepted": dict.fromkeys(MAPPERS, 0),
        "rejected": 0,
        "duplicates": 0,
        "invalid": 0,
    }
