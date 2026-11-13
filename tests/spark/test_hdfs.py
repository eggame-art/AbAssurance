import json
import os
from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import pytest

from cleaning import clean_datasets
from messaging.config import KafkaConfig
from pipeline.run import MAPPERS
from pipeline.validation import ID_FIELDS, validate_datasets
from shared.errors import ConfigurationError
from spark_pipeline.hdfs import HdfsState, materialize_hdfs, read_snapshot

pytestmark = pytest.mark.hdfs_integration


@pytest.fixture
def hdfs_state(spark: Any) -> Iterator[HdfsState]:
    root = os.environ.get("HDFS_TEST_ROOT", "hdfs://namenode:9000/abassurance-tests")
    state = HdfsState(spark, f"{root}/{uuid4().hex}")

    yield state

    state.fs.delete(state.java_path(""), True)


def assert_demo_snapshot(spark: Any, state: HdfsState, sources: dict, manifest: dict) -> None:
    from spark_pipeline.schemas import entity_schema

    expected, rejected = clean_datasets(sources, MAPPERS)

    assert rejected == []
    assert manifest["counts"]["accepted"] == dict.fromkeys(MAPPERS, 4)
    assert state.read_json(f"snapshots/{manifest['snapshot']}/report.json") == manifest

    actual = {}

    for entity in MAPPERS:
        frame = read_snapshot(spark, state.root, entity, manifest=manifest)
        rows = [row.asDict(recursive=True) for row in frame.drop("_kafka").collect()]
        key = ID_FIELDS[entity]

        assert len(rows) == len({row[key] for row in rows}) == 4
        assert sorted(rows, key=lambda row: row[key]) == sorted(expected[entity], key=lambda row: row[key])
        assert [(field.name, field.dataType) for field in frame.drop("_kafka").schema] == [
            (field.name, field.dataType) for field in entity_schema(entity)
        ]

        actual[entity] = rows

    validate_datasets(actual)


def test_hdfs_connection_atomic_manifest_and_exclusive_lock(spark: Any, hdfs_state: HdfsState) -> None:
    state = hdfs_state

    with state.lock():
        state.bind(KafkaConfig())
        state.write_json_atomic("probe.json", {"value": "first"})
        state.write_json_atomic("probe.json", {"value": "second"})

        assert state.read_json("probe.json") == {"value": "seconde"}

        with pytest.raises(ConfigurationError, match="already in use"), HdfsState(spark, state.root).lock():
            pytest.fail("Concurrent HDFS writer acquired the lock")

    assert not state.exists(".lock")


def test_hdfs_stream_parquet_replay_rejections_and_failed_publication(
    spark: Any, hdfs_state: HdfsState, tmp_path: Any, demo_sources: dict, envelopes: Any, monkeypatch: Any
) -> None:
    from pyspark.sql.readwriter import DataFrameWriter

    from spark_pipeline.run import ingest_available
    from spark_pipeline.schemas import RAW_SCHEMA

    state = hdfs_state
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    rows = envelopes(demo_sources)
    malformed = {**rows[0], "offset": 500, "value": "{bad-json"}
    (incoming / "first.json").write_text(
        "\n".join(json.dumps(row, default=str) for row in rows + [malformed]), encoding="utf-8"
    )

    def execute() -> dict:
        with state.lock():
            state.bind(KafkaConfig())
            stream = spark.readStream.schema(RAW_SCHEMA).json(incoming.as_uri())
            ingest_available(stream, state.root)

            return materialize_hdfs(spark, state, "insurance")

    first = execute()

    assert first["counts"]["input"] == 17
    assert first["counts"]["invalid"] == 1
    assert_demo_snapshot(spark, state, demo_sources, first)

    (incoming / "republication.json").write_text(
        "\n".join(json.dumps(row, default=str) for row in envelopes(demo_sources, copies=2)[16:]), encoding="utf-8"
    )

    replayed = execute()

    assert replayed["counts"]["input"] == 33
    assert replayed["counts"]["duplicates"] == 16
    assert replayed["counts"]["invalid"] == 1
    assert_demo_snapshot(spark, state, demo_sources, replayed)

    rejections = read_snapshot(spark, state.root, "rejected").collect()

    assert len(rejections) == 17
    assert all(row.reason and row.value and row.topic for row in rejections)

    assert execute()["counts"] == replayed["counts"]

    original_write = DataFrameWriter.parquet
    original_report = HdfsState.write_json_atomic

    def fail_data(writer: Any, path: str, *args: Any, **kwargs: Any) -> None:
        if path.endswith("/unified/payments"):
            raise OSError("Interrupted Parquet write")

        original_write(writer, path, *args, **kwargs)

    def fail_report(storage: HdfsState, path: str, value: dict) -> None:
        if path.endswith("report.json"):
            raise OSError("Interrupted report write")

        original_report(storage, path, value)

    current = state.read_json("CURRENT.json")

    (incoming / "pending.json").write_text(
        "\n".join(json.dumps(row, default=str) for row in envelopes(demo_sources, copies=3)[32:]), encoding="utf-8"
    )

    for target, attribute, failure in (
        (DataFrameWriter, "parquet", fail_data),
        (HdfsState, "write_json_atomic", fail_report),
    ):
        with monkeypatch.context() as patch:
            patch.setattr(target, attribute, failure)

            with pytest.raises(OSError, match="Interrupted"):
                execute()

        assert state.read_json("CURRENT.json") == current
        assert not state.exists(".lock")
        assert_demo_snapshot(spark, state, demo_sources, current)

    recovered = execute()

    assert recovered["counts"] == {**replayed["counts"], "input": 49, "duplicates": 32, "rejected": 33}


def test_hdfs_empty_input_remains_readable(spark: Any, hdfs_state: HdfsState, tmp_path: Any) -> None:
    from spark_pipeline.run import ingest_available
    from spark_pipeline.schemas import RAW_SCHEMA

    with hdfs_state.lock():
        hdfs_state.bind(KafkaConfig())
        stream = spark.readStream.schema(RAW_SCHEMA).json(tmp_path.as_uri())
        ingest_available(stream, hdfs_state.root)
        manifest = materialize_hdfs(spark, hdfs_state, "insurance")

    assert manifest["counts"]["input"] == 0

    for entity in (*MAPPERS, "rejected"):
        assert read_snapshot(spark, hdfs_state.root, entity).count() == 0


def test_hdfs_persistence_across_container_restart(spark: Any, demo_sources: dict, envelopes: Any) -> None:
    from spark_pipeline.run import ingest_available
    from spark_pipeline.schemas import RAW_SCHEMA

    root = os.environ.get("HDFS_PERSISTENCE_ROOT")
    phase = os.environ.get("HDFS_PERSISTENCE_PHASE")

    if not root or phase not in ("write", "read"):
        pytest.skip("Use scripts/test_hdfs_persistence.ps1 to verify a real Docker restart")

    state = HdfsState(spark, root)

    if phase == "write":
        with state.lock():
            state.bind(KafkaConfig())
            spark.createDataFrame(envelopes(demo_sources), RAW_SCHEMA).write.mode("errorifexists").json(
                state.path("incoming")
            )
            stream = spark.readStream.schema(RAW_SCHEMA).json(state.path("incoming"))
            ingest_available(stream, state.root)
            materialize_hdfs(spark, state, "insurance")

    manifest = state.read_json("CURRENT.json")

    assert manifest["counts"]["input"] == 16
    assert_demo_snapshot(spark, state, demo_sources, manifest)

    if phase == "read":
        with state.lock():
            state.bind(KafkaConfig())
            stream = spark.readStream.schema(RAW_SCHEMA).json(state.path("incoming"))
            ingest_available(stream, state.root)
            resumed = materialize_hdfs(spark, state, "insurance")

        assert resumed["counts"] == manifest["counts"]
