import os
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest

from cleaning import clean_datasets
from extraction.config import load_settings
from extraction.oracle import fetch_oracle
from extraction.sqlserver import fetch_sqlserver
from messaging.config import SOURCE_TABLES, load_kafka_config
from messaging.producer import publish_extracted
from pipeline.run import MAPPERS
from pipeline.validation import ID_FIELDS, validate_datasets
from spark_pipeline.hdfs import HdfsState, read_snapshot
from spark_pipeline.run import run_available

pytestmark = [pytest.mark.hdfs_integration, pytest.mark.spark_kafka_integration]


def test_real_databases_kafka_spark_hdfs_exact_records_and_resume(spark: Any) -> None:
    settings = load_settings()
    config = replace(load_kafka_config(), topic_prefix=f"hdfs-test-{uuid4().hex}")
    root = os.environ.get("HDFS_TEST_ROOT", "hdfs://namenode:9000/abassurance-tests")
    state = HdfsState(spark, f"{root}/{uuid4().hex}")

    sources = {
        source: {
            entity: fetch_oracle(entity, settings.oracle)
            if (source == "abassurance")
            else fetch_sqlserver(entity, settings.sqlserver)
            for entity in entities
        }
        for source, entities in SOURCE_TABLES.items()
    }
    expected, rejected = clean_datasets(sources, MAPPERS)

    assert rejected == []
    assert sum(len(rows) for entities in sources.values() for rows in entities.values()) == 16

    try:
        for _ in range(3):
            assert publish_extracted(settings=settings, config=config) == 16

        first = run_available(spark, config, state.root, max_offsets=8)

        assert first["counts"] == {
            "input": 48,
            "accepted": dict.fromkeys(MAPPERS, 4),
            "rejected": 32,
            "duplicates": 32,
            "invalid": 0,
        }

        actual = {}

        for entity in MAPPERS:
            frame = read_snapshot(spark, state.root, entity, manifest=first)
            rows = [row.asDict(recursive=True) for row in frame.drop("_kafka").collect()]
            key = ID_FIELDS[entity]

            assert len(rows) == len({row[key] for row in rows}) == 4
            assert sorted(rows, key=lambda row: row[key]) == sorted(expected[entity], key=lambda row: row[key])

            actual[entity] = rows

        validate_datasets(actual)

        assert read_snapshot(spark, state.root, "rejected", manifest=first).count() == 32
        assert run_available(spark, config, state.root, max_offsets=8)["counts"] == first["counts"]
        assert publish_extracted(settings=settings, config=config) == 16

        last = run_available(spark, config, state.root, max_offsets=8)

        assert last["counts"]["input"] == 64
        assert last["counts"]["duplicates"] == 48
        assert last["counts"]["accepted"] == first["counts"]["accepted"]

    finally:
        state.fs.delete(state.java_path(""), True)
