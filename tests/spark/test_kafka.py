import json
from dataclasses import replace
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

pytestmark = pytest.mark.spark_kafka_integration


def read_json_parts(path):
    return [json.loads(line) for file in path.glob("part-*.json") for line in file.read_text("utf-8").splitlines()]


def test_real_databases_kafka_spark_republications_and_resume(spark, tmp_path):
    from spark_pipeline.run import run_available

    settings = load_settings()
    config = replace(load_kafka_config(), topic_prefix=f"spark-test-{uuid4().hex}")

    sources = {
        source: {
            entity: fetch_oracle(entity, settings.oracle)
            if source == "abassurance"
            else fetch_sqlserver(entity, settings.sqlserver)
            for entity in tables
        }
        for source, tables in SOURCE_TABLES.items()
    }

    assert sum(len(rows) for entities in sources.values() for rows in entities.values()) == 16

    expected, rejected = clean_datasets(sources, MAPPERS)

    assert rejected == []

    for _ in range(3):
        assert publish_extracted(settings=settings, config=config) == 16

    state = tmp_path / "state"

    first = run_available(spark, config, state, max_offsets=8)

    assert first["counts"] == {
        "input": 48,
        "accepted": dict.fromkeys(MAPPERS, 4),
        "rejected": 32,
        "duplicates": 32,
        "invalid": 0,
    }

    folder = state / "snapshots" / first["snapshot"]
    actual = {}

    for entity in MAPPERS:
        rows = read_json_parts(folder / entity)

        assert len(rows) == 4
        assert all(row.pop("_kafka")["topic"].startswith(config.topic_prefix) for row in rows)

        key = ID_FIELDS[entity]

        assert sorted(rows, key=lambda row: row[key]) == sorted(expected[entity], key=lambda row: row[key])

        actual[entity] = rows

    validate_datasets(actual)

    assert len(read_json_parts(folder / "rejected")) == 32

    assert run_available(spark, config, state, max_offsets=8)["counts"] == first["counts"]

    assert publish_extracted(settings=settings, config=config) == 16

    last = run_available(spark, config, state, max_offsets=8)

    assert last["counts"]["input"] == 64
    assert last["counts"]["duplicates"] == 48
    assert last["counts"]["accepted"] == first["counts"]["accepted"]
