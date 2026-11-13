import json
from collections import Counter
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest

from extraction.config import load_settings
from extraction.oracle import fetch_oracle
from extraction.sqlserver import fetch_sqlserver
from messaging.config import SOURCE_TABLES, load_kafka_config
from messaging.consumer import consume_messages
from messaging.producer import publish_extracted

pytestmark = pytest.mark.kafka_integration


def test_live_extraction_publication_and_reception(capsys: pytest.CaptureFixture[str]) -> None:
    settings = load_settings()
    config = replace(load_kafka_config(), topic_prefix=f"test-{uuid4().hex}", group_id=f"test-{uuid4().hex}")
    records = {
        source: {
            entity: fetch_oracle(entity, settings.oracle)
            if source == "abassurance"
            else fetch_sqlserver(entity, settings.sqlserver)
            for entity in tables
        }
        for source, tables in SOURCE_TABLES.items()
    }
    expected = Counter(
        {(source, entity): len(rows) for source, entities in records.items() for entity, rows in entities.items()}
    )

    assert all(count > 0 for count in expected.values())
    count = sum(expected.values())

    assert publish_extracted(settings=settings, config=config) == count
    assert consume_messages(config, max_messages=count) == count
    received = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    assert Counter((row["message"]["source_system"], row["message"]["entity"]) for row in received) == expected

    for row in received:
        message = row["message"]
        source, entity = message["source_system"], message["entity"]

        assert row["topic"] == config.topic(source, entity)

        primary_key = SOURCE_TABLES[source][entity][1]
        original = next(
            record
            for record in records[source][entity]
            if str(record[primary_key]) == str(message["data"][primary_key])
        )

        assert set(message["data"]) == set(original)

        for name, value in original.items():
            if isinstance(value, Decimal):
                assert message["data"][name] == str(value)
