import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from confluent_kafka import KafkaError, KafkaException, Producer

from extraction.config import Settings
from messaging import consumer, producer
from messaging.config import SOURCE_TABLES, KafkaConfig, load_kafka_config
from shared.errors import ConfigurationError, ExtractionError, MessagingError, run_cli


@pytest.fixture
def kafka_producer(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    client = MagicMock(spec=Producer)
    client.flush.return_value = 0

    monkeypatch.setattr(producer, "Producer", MagicMock(return_value=client))

    return client


@pytest.fixture
def kafka_consumer(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    client = MagicMock()

    monkeypatch.setattr(consumer, "Consumer", MagicMock(return_value=client))

    return client


def test_kafka_configuration_reuses_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC_PREFIX", "KAFKA_GROUP_ID", "KAFKA_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)

    path = tmp_path / ".env"
    path.write_text("KAFKA_BOOTSTRAP_SERVERS=broker:9092\nKAFKA_TOPIC_PREFIX=raw\n", encoding="utf-8")
    monkeypatch.setenv("KAFKA_TIMEOUT", "7")

    config = load_kafka_config(path)

    assert config.bootstrap_servers == "broker:9092"
    assert config.timeout == 7
    assert config.topic("assureplus", "payments") == "raw.assureplus.payments"
    assert len(set(config.topics())) == 8
    assert len(max(config.topics(), key=len)) <= 249


@pytest.mark.parametrize(
    "values,name",
    [
        ({"bootstrap_servers": " "}, "KAFKA_BOOTSTRAP_SERVERS"),
        ({"bootstrap_servers": "broker:9092,"}, "KAFKA_BOOTSTRAP_SERVERS"),
        ({"topic_prefix": "bad/topic"}, "KAFKA_TOPIC_PREFIX"),
        ({"topic_prefix": "x" * 226}, "KAFKA_TOPIC_PREFIX"),
        ({"group_id": " "}, "KAFKA_GROUP_ID"),
        ({"timeout": 0}, "KAFKA_TIMEOUT"),
        ({"timeout": 301}, "KAFKA_TIMEOUT"),
    ],
)
def test_invalid_kafka_configuration(values: dict, name: str) -> None:
    with pytest.raises(ConfigurationError, match=name):
        KafkaConfig(**values)


@pytest.mark.parametrize("source,entity", [("unknown", "customers"), ("abassurance", "unknown")])
def test_invalid_routing_never_publishes(kafka_producer: MagicMock, source: str, entity: str) -> None:
    with pytest.raises(ValueError, match="Unknown source/entity"):
        producer.publish_records(kafka_producer, [], source, entity, KafkaConfig())

    kafka_producer.produce.assert_not_called()


def test_raw_message_keeps_origin_types_and_exact_values(kafka_producer: MagicMock) -> None:
    record = {
        "AB_CLIENT_ID": 100,
        "name": "Élodie",
        "amount": Decimal("12345678901234567890.1200"),
        "date": date(2025, 4, 15),
        "datetime": datetime(2025, 4, 15, 14, 30),
        "uuid": UUID("123e4567-e89b-12d3-a456-426614174000"),
        "optional": None,
    }

    assert producer.publish_records(kafka_producer, iter([record]), "abassurance", "customers", KafkaConfig()) == 1

    arguments, keywords = kafka_producer.produce.call_args
    message = json.loads(keywords["value"])

    assert arguments == ("insurance.abassurance.customers",)
    assert keywords["key"] == b"abassurance:customers:100"
    assert "Élodie".encode() in keywords["value"]

    assert message == {
        "source_system": "abassurance",
        "entity": "customers",
        "data": {
            "AB_CLIENT_ID": 100,
            "name": "Élodie",
            "amount": "12345678901234567890.1200",
            "date": "2025-04-15",
            "datetime": "2025-04-15T14:30:00",
            "uuid": "123e4567-e89b-12d3-a456-426614174000",
            "optional": None,
        },
    }

    assert record["amount"] == Decimal("12345678901234567890.1200")
    kafka_producer.flush.assert_called_once_with(30)


@pytest.mark.parametrize("value", [object(), float("nan"), float("inf")])
def test_unserializable_records_fail_without_silent_conversion(kafka_producer: MagicMock, value: object) -> None:
    with pytest.raises(MessagingError, match="publication failed"):
        producer.publish_records(
            kafka_producer, [{"AB_CLIENT_ID": 1, "value": value}], "abassurance", "customers", KafkaConfig()
        )

    kafka_producer.produce.assert_not_called()
    kafka_producer.flush.assert_called_once()


@pytest.mark.parametrize("failure", [BufferError("queue full"), KafkaException(KafkaError(KafkaError._TRANSPORT))])
def test_immediate_publication_failure_drains_pending_messages(kafka_producer: MagicMock, failure: Exception) -> None:
    kafka_producer.produce.side_effect = failure

    with pytest.raises(MessagingError, match="publication failed") as captured:
        producer.publish_records(kafka_producer, [{"AB_CLIENT_ID": 1}], "abassurance", "customers", KafkaConfig())

    assert captured.value.__cause__ is failure
    kafka_producer.flush.assert_called_once()


@pytest.mark.parametrize("failure", ["callback", "timeout"])
def test_delivery_failure_is_not_reported_as_success(kafka_producer: MagicMock, failure: str) -> None:
    def flush(timeout: int) -> int:
        if failure == "callback":
            callback = kafka_producer.produce.call_args.kwargs["on_delivery"]
            callback(KafkaError(KafkaError._MSG_TIMED_OUT), MagicMock())

            return 0

        return 1

    kafka_producer.flush.side_effect = flush

    with pytest.raises(MessagingError, match="delivery failed or timed out"):
        producer.publish_records(kafka_producer, [{"AB_CLIENT_ID": 1}], "abassurance", "customers", KafkaConfig())


def test_publisher_reuses_both_extractors_for_all_entities(
    kafka_producer: MagicMock, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    oracle = MagicMock(side_effect=lambda entity, config: [{SOURCE_TABLES["abassurance"][entity][1]: 1}])
    sqlserver = MagicMock(side_effect=lambda entity, config: [{SOURCE_TABLES["assureplus"][entity][1]: 1}])

    monkeypatch.setattr(producer, "fetch_oracle", oracle)
    monkeypatch.setattr(producer, "fetch_sqlserver", sqlserver)

    assert producer.publish_extracted(settings=settings, config=KafkaConfig()) == 8

    for source, extractor, config in (
        ("abassurance", oracle, settings.oracle),
        ("assureplus", sqlserver, settings.sqlserver),
    ):
        assert extractor.call_count == 4

        for entity in SOURCE_TABLES[source]:
            extractor.assert_any_call(entity, config)

    assert {call.args[0] for call in kafka_producer.produce.call_args_list} == set(KafkaConfig().topics())

    options = cast(MagicMock, producer.Producer).call_args.args[0]

    assert options["enable.idempotence"] is True
    assert options["acks"] == "all"
    assert not settings.output_dir.exists()


def test_connection_failure_prevents_extraction_and_hides_driver_details(
    kafka_producer: MagicMock, settings: Settings, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    kafka_producer.list_topics.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT, "sensitive-detail"))

    fetch = MagicMock()
    monkeypatch.setattr(producer, "fetch_oracle", fetch)

    status = run_cli(
        lambda: producer.publish_extracted(settings=settings, config=KafkaConfig()),
        producer.LOGGER,
        "Publication failed",
    )

    assert status == 1
    assert "Kafka connection failed" in caplog.text
    assert "sensitive-detail" not in caplog.text

    fetch.assert_not_called()
    kafka_producer.produce.assert_not_called()


def test_extraction_failure_propagates_without_publication(
    kafka_producer: MagicMock, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(producer, "fetch_oracle", MagicMock(side_effect=ExtractionError("Oracle unavailable")))

    with pytest.raises(ExtractionError, match="Oracle unavailable"):
        producer.publish_extracted(settings=settings, config=KafkaConfig())

    kafka_producer.produce.assert_not_called()


def test_empty_sources_publish_nothing(
    kafka_producer: MagicMock, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(producer, "fetch_oracle", lambda entity, config: [])
    monkeypatch.setattr(producer, "fetch_sqlserver", lambda entity, config: [])

    assert producer.publish_extracted(settings=settings, config=KafkaConfig()) == 0

    kafka_producer.produce.assert_not_called()


def test_consumer_prints_then_commits_and_closes(kafka_consumer: MagicMock, capsys: pytest.CaptureFixture[str]) -> None:
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b'{"source_system":"assureplus","entity":"customers","data":{"AP_USER_ID":1}}'
    message.topic.return_value = "insurance.assureplus.customers"

    kafka_consumer.poll.side_effect = [None, message]

    def commit(**kwargs: object) -> None:
        output = json.loads(capsys.readouterr().out)

        assert output["topic"] == "insurance.assureplus.customers"
        assert output["message"]["data"] == {"AP_USER_ID": 1}

    kafka_consumer.commit.side_effect = commit

    assert consumer.consume_messages(KafkaConfig(), max_messages=1) == 1

    kafka_consumer.subscribe.assert_called_once_with(KafkaConfig().topics())
    kafka_consumer.commit.assert_called_once_with(message=message, asynchronous=False)
    kafka_consumer.close.assert_called_once()

    options = cast(MagicMock, consumer.Consumer).call_args.args[0]

    assert options["enable.auto.commit"] is False
    assert options["auto.offset.reset"] == "earliest"


@pytest.mark.parametrize("failure", ["connection", "poll", "broker", "null", "json", "commit", "output", "interrupt"])
def test_consumer_failures_always_close_and_do_not_commit_early(
    kafka_consumer: MagicMock, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    error = KafkaException(KafkaError(KafkaError._TRANSPORT))

    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b"{}"
    message.topic.return_value = "insurance.abassurance.customers"

    kafka_consumer.poll.return_value = message
    expected: type[BaseException] = MessagingError

    if failure == "connection":
        kafka_consumer.list_topics.side_effect = error
    elif failure == "poll":
        kafka_consumer.poll.side_effect = error
    elif failure == "broker":
        message.error.return_value = KafkaError(KafkaError._UNKNOWN_TOPIC)
    elif failure == "null":
        message.value.return_value = None
    elif failure == "json":
        message.value.return_value = b"not-json"
    elif failure == "commit":
        kafka_consumer.commit.side_effect = error
    elif failure == "output":
        monkeypatch.setattr("builtins.print", MagicMock(side_effect=OSError("output failed")))
        expected = OSError
    else:
        kafka_consumer.poll.side_effect = KeyboardInterrupt
        expected = KeyboardInterrupt

    with pytest.raises(expected):
        consumer.consume_messages(KafkaConfig(), max_messages=1)

    kafka_consumer.close.assert_called_once()

    if failure != "commit":
        kafka_consumer.commit.assert_not_called()


def test_consumer_idle_timeout_is_bounded(kafka_consumer: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    ticks = iter([0, 0, 31])

    monkeypatch.setattr(consumer, "monotonic", lambda: next(ticks))
    kafka_consumer.poll.return_value = None

    assert consumer.consume_messages(KafkaConfig()) == 0

    kafka_consumer.commit.assert_not_called()
    kafka_consumer.close.assert_called_once()


def test_invalid_consumer_limit_does_not_connect(kafka_consumer: MagicMock) -> None:
    with pytest.raises(ConfigurationError, match="max_messages"):
        consumer.consume_messages(KafkaConfig(), max_messages=0)

    cast(MagicMock, consumer.Consumer).assert_not_called()
