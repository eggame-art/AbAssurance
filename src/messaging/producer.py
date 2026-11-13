import argparse
import json
import logging
from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from extraction.config import Settings, load_settings
from extraction.oracle import fetch_oracle
from extraction.sqlserver import fetch_sqlserver
from messaging.config import SOURCE_TABLES, KafkaConfig, load_kafka_config
from shared.errors import MessagingError, error_context, run_cli
from shared.types import Record

LOGGER = logging.getLogger(__name__)


def _json_value(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, (Decimal, UUID)):
        return str(value)

    raise TypeError(f"Unsupported JSON type: {type(value).__name__}")


def publish_records(
    producer: Producer, records: Iterable[Record], source: str, entity: str, config: KafkaConfig
) -> int:
    topic = config.topic(source, entity)
    primary_key = SOURCE_TABLES[source][entity][1]

    failures: list[KafkaError] = []
    count = 0

    def delivered(error: KafkaError | None, message: Message) -> None:
        if error is not None and not failures:
            failures.append(error)

    with error_context(
        f"Kafka publication failed for {topic}; delivery may be partial",
        (KafkaException, BufferError, KeyError, TypeError, ValueError),
        MessagingError,
    ):
        try:
            for record in records:
                value = json.dumps(
                    {"source_system": source, "entity": entity, "data": record},
                    default=_json_value,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")

                producer.produce(
                    topic, key=f"{source}:{entity}:{record[primary_key]}".encode(), value=value, on_delivery=delivered
                )

                producer.poll(0)
                count += 1

                if failures:
                    break

        finally:
            remaining = producer.flush(config.timeout)

    if failures or remaining:
        raise MessagingError(f"Kafka delivery failed or timed out for {topic}; delivery may be partial")

    return count


def publish_extracted(*, settings: Settings | None = None, config: KafkaConfig | None = None) -> int:
    settings = settings if settings is not None else load_settings()
    config = config if config is not None else load_kafka_config()

    with error_context(
        "Kafka connection failed; check broker configuration and readiness", (KafkaException,), MessagingError
    ):
        producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "message.timeout.ms": config.timeout * 1000,
            }
        )

        producer.list_topics(timeout=config.timeout)

    count = 0

    for source, tables in SOURCE_TABLES.items():
        for entity in tables:
            records = (
                fetch_oracle(entity, settings.oracle)
                if (source == "abassurance")
                else fetch_sqlserver(entity, settings.sqlserver)
            )

            sent = publish_records(producer, records, source, entity, config)

            LOGGER.info("%s: %d messages delivered", config.topic(source, entity), sent)

            count += sent

    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish raw Oracle and SQL Server records to Kafka.")
    parser.add_argument("--env-file", type=Path, help="Alternative environment file")

    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    def execute() -> None:
        publish_extracted(settings=load_settings(arguments.env_file), config=load_kafka_config(arguments.env_file))

    return run_cli(execute, LOGGER, "Kafka publisher failed")


if __name__ == "__main__":
    raise SystemExit(main())
