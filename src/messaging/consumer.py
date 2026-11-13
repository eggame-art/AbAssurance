import argparse
import json
import logging
from pathlib import Path
from time import monotonic

from confluent_kafka import Consumer, KafkaException

from messaging.config import KafkaConfig, load_kafka_config
from shared.errors import ConfigurationError, MessagingError, error_context, run_cli

LOGGER = logging.getLogger(__name__)


def consume_messages(config: KafkaConfig | None = None, *, max_messages: int = 10) -> int:
    config = config if config is not None else load_kafka_config()

    if max_messages < 1:
        raise ConfigurationError("max_messages must be positive")

    with error_context(
        "Kafka consumer failed; check broker configuration and messages", (KafkaException,), MessagingError
    ):
        consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "socket.timeout.ms": config.timeout * 1000,
            }
        )

        try:
            consumer.list_topics(timeout=config.timeout)
            consumer.subscribe(config.topics())

            count = 0
            deadline = monotonic() + config.timeout

            while count < max_messages and (remaining := deadline - monotonic()) > 0:
                message = consumer.poll(min(1.0, remaining))

                if message is None:
                    continue

                if message.error() is not None:
                    raise KafkaException(message.error())

                payload = message.value()

                if payload is None:
                    raise MessagingError("Kafka message contains no JSON value")

                with error_context("Kafka message contains invalid JSON", (ValueError, TypeError), MessagingError):
                    value = json.loads(payload)

                print(json.dumps({"topic": message.topic(), "message": value}, ensure_ascii=False), flush=True)

                consumer.commit(message=message, asynchronous=False)

                count += 1
                deadline = monotonic() + config.timeout

            LOGGER.info("Received %d messages", count)

            return count

        finally:
            consumer.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Display Kafka messages and commit offsets after output.")
    parser.add_argument("--env-file", type=Path, help="Alternative environment file")
    parser.add_argument("--max-messages", type=int, default=10, help="Stop after this many messages or an idle timeout")

    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    def execute() -> None:
        consume_messages(load_kafka_config(arguments.env_file), max_messages=arguments.max_messages)

    try:
        return run_cli(execute, LOGGER, "Kafka consumer failed")
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
