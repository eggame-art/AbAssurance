import argparse
import json
import time
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from abassurance.systems.data_integration.extraction.config import load_settings
from abassurance.systems.data_integration.extraction.oracle import fetch_oracle
from abassurance.systems.data_integration.extraction.sqlserver import fetch_sqlserver
from abassurance.systems.data_integration.pipeline.run import MAPPERS
from abassurance.systems.data_integration.pipeline.validation import ID_FIELDS
from abassurance.systems.messaging.config import SOURCE_TABLES, load_kafka_config
from abassurance.systems.spark_pipeline.decoding import decode_message
from confluent_kafka import Consumer

KAFKA_TIMEOUT = 60


def source_rows(settings) -> dict:
    rows = {}

    for source, tables in SOURCE_TABLES.items():
        fetch = fetch_oracle if source == "abassurance" else fetch_sqlserver
        connection = settings.oracle if source == "abassurance" else settings.sqlserver

        for entity, (_, primary_key) in tables.items():
            for record in fetch(entity, connection):
                rows[f"{source}:{entity}:{record[primary_key]}"] = (source, entity, record)

    return rows


def verify_record(actual: dict, expected: dict) -> None:
    if set(actual) != set(expected):
        raise AssertionError("Source columns differ")

    for name, value in expected.items():
        received = actual[name]

        if isinstance(value, Decimal):
            valid = isinstance(received, str) and Decimal(received) == value
        elif isinstance(value, datetime):
            valid = isinstance(received, str) and datetime.fromisoformat(received) == value
        elif isinstance(value, date):
            valid = isinstance(received, str) and datetime.fromisoformat(received).date() == value
        elif isinstance(value, UUID):
            valid = isinstance(received, str) and received == str(value)
        else:
            valid = type(received) is type(value) and received == value

        if not valid:
            raise AssertionError(f"Source value or type differs: {name}")


def verify_message(message, config, expected: dict, seen: set) -> tuple[str, str]:
    if message.error():
        raise RuntimeError("Kafka consumer failed")

    raw_key = message.key()
    raw_value = message.value()
    topic = message.topic()

    if raw_key is None or raw_value is None or topic is None:
        raise AssertionError("Kafka message is missing its key, value, or topic")

    key = raw_key.decode("utf-8")

    if key not in expected or key in seen:
        raise AssertionError("Unexpected or repeated Kafka source key")

    value = raw_value.decode("utf-8")
    decoded = decode_message(value, key, topic, config.topic_prefix)

    if decoded["reason"] is not None:
        raise AssertionError("Spark rejected the Talaxie message: " + decoded["reason"])

    source, entity, original = expected[key]
    envelope = json.loads(value)

    verify_record(envelope["data"], original)

    mapped_json = decoded["mapped_json"]

    if mapped_json is None:
        raise AssertionError("Spark returned no consolidated mapping")

    if json.loads(mapped_json) != MAPPERS[entity](original, source):
        raise AssertionError("Consolidated mapping differs from the existing backend")

    seen.add(key)

    return source, entity


def verify_kafka(config, expected: dict) -> dict:
    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": f"talaxie-validation-{uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    seen = set()
    counts = Counter()
    deadline = time.monotonic() + KAFKA_TIMEOUT

    try:
        consumer.subscribe(config.topics())

        while time.monotonic() < deadline and len(seen) < len(expected):
            message = consumer.poll(1)

            if message is None:
                continue

            source, entity = verify_message(message, config, expected, seen)
            counts[f"{source}:{entity}"] += 1

        if seen != expected.keys():
            raise AssertionError(f"Missing Kafka records: {len(expected) - len(seen)}")

        if consumer.poll(2) is not None:
            raise AssertionError("Unexpected additional Kafka record")

        return dict(counts)
    finally:
        consumer.close()


def verify_snapshot(root: str, expected: dict) -> dict:
    from abassurance.systems.spark_pipeline.hdfs import HdfsState, read_snapshot
    from abassurance.systems.spark_pipeline.run import create_session

    spark = create_session(kafka=False)

    try:
        manifest = HdfsState(spark, root).read_json("CURRENT.json")
        expected_entities = {entity: {} for entity in ID_FIELDS}

        for source, entity, record in expected.values():
            mapped = MAPPERS[entity](record, source)
            expected_entities[entity][mapped[ID_FIELDS[entity]]] = mapped

        for entity, wanted in expected_entities.items():
            actual = {}
            frame = read_snapshot(spark, root, entity, manifest=manifest)

            for row in frame.toLocalIterator():
                data = row.asDict(recursive=True)
                data.pop("_kafka", None)
                actual[data[ID_FIELDS[entity]]] = data

            if actual != wanted:
                raise AssertionError(f"HDFS snapshot differs for {entity}")

        if manifest["counts"]["rejected"] != 0:
            raise AssertionError("Fresh Talaxie validation snapshot contains rejections")

        return manifest
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir")

    arguments = parser.parse_args()
    config = load_kafka_config()

    if not config.topic_prefix.startswith("talaxie-validation-"):
        parser.error("Use fresh isolated topics with KAFKA_TOPIC_PREFIX=talaxie-validation-<run>")

    expected = source_rows(load_settings())

    if not expected:
        raise AssertionError("Source tables are empty; nothing was validated")

    report = {"published": verify_kafka(config, expected), "records": len(expected)}

    if arguments.state_dir:
        report["snapshot"] = verify_snapshot(arguments.state_dir, expected)

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
